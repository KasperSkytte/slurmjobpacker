"""Behavioural tests for lua/sjp.lua, the plugin itself, run by a Lua 5.4
interpreter outside Slurm (apt install lua5.4).

The placement tables are written by sjpd's own code, and every job's expected
placement is worked out by the Python twins of the plugin's decisions
(plugin_lookup, has_room, pick_node, soonest_node): the plugin and sjpd must
agree. Also checks what only the plugin does: pins remembered across a burst
of submissions, and the jobs it leaves alone.

    python3 tests/test_lua.py
"""
import os, shutil, subprocess, sys, tempfile, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sjp import config, policy                                     # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LUA = shutil.which("lua5.4") or shutil.which("lua")
if not LUA:
    sys.exit("no Lua interpreter: apt install lua5.4")

fails = []
def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + (f"  [{detail}]" if detail else ""))
    if not cond:
        fails.append(name)

print("0. both Lua files compile")
luac = shutil.which("luac5.4") or shutil.which("luac")
for f in ("lua/sjp.lua", "lua/job_submit.lua"):
    r = subprocess.run([luac, "-p", os.path.join(REPO, f)], capture_output=True, text=True)
    check(f, r.returncode == 0, r.stderr.strip())

CFG = config.defaults()
DEMAND = [tuple(x) for x in CFG["policy"]["demand"]]
NOW = time.time()
TIERS = {"slim1": 10, "fat1": 8}
# Four nodes: two slim (4 GB per CPU), two fat (16 GB per CPU), partly in use.
NODES = {"n1": (64, 256000, "slim1"), "n2": (64, 256000, "slim1"),
         "n3": (64, 1024000, "fat1"), "n4": (64, 1024000, "fat1")}
FREE = {"n1": (32, 128000, ["slim1"]), "n2": (8, 200000, ["slim1"]),
        "n3": (48, 900000, ["fat1"]), "n4": (0, 0, ["fat1"])}
ENDS = {"n1": [(int(NOW) + 3600, 32, 128000)], "n2": [(int(NOW) + 600, 56, 56000)],
        "n3": [(int(NOW) + 7200, 16, 124000)], "n4": [(int(NOW) + 1800, 64, 1024000)]}
TOTAL = {}
for c, m, p in NODES.values():
    TOTAL.setdefault(p, []).append((c, m))
FREE_BY_PART = {}
for n, (fc, fm, ps) in FREE.items():
    for p in ps:
        FREE_BY_PART.setdefault(p, []).append((fc, fm))
TABLE = policy.build_table(CFG, TOTAL, FREE_BY_PART)
FULL = policy.build_table(CFG, TOTAL, TOTAL)
CAP = policy.caps(TOTAL)

tmp = tempfile.mkdtemp(prefix="sjp-lua-")
DISABLE = os.path.join(tmp, "disable")


def write_table(name, full=None, ends=None, generated_at=None, version=1, free=None):
    """The table sjpd would write; free: other free space than FREE."""
    table = TABLE
    if free is not None:
        by_part = {}
        for fc, fm, ps in free.values():
            for p in ps:
                by_part.setdefault(p, []).append((fc, fm))
        table = policy.build_table(CFG, TOTAL, by_part)
    pin = dict(nodes=free or FREE, tiers=TIERS, room=None, enabled=True, queue_at=0, ends=ends)
    path = os.path.join(tmp, name)
    with open(path, "w") as f:
        f.write(policy.render_lua(table, CFG, generated_at or time.time(), version, TOTAL,
                                  pin, full))
    return path


def lua_value(v):
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int) and v >= 2 ** 63:
        return hex(v)                    # Lua reads it as Slurm's NO_VAL64, as slurmctld does
    if isinstance(v, (int, float)):
        return repr(v)
    if isinstance(v, str):
        return '"%s"' % v.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
    if isinstance(v, dict):
        return "{" + ", ".join(f"{k} = {lua_value(x)}" for k, x in v.items()) + "}"
    return "{" + ", ".join(lua_value(x) for x in v) + "}"


def job(cpus, mem=None, minutes=60, **kw):
    """A job_desc as job_submit.lua sees it: --mem as min_mem_per_node."""
    j = dict(script="#!/bin/sh\n", name="t", min_cpus=cpus, time_limit=minutes,
             min_mem_per_node=mem if mem is not None else 0xfffffffffffffffe)
    j.update(kw)
    return j


def run(table, cases):
    """cases: [(id, job, fresh)] -> {id: (placed, partition, req_nodes, comment, why)}."""
    jobs = os.path.join(tmp, "jobs.lua")
    with open(jobs, "w") as f:
        f.write("return " + lua_value([dict(id=i, fresh=fresh, job=j) for i, j, fresh in cases]))
    r = subprocess.run([LUA, os.path.join(REPO, "tests/lua_harness.lua"),
                        os.path.join(REPO, "lua/sjp.lua"), table, DISABLE, jobs],
                       capture_output=True, text=True)
    if r.returncode or r.stderr:
        print(r.stderr)
    out = {}
    for line in r.stdout.splitlines():
        i, placed, part, node, note, why = line.split("|", 5)
        out[i] = (placed == "true", part, node, note, why)
    return out


def expect(j, full, ends):
    """What the plugin should do with job j, from the Python twins:
    (placed, sorted partitions, pinned node)."""
    cpus, minutes = j["min_cpus"], j["time_limit"]
    mem_given = j["min_mem_per_node"] != 0xfffffffffffffffe
    mem = j["min_mem_per_node"] if mem_given else 512
    parts = policy.plugin_lookup(TABLE, CAP, CFG, cpus, mem, minutes)[0]
    by_shape = False
    if not policy.has_room(cpus, mem, parts, FREE):
        if full is None:
            return False, "", ""
        parts, by_shape = policy.plugin_lookup(full, CAP, CFG, cpus, mem, minutes)[0], True
    node, pparts = None, []
    if mem_given and not by_shape:
        node, pparts, _ = policy.pick_node(cpus, mem, parts, FREE, TIERS, DEMAND,
                                           CFG["pin"]["min_gain"], CFG["pin"]["min_ratio_gain"])
    elif mem_given and ends is not None:
        node, pparts, _ = policy.soonest_node(cpus, mem, parts, FREE, ends, NOW, TIERS)
    return True, ",".join(sorted(pparts if node else parts)), node or ""


JOBS = {"small": job(8, 32000), "himem": job(4, 160000), "wide": job(40, 160000),
        "fat-wide": job(40, 640000), "big": job(60, 240000), "whole": job(64, 1000000),
        "nomem": job(1), "long": job(16, 64000, minutes=4000)}

for title, full, ends in (("1. as sjpd decides: room now, or not placed", None, None),
                          ("2. always_place: by shape when no node has room", FULL, None),
                          ("3. wait_for_room: and pinned to the node with room first", FULL, ENDS)):
    print("\n" + title)
    got = run(write_table(f"t{title[0]}.lua", full, ends),
              [(k, dict(v), True) for k, v in JOBS.items()])
    for k, j in JOBS.items():
        want = expect(j, full, ends)
        g = got.get(k)
        have = (g[0], ",".join(sorted(g[1].split(","))) if g[0] else "", g[2]) if g else None
        check(f"{k}: {want}", have == want, f"plugin: {have}, why: {g[4] if g else '-'}")

print("\n4. what the plugin writes on the job")
got = run(write_table("t4.lua", FULL, ENDS), [("whole", job(64, 1000000), True),
                                              ("small", job(8, 32000), True)])
note = got["whole"][3]
check("a job waiting for room is marked so, with the wait",
      note.startswith("sjp:pin=n4;from=") and ";room=no;eta=0.5h" in note, note)
check("the slurmctld log line says why", "no room, by shape pin=n4 eta 0.5h" in got["whole"][4],
      got["whole"][4])
check("a job with room is marked with the node's free space",
      ";free=" in got["small"][3] and "room=no" not in got["small"][3], got["small"][3])

print("\n5. a burst: pins the plugin made count until sjpd has seen them")
# Room for one 16-CPU job on n1 (a match by memory per CPU) and one on n2.
bfree = {"n1": (16, 64000, ["slim1"]), "n2": (20, 64000, ["slim1"]),
         "n3": (0, 0, ["fat1"]), "n4": (0, 0, ["fat1"])}
burst = [(f"b{i}", job(16, 64000), i == 0) for i in range(3)]
got = run(write_table("t5.lua", free=bfree), burst)
seen = str([got[k] for k in sorted(got)])
check("the first is pinned to the better node", got["b0"][2] == "n1", seen)
check("the second no longer sees room there; only n2 has, so Slurm picks it",
      got["b1"][0] and got["b1"][2] == "" and "only one node has room" not in got["b1"][4]
      and got["b1"][3].startswith("sjp:placed="), seen)

print("\n6. jobs the plugin leaves to the site's rules")
skip = {"not batch": job(1, 1000, script=""), "gpu": job(1, 1000, tres_per_node="gres/gpu:1"),
        "constraint": job(1, 1000, features="avx512"), "multi-node": job(1, 1000, min_nodes=2),
        "no memory": job(1, 0), "reservation": job(1, 1000, reservation="r1")}
got = run(write_table("t6.lua"), [(k, v, True) for k, v in skip.items()])
for k in skip:
    check(f"{k}: not placed, says {k!r}", got[k][0] is False and got[k][4] == k,
          str(got[k]))
got = run(write_table("t7.lua", generated_at=time.time() - 3600), [("old", job(8, 32000), True)])
check("a table sjpd has not refreshed: not placed, \"stale\"",
      got["old"][0] is False and got["old"][4] == "stale", str(got["old"]))
open(DISABLE, "w").close()
got = run(write_table("t8.lua"), [("off", job(8, 32000), True)])
check("the disable file: not placed, \"no table\"",
      got["off"][0] is False and got["off"][4] == "no table", str(got["off"]))
os.remove(DISABLE)

shutil.rmtree(tmp)
print("\nFAILURES: " + ", ".join(fails) if fails else "\nALL PASS")
sys.exit(1 if fails else 0)
