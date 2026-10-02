"""Behavioural tests for the placement core, on an example topology: two slim
and two fat partitions, each a faster and a slower generation.

These are the claims the design rests on. If one of them fails, the argument in
docs/design.html is wrong, not just the code.

Safe to run on a live cluster: nothing here may start a process. subprocess.run
is replaced before sjp is imported, and any attempt fails the run.
"""
import sys, os, time, subprocess, tempfile
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# A plain traceback on failure. Distribution crash handlers (Ubuntu's apport)
# start processes of their own, which the guard below would block.
sys.excepthook = sys.__excepthook__

class _NoProcesses(AssertionError):
    pass
def _no_processes(args, **kw):
    raise _NoProcesses(f"tests must not start processes: {args}")
subprocess.run = _no_processes

from sjp import config, policy, daemon

CFG = config.defaults()
DEMAND = [tuple(x) for x in CFG['policy']['demand']]
TOTAL = {
    'slim2':  [(192, 1021567)] * 5 + [(256, 1021540), (192, 505529)],
    'fat2': [(192, 2041663), (256, 2041636)],
    'slim1':  [(288, 1537338)] * 2 + [(256, 1537407)] * 2,
    'fat1': [(288, 2311479)] * 2,
}
def scale(frac):
    """Free shapes with `frac` of every node already consumed, ratio-matched."""
    return {p: [(int(c * (1 - frac)), int(m * (1 - frac))) for c, m in v]
            for p, v in TOTAL.items()}

fails = []
def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + (f"  [{detail}]" if detail else ""))
    if not cond: fails.append(name)

print("1. feasibility is absolute, not a ratio")
tiny_fat = policy.feasible(1, 32768, TOTAL)          # 1 CPU, 32 GB -> 32k MB/CPU
check("a 1-CPU/32GB job is feasible everywhere", set(tiny_fat) == set(TOTAL),
      ",".join(sorted(tiny_fat)))
huge = policy.feasible(24, 2252800, TOTAL)           # 24 CPU, 2.2 TB
# fat2 tops out at 2,041,663 MB, so 2.2 TB fits on node14/15 only:
# two nodes in the entire cluster.
check("a 24-CPU/2.2TB job is feasible only on fat1",
      set(huge) == {'fat1'}, ",".join(sorted(huge)))

print("\n2. idle cluster packs by ratio (the cold-start case)")
idle = policy.choose(1, 32768, TOTAL, scale(0.0), DEMAND, 0.25)
check("high-ratio job avoids slim nodes when everything is free",
      not ({'slim2', 'slim1'} & set(idle)), ",".join(idle))

print("\n3. a nearly full cluster admits anything that fits (phi is spent everywhere)")
loaded = policy.choose(1, 32768, TOTAL, scale(0.99), DEMAND, 0.25)
check("same job admitted to every partition that fits it", len(loaded) > len(idle)
      and set(loaded) == set(policy.feasible(1, 32768, TOTAL)),
      f"idle={len(idle)} loaded={len(loaded)}: {','.join(loaded)}")

print("\n5. the table only changes when the decisions change")
d = daemon.Daemon(CFG)
d.mode = "observe"
sigs = []
for frac in (0.0, 0.0, 0.0, 0.5, 0.5, 0.9):
    tbl = policy.build_table(CFG, TOTAL, scale(frac))
    sigs.append(hash(tuple(sorted((k, ",".join(v)) for k, v in tbl.items()))))
check("identical state -> identical signature", sigs[0] == sigs[1] == sigs[2])
check("different load -> different signature", len(set(sigs)) > 1,
      f"{len(set(sigs))} distinct across 3 load levels")

print("\n6. sjp places a job only where some node has room for it now")
free6 = {"a": (8, 64000, ["slim1"]), "b": (64, 32000, ["slim1", "slim2"]),
         "c": (2, 500000, ["fat1"])}
check("a node in the partitions has room", policy.has_room(8, 32000, ["slim1"], free6))
check("room only in other partitions does not count",
      not policy.has_room(2, 400000, ["slim1", "slim2"], free6))
check("CPUs and memory must both fit on one node",
      not policy.has_room(64, 64000, ["slim1", "slim2"], free6))

print("\n7. rendered table is loadable and complete")
tbl = policy.build_table(CFG, TOTAL, scale(0.3))
lua = policy.render_lua(tbl, CFG, time.time(), 7)
b = CFG['policy']['buckets']
expect = (len(b['mem_per_cpu'])+1) * (len(b['cpus'])+1) * (len(b['walltime_h'])+1)
check("every bucket has an entry", len(tbl) == expect, f"{len(tbl)} == {expect}")
check("no empty partition set", all(v for v in tbl.values()),
      "3 of 64 shape buckets fit no node; they must still name a real partition")
check("lua renders", lua.startswith("-- generated") and lua.rstrip().endswith("}"))
lua2 = policy.render_lua(tbl, CFG, time.time(), 7, TOTAL)
check("caps emitted for every partition",
      all(f'["{p}"]' in lua2.split("t = {")[0] for p in TOTAL))
check("cap values are the largest node in each partition",
      '["fat1"] = {288, 2311479}' in lua2 and '["fat2"] = {256, 2041663}' in lua2)

print("\n8. bucketing must not discard feasibility")
# 24 CPU x 2200 GB and 24 CPU x 800 GB share bucket (7,4); only fat1 holds the
# first. The plugin filters by real size against the emitted caps.
big = 2200 * 1024
cap = {p: (max(c for c, _ in v), max(m for _, m in v)) for p, v in TOTAL.items()}
setfor74 = "fat2"                      # what the table actually held
kept = [p for p in setfor74.split(",") if 24 <= cap[p][0] and big <= cap[p][1]]
check("a 2.2TB job is not left with a partition that cannot hold it",
      kept == [], "fat2 max mem is %d < %d" % (cap['fat2'][1], big))
check("fat1 can hold it", big <= cap['fat1'][1])

print("\n9. plugin_lookup mirrors the plugin's refit")
tbl = policy.build_table(CFG, TOTAL, scale(0.3))
cap = policy.caps(TOTAL)
parts, key, refit = policy.plugin_lookup(tbl, cap, CFG, 24, 2200 * 1024, 60)
check("a 2.2TB job is refit to the only partition that holds it",
      parts == ['fat1'], f"{key} -> {parts} refit={refit}")
parts, key, refit = policy.plugin_lookup(tbl, cap, CFG, 1, 2048, 60)
check("an ordinary job is not refit", not refit and parts == tbl[key], ",".join(parts))

print("\n10. a dry run changes nothing and says what it would have done")
import io, json, tempfile
from sjp import slurm
ran = []
real_run, real_q = slurm._run, slurm.qos_cpu_limits
slurm._run = lambda args, timeout=10.0: ran.append(args) or ""
slurm.qos_cpu_limits = lambda qos: (864, 1760)            # what the QOS says
try:
    cfg = config.defaults()
    cfg["general"]["mode"] = "observe"
    cfg["limits"]["mode"] = "global"
    cfg["general"]["state_dir"] = tempfile.mkdtemp()
    cfg["general"]["disable_file"] = os.path.join(tempfile.mkdtemp(), "disable")
    d = daemon.Daemon(cfg)
    d.log_fh = io.StringIO()
    nodes = {f"{p}{i}": dict(cpus=c, mem=m, alloc_cpus=0, alloc_mem=0,
                             partitions=[p], up=True)
             for p, v in TOTAL.items() for i, (c, m) in enumerate(v)}
    d.sh.nodes, d.sh.parts = nodes, {p: dict(tier=1, state="UP") for p in TOTAL}
    d.sh.pending = [dict(jobid="1", user="u", cpus=4, mem=8192, req_mem=8192, qos="normal",
                         reason="QOSMaxCpuPerUserLimit", partition="slim1", state="PD")]
    d.score_once(d.sh.nodes, d.sh.parts, time.time())
    for _ in range(cfg["limits"]["hysteresis"]):
        d.act_once()                     # idle cluster, a capped job that fits: pulse
    recs = [json.loads(l) for l in d.log_fh.getvalue().splitlines()]
    acts = {r["action"]: r for r in recs if r["event"] == "action"}
    check("actuation is off outside enforce", not slurm.actuation())
    check("no state-changing command was run", ran == [], str(ran))
    check("the policy table is not written where the plugin reads it",
          not os.path.exists(d.table_path))
    check("the would-be table is written for inspection",
          os.path.exists(d.dryrun_table_path))
    q = acts.get("set_qos_cpu_limits", {})
    check("the limit change is logged with its command and reason, not run",
          q.get("executed") is False and q.get("cmd", "").startswith("sacctmgr -i modify qos")
          and "held only by the CPU cap" in q.get("why", ""), json.dumps(q)[:200])
    j10 = dict(jobid="2", user="u", name="x", cpus=4, mem=8192, req_mem=8192, qos="normal",
               reason="Resources", partition="slim1", state="PD", timelimit=60)
    r = d.evaluate(j10, ["slim1"], d.snap)
    check("a job that fits free space is placed", r["placed"] and r["would"], r["would"])
    full = dict(d.snap, node_free={n: (0, 0, ps) for n, (_, _, ps) in d.snap["node_free"].items()})
    r = d.evaluate(j10, ["slim1"], full)
    check("with no room anywhere it would take, it is left to the site's rule",
          r["placed"] is False and r["would"] == "" and r["verdict"] == "not placed"
          and r["pin"] is None, r["why"])
    check("apply() refuses while actuation is off",
          slurm.apply(slurm.cmd_set_qos_cpu_limits("sjp-test-no-such-qos", 1, 1)) is False
          and ran == [])
    slurm.set_actuation(True)
    check("apply() runs once actuation is on",
          slurm.apply(slurm.cmd_set_qos_cpu_limits("sjp-test-no-such-qos", 1, 1)) is True
          and len(ran) == 1)
finally:
    slurm._run, slurm.qos_cpu_limits = real_run, real_q
    slurm.set_actuation(False)

print("\n10a. QOS caps are raised only in a short pulse, only for jobs they hold")
from sjp import limits
cfg = config.defaults()
lp = limits.LimitPulse(cfg, "normal", (864, 1760))
H = cfg["limits"]["hysteresis"]
out = [lp.observe(0.9, 0, now=t) for t in range(H + 2)]
check("no pulse while no job is held by the caps", all(o is None for o in out))
out = [lp.observe(0.9, 3, now=100 + t) for t in range(H)]
check("a pulse after the hysteresis, to the ceiling",
      out[-1] == (1728, 3520) and all(o is None for o in out[:-1]), str(out))
check("held for the pulse", lp.observe(0.9, 3, now=100 + H + 30) is None)
check("back to base when the pulse is over",
      lp.observe(0.9, 3, now=100 + H + 61) == (864, 1760), lp.why)
out = [lp.observe(0.9, 3, now=200 + t) for t in range(H + 2)]
check("no new pulse during the cooldown", all(o is None for o in out))
out = [lp.observe(0.9, 3, now=500 + t) for t in range(H)]
check("a new pulse as soon as the cooldown is over, if the need persisted",
      out[0] == (1728, 3520) and all(o is None for o in out[1:]), str(out))
check("a pulse ends early when the cluster fills up",
      lp.observe(0.05, 3, now=500 + H + 5) == (864, 1760), lp.why)
check("the pulse is released by the reasons squeue really prints",
      slurm.CAP_REASONS == {"QOSMaxCpuPerUserLimit", "MaxCpuPerAccount"})

print("\n10c. each QOS pulses from its own caps, and an interrupted pulse is undone")
cfg = config.defaults()
cfg["limits"]["mode"] = "global"
cfg["limits"]["state_file"] = os.path.join(tempfile.mkdtemp(), "limits.json")
real_q = slurm.qos_cpu_limits
live = {"normal": (1000, 2000), "long": (100, 200)}
slurm.qos_cpu_limits = lambda qos: live[qos]
try:
    d = daemon.Daemon(cfg); d.log_fh = io.StringIO()
    d.resolve_limits()
    check("each QOS's base caps are read from that QOS",
          (d.limiter("normal").base_u, d.limiter("long").base_u) == (1000, 100))
    d.remember_pulse("normal", True)                     # a pulse starts, then sjpd dies
    live["normal"] = (2000, 4000)                        # the QOS still shows the pulse
    d2 = daemon.Daemon(cfg); d2.log_fh = io.StringIO()
    d2.resolve_limits()
    check("after a crash mid-pulse the recorded base is used, not the raised caps",
          (d2.limiter("normal").base_u, d2.limiter("normal").base_a) == (1000, 2000))
    d2.remember_pulse("normal", False)
    d3 = daemon.Daemon(cfg); d3.log_fh = io.StringIO()
    d3.resolve_limits()
    check("once back to base, the QOS is read again", d3.limiter("normal").base_u == 2000)
    live["nocap"] = (None, None)
    check("a QOS without caps: nothing to pulse", not d3.limiter("nocap").known
          and d3.limiter("nocap").observe(0.9, 5) is None)

    # Caps changed by hand are followed, with no restart.
    cfg["general"]["state_dir"] = tempfile.mkdtemp()
    d4 = daemon.Daemon(cfg); d4.log_fh = io.StringIO(); d4.mode = "enforce"
    live["normal"] = (1000, 2000)
    lim = d4.limiter("normal")
    live["normal"] = (500, 1000)                         # the admin lowers them
    ch = lim.observe(0.9, 5, now=0)
    for t in range(1, cfg["limits"]["hysteresis"]):
        ch = lim.observe(0.9, 5, now=t * 1.0)
    check("a pulse starts from the caps as they are now",
          d4.follow_admin("normal", lim, ch, (1000, 2000)) == ((1000, 2000), (500, 1000))
          and (lim.base_u, lim.base_a) == (500, 1000), str((lim.base_u, lim.cur_u)))
    live["normal"] = (700, 1400)                         # changed again during the pulse
    ch = lim.observe(0.9, 5, now=1000.0)                 # the pulse is over
    check("caps changed during a pulse are kept and become the base",
          d4.follow_admin("normal", lim, ch, (1000, 2000)) == (None, (1000, 2000))
          and (lim.base_u, lim.cur_u) == (700, 700))
    live["normal"] = (1000, 2000)                        # still raised: a normal end
    lim.raised_at, lim.cur_u, lim.cur_a = 2000.0, 1000, 2000
    ch = lim.observe(0.9, 5, now=3000.0)
    check("otherwise the pulse ends back at base",
          d4.follow_admin("normal", lim, ch, (1000, 2000)) == ((700, 1400), (1000, 2000)))
finally:
    slurm.qos_cpu_limits = real_q

print("\n10d. the starvation guard widens long-waiting jobs, and only those")
cfg = config.defaults()
d = daemon.Daemon(cfg); d.log_fh = io.StringIO()
d.snap = dict(total=TOTAL)
now = time.time()
real_ac = slurm.admin_comment
slurm.admin_comment = lambda jid: "" if jid == "8" else ("sjp:pin=n1;from=slim1" if jid == "3"
                                                        else "sjp:placed")
def pjob(jid, part, waited_h, cpus=8, mem=32768, reason="Resources", **kw):
    return dict(dict(jobid=jid, state="PD", reason=reason, partition=part, cpus=cpus,
                     mem=mem, req_mem=mem, eligible=now - waited_h * 3600, gpu=False,
                     req_nodes=""), **kw)
jobs = [pjob("1", "slim1", 5),                     # slim, over its 4 h budget
        pjob("2", "slim1", 2),                     # within budget
        pjob("3", "fat1", 2, cpus=1, mem=65536),   # fat, over its 1 h budget
        pjob("4", "slim1", 9, reason="QOSMaxCpuPerUserLimit"),   # widening cannot help
        pjob("5_[1-9]", "slim1", 9),               # array: left alone
        pjob("6", "gpu", 9),                       # not a batch partition sjp assigns
        pjob("7", "slim1", 9, req_nodes="node12"), # pinned: left alone
        pjob("8", "slim1", 9),                     # placed by the site's own rule
        pjob("9", "slim1", 9, nnodes=2)]           # multi-node: not sjp's
try:
    d.widen_starving(jobs, now)
finally:
    slurm.admin_comment = real_ac
recs = [json.loads(l) for l in d.log_fh.getvalue().splitlines()]
w = {r["jobid"]: r for r in recs if r.get("action") == "widen_partitions"}
check("slim job over its budget, and fat job over its shorter one, are widened; "
      "a job sjp did not place is not",
      sorted(w) == ["1", "3"], str(sorted(w)))
check("widened to every partition that can hold the job",
      w["1"]["after"] == ",".join(sorted(policy.feasible(8, 32768, TOTAL))), w["1"]["after"])
check("a dry run only says what it would do", not w["1"]["executed"]
      and w["1"]["cmd"].startswith("scontrol update jobid=1 partition="))
d.widen_starving(jobs, now)
check("each job is considered once", d.log_fh.getvalue().count("widen_partitions") == 2)

print("\n10e. flex mode moves capped jobs that fit now to the flex QOS, and back")
cfg = config.defaults()
cfg["limits"]["mode"] = "flex"
cfg["general"]["state_dir"] = tempfile.mkdtemp()
d = daemon.Daemon(cfg); d.log_fh = io.StringIO(); d.mode = "enforce"
notes = {"1": "sjp:placed", "2": "", "3": "", "4": "", "5": "sjp:placed;qos=normal>flex",
         "6": ""}
ran = []
real_ac, real_apply = slurm.admin_comment, slurm.apply
real_uq, real_cn = slurm.user_qos, slurm.cluster_name
assoc = {("u", "a"): {"normal", "flex"}, ("v", "a"): {"normal"}}
slurm.user_qos = lambda cluster: {k: set(v) for k, v in assoc.items()}
slurm.cluster_name = lambda: "c"
slurm.admin_comment = lambda jid: notes[jid]
slurm.apply = lambda argv, timeout=10.0: ran.append(argv) or True
def fj(jid, cpus, prio, reason="QOSMaxCpuPerUserLimit", qos="normal", **kw):
    return dict(dict(jobid=jid, cpus=cpus, mem=cpus * 4000, req_mem=cpus * 4000,
                     priority=prio, reason=reason, qos=qos, partition="slim1",
                     gpu=False, req_nodes="", user="u", account="a"), **kw)
nf10 = {"a": (64, 512000, ["slim1"]), "b": (16, 64000, ["slim1", "slim2"])}
pend = [fj("0", 8, 10, nnodes=2), fj("1", 48, 9), fj("2", 32, 8), fj("3", 16, 7),
        fj("4", 8, 6, reason="Resources"), fj("6", 4, 5, partition="fat1")]
try:
    d.flex(pend, nf10, 100.0)
    moved = [a[2] for a in ran]
    check("capped jobs that fit move, highest priority first, until the space is used",
          moved == ["jobid=1", "jobid=3"], str(moved))
    check("the move keeps the job's own QOS in its sjp mark",
          ran[0][3:] == ["qos=flex", "admincomment=sjp:placed;qos=normal>flex"]
          and ran[1][4] == "admincomment=sjp:qos=normal>flex", str(ran))
    ran.clear()
    d.flex(pend, nf10, 110.0)
    check("a moved job is not moved again during the cooldown", ran == [])
    pend2 = [fj("1", 48, 9, qos="flex"), fj("5", 8, 9, qos="flex")]
    notes["1"] = "sjp:placed;qos=normal>flex"
    d.unflex(pend2, 120.0)
    check("still pending in flex: a job moved this run waits flex_revert_after; "
          "one from before a restart goes back now",
          [a[2:] for a in ran] == [["jobid=5", "qos=normal"]], str(ran))
    ran.clear()
    d.unflex(pend2[:1], 100.0 + cfg["limits"]["flex_revert_after"] + 1)
    check("then it goes back to its own QOS", [a[2:] for a in ran] == [["jobid=1", "qos=normal"]])
    ran.clear()
    notes["7"] = ""
    d.unflex([fj("7", 8, 9, qos="flex")], 500.0)
    check("a job someone else put in flex is left alone", ran == [])
    notes["9"] = "sjp:placed;flex=normal"
    d.unflex([fj("9", 8, 9, qos="flex")], 600.0)
    check("the mark older versions wrote still brings a job back",
          [a[2:] for a in ran] == [["jobid=9", "qos=normal"]], str(ran))
    ran.clear()
    notes["8"] = ""
    d.flex([fj("8", 4, 9, user="v")], nf10, 1000.0)
    check("a user who may not use the flex QOS: the job is not moved, and the log says so",
          ran == [] and "user v (account a) may not use QOS flex" in d.log_fh.getvalue())
finally:
    slurm.admin_comment, slurm.apply = real_ac, real_apply
    slurm.user_qos, slurm.cluster_name = real_uq, real_cn
check("the marks the plugin writes parse back",
      slurm.sjp_marks("sjp:placed=a,b;job=4c,16G,4.0G/c;v=20")
      == {"placed": "a,b", "job": "4c,16G,4.0G/c", "v": "20"})
check("sjp marks parse and extend",
      slurm.sjp_marks("x;sjp:pin=n1;from=a,b") == {"pin": "n1", "from": "a,b"}
      and slurm.set_mark("site note", "qos", "normal>flex") == "site note;sjp:qos=normal>flex"
      and slurm.set_mark("sjp:placed=a;qos=normal>flex", "qos", "normal>flex")
      == "sjp:placed=a;qos=normal>flex"
      and slurm.set_mark("sjp:placed=a;flex=normal", "qos", "normal>flex", drop=("flex",))
      == "sjp:placed=a;qos=normal>flex"
      and slurm.sjp_marks("") == {})

print("\n10b. the process launcher itself refuses writes while actuation is off")
import subprocess
launched = []
blocker = subprocess.run
subprocess.run = lambda args, **kw: launched.append(args) or subprocess.CompletedProcess(args, 0, "", "")
try:
    slurm.set_actuation(False)
    refused = 0
    writes = [slurm.cmd_set_qos_cpu_limits("sjp-test-no-such-qos", 1, 1),
              slurm.cmd_set_job_partitions("1", ["slim2"]),
              slurm.cmd_set_job_qos("1", "flex"),
              *slurm.cmd_release_pin("1", "slim1", "sjp:released=n1")]
    for argv in writes:
        try:
            slurm._run(argv)
        except slurm.SlurmError:
            refused += 1
    check("every state-changing command is refused before a process starts",
          refused == len(writes) and launched == [], f"refused={refused} launched={launched}")
    slurm._run(["scontrol", "show", "nodes", "--oneliner"])
    check("read-only commands still run", len(launched) == 1)
finally:
    subprocess.run = blocker

print("\n11. partitions are discovered; interactive and GPU nodes excluded by default")
check("GPU found in Gres", slurm._has_gpu("gpu:a10:1(S:0)", "cpu=64"))
check("GPU found in CfgTRES", slurm._has_gpu("(null)", "cpu=64,mem=1M,gres/gpu=2"))
check("no GPU", not slurm._has_gpu("(null)", "cpu=64,mem=1M"))
def node(parts, gpu=False):
    return dict(cpus=64, mem=256000, alloc_cpus=0, alloc_mem=0,
                partitions=parts, up=True, gpu=gpu)
nodes = {"a": node(["slim2"]), "b": node(["Interactive"]), "g": node(["gpu"], True),
         "m1": node(["mixed"]), "m2": node(["mixed"], True)}
parts = {p: dict(tier=1, state="UP") for p in ("slim2", "Interactive", "gpu", "mixed")}
cfg = config.defaults()
d = daemon.Daemon(cfg)
keep, dropped = d.partition_filter(parts, nodes)
check("defaults keep only CPU batch partitions", sorted(keep) == ["mixed", "slim2"],
      f"{keep} {dropped}")
total, _ = d.shapes(nodes, parts)
check("a mixed partition keeps only its CPU nodes", len(total["mixed"]) == 1)
cfg["topology"].update(exclude_interactive=False, exclude_gpu_nodes=False,
                       exclude_partitions=["slim2"])
keep, dropped = d.partition_filter(parts, nodes)
check("each exclusion can be turned off; names can be excluded",
      sorted(keep) == ["Interactive", "gpu", "mixed"] and "slim2" in dropped, f"{keep}")

print("\n12. node pins follow PriorityTier, then shape")
TIERS = {'slim1': 10, 'slim2': 9, 'fat1': 8, 'fat2': 7}
# A slim1 node already filled with low-memory jobs has 8 CPUs and ~490 GB left:
# 61 GB per CPU, the ratio of a high-memory job. The empty slim1 node would do too,
# but a 4-CPU/200 GB job there wastes its CPUs for everyone else. slim2 has a
# snug node as well, but ranks below slim1, and Slurm tries slim1 first.
nf = {'n12': (8, 500000, ['slim1']), 'n16': (256, 1500000, ['slim1']),
      'n03': (4, 204800, ['slim2']), 'n14': (288, 2300000, ['fat1'])}
node, parts, info = policy.pick_node(4, 204800, ['slim2', 'slim1'], nf, TIERS, DEMAND, 1.0)
check("pins inside the highest-ranked partition with room",
      node in ('n12', 'n16') and parts == ['slim1'], f"{node} {parts} {info['why']}")
check("there, on the slim node whose leftover memory suits the job",
      node == 'n12', info["why"])
nf2 = dict(nf, n12=(0, 0, ['slim1']), n16=(0, 0, ['slim1']))
node, parts, info = policy.pick_node(4, 204800, ['slim2', 'slim1'], nf2, TIERS, DEMAND, 1.0)
check("falls to the next rank only when the top one is full",
      node is None and "only n03" in info["why"], info["why"])
nf3 = {'a': (64, 256000, ['slim1']), 'b': (64, 256000, ['slim1'])}
node, _, info = policy.pick_node(4, 8192, ['slim1'], nf3, TIERS, DEMAND, 1.0)
check("no pin when the candidates are equally good", node is None, info["why"])
G = 1024
# Where capacity cannot tell the nodes apart, the free memory per CPU decides.
nf4 = {'small': (8, 40 * G, ['slim1']), 'big': (256, 1500 * G, ['slim1'])}
node, _, info = policy.pick_node(8, 32 * G, ['slim1'], nf4, TIERS, DEMAND, 1.0, 0.1)
check("near-equal by capacity: the closest memory per CPU wins (5 vs 5.9 GB for 4)",
      node == 'small', info["why"])
nf5 = {'r60': (4, 240 * G, ['slim1']), 'r150': (4, 600 * G, ['slim1'])}
node, _, info = policy.pick_node(1, 100 * G, ['slim1'], nf5, TIERS, DEMAND, 1.0, 0.1)
check("beyond the demand mix, where capacity is blind, the ratio still decides",
      node == 'r150', info["why"])
node, _, info = policy.pick_node(8, 32 * G, ['slim1'], nf4, TIERS, DEMAND, 1.0, 0.5)
check("no pin when the ratio difference is below min_ratio_gain", node is None, info["why"])
check("a job waiting on a dependency is pinned like any other",
      policy.pin_eligible(dict(jobid="5", nnodes=1, req_nodes="", reason="Dependency")) is None)
node, _, info = policy.pick_node(512, 8192, ['slim1'], nf3, TIERS, DEMAND, 1.0)
check("no pin when nothing has room", node is None and "no node" in info["why"])
check("arrays, multi-node, user nodelists and held jobs are never pinned",
      all(policy.pin_eligible(dict(jobid=i, nnodes=n, req_nodes=r, reason=why))
          for i, n, r, why in (("5_1", 1, "", "None"), ("5", 2, "", "None"),
                               ("5", 1, "node01", "None"), ("5", 1, "", "JobHeldUser")))
      and policy.pin_eligible(dict(jobid="5", nnodes=1, req_nodes="", reason="None", ntasks=100))
      and policy.pin_eligible(dict(jobid="5", nnodes=1, req_nodes="", reason="None", ntasks=1)) is None)
tbl = policy.build_table(CFG, TOTAL, scale(0.3))
lua = policy.render_lua(tbl, CFG, time.time(), 1, TOTAL,
                        dict(nodes=nf, tiers=TIERS,
                             room=dict(by_qos={"normal": {1000: 64}}, user_qos={1000: "normal"})))
check("free space and pin data are emitted", 'pin = {' in lua and 'enabled = true' in lua
      and '["n16"] = {256, 1500000, "slim1"}' in lua
      and '["normal"] = {[1000] = 64}' in lua and '[1000] = "normal"' in lua)
lua = policy.render_lua(tbl, CFG, time.time(), 1, TOTAL, dict(nodes=nf, tiers=TIERS,
                        room=None, enabled=False))
check("free space is emitted without pinning too, marked as such",
      'enabled = false' in lua and '["n16"] = {256, 1500000, "slim1"}' in lua)

print("\n12a. with [pin] time_aware, long jobs join nodes whose jobs run long")
two = {"short": (64, 256000, ["slim1"]), "long": (64, 256000, ["slim1"])}
busy = {"short": 3600, "long": 14 * 86400}
args = (4, 16000, ["slim1"], two, {"slim1": 1}, CFG["policy"]["demand"], 1.0, 0.1)
check("off by default: two equal nodes, no pin", policy.pick_node(*args)[0] is None)
check("a 2-week job goes to the node whose jobs run for weeks",
      policy.pick_node(*args, busy, 14 * 1440)[0] == "long",
      policy.pick_node(*args, busy, 14 * 1440)[2]["why"])
check("a 30-minute job extends neither: no pin", policy.pick_node(*args, busy, 30)[0] is None)
check("an empty node counts as busy for a minute, so a long job avoids it",
      policy.pick_node(*args, {"long": 7 * 86400}, 2 * 1440)[0] == "long")
check("node busy times come from running jobs' end times and node lists",
      daemon.Daemon.busy_until([dict(state="R", end=100.0, nodelist="n[1-2]"),
                                dict(state="R", end=300.0, nodelist="n2"),
                                dict(state="PD", end=900.0, nodelist="")], 0)
      == {"n1": 100.0, "n2": 300.0})
lua_t = policy.render_lua(tbl, CFG, time.time(), 1, TOTAL,
                          dict(nodes=nf, tiers=TIERS, room=None, busy={"n16": 1790000000}))
check("busy times are emitted only when time_aware is on",
      'time_aware = true' in lua_t and '["n16"] = 1790000000' in lua_t
      and 'time_aware' not in policy.render_lua(tbl, CFG, time.time(), 1, TOTAL,
                                                dict(nodes=nf, tiers=TIERS, room=None)))

print("\n12b. reservations count as jobs planned on their nodes")
real_run = slurm._run
slurm._run = lambda args, timeout=10.0: (
    "ReservationName=whole StartTime=2026-10-02T10:00:00 EndTime=2026-10-03T10:00:00 "
    "Duration=1-00:00:00 Nodes=a NodeCnt=1 CoreCnt=8 Features=(null) PartitionName=(null) "
    "Flags=MAINT TRES=cpu=8 Users=root\n"
    "ReservationName=part StartTime=2026-10-02T10:00:00 EndTime=2026-10-03T10:00:00 "
    "Duration=1-00:00:00 Nodes=b,c NodeCnt=2 CoreCnt=6 Features=(null) PartitionName=(null) "
    "Flags=SPEC_NODES   NodeName=b CoreIDs=0-3   NodeName=c CoreIDs=4,6 TRES=cpu=12 Users=u\n"
    "ReservationName=licenses StartTime=2026-10-02T10:00:00 EndTime=2026-10-03T10:00:00 "
    "Duration=1-00:00:00 Nodes=(null) NodeCnt=0 CoreCnt=0 Licenses=x:1\n")
try:
    rs = {r["name"]: r for r in slurm.reservations()}
finally:
    slurm._run = real_run
check("whole-node and per-core reservations are read; ones without nodes skipped",
      sorted(rs) == ["part", "whole"] and rs["whole"]["nodes"] == {"a": None}
      and rs["part"]["nodes"] == {"b": 4, "c": 2}, str(rs))
t0 = 1_000_000.0
def rnode(cpus, mem, alloc=0, threads=1):
    return dict(cpus=cpus, mem=mem, alloc_cpus=alloc, alloc_mem=alloc * mem // cpus,
                threads=threads, partitions=["slim1"], up=True)
nodes12 = {"a": rnode(8, 32000), "b": rnode(16, 64000, threads=2), "c": rnode(16, 64000),
           "d": rnode(16, 64000)}
resv12 = [dict(name="whole", start=t0 - 60, end=t0 + 3600, nodes={"a": None}),
          dict(name="part", start=t0 - 60, end=t0 + 3600, nodes={"b": 4, "c": 2}),
          dict(name="soon", start=t0 + 7200, end=t0 + 9000, nodes={"d": None})]
jobs12 = [dict(state="R", reservation="part", nodelist="c", cpus=1)]
daemon.Daemon.reserve(nodes12, resv12, jobs12, t0)
check("an active whole-node reservation takes the whole node",
      nodes12["a"]["alloc_cpus"] == 8 and nodes12["a"]["alloc_mem"] == 32000)
check("reserved cores count with the node's threads, memory at its memory per CPU",
      nodes12["b"]["alloc_cpus"] == 8 and nodes12["b"]["alloc_mem"] == 32000)
check("CPUs the reservation's own jobs use are not counted twice",
      nodes12["c"]["alloc_cpus"] == 1, str(nodes12["c"]))
check("a reservation still to start is kept for later, not taken now",
      nodes12["d"]["alloc_cpus"] == 0 and nodes12["d"]["later"] == [(t0 + 7200, 16, 64000)])
nf12 = {"d": (16, 64000, ["slim1"]), "e": (16, 64000, ["slim1"])}
later12 = {"d": nodes12["d"]["later"]}
check("a job ending before the reservation starts can use the node",
      policy.free_for(nf12, later12, t0, 60)["d"] == (16, 64000, ["slim1"]))
check("a job running into it, or without a time limit, cannot",
      policy.free_for(nf12, later12, t0, 180)["d"][:2] == (0, 0)
      and policy.free_for(nf12, later12, t0, None)["d"][:2] == (0, 0))
lua_r = policy.render_lua(tbl, CFG, time.time(), 1, TOTAL,
                          dict(nodes=nf, tiers=TIERS, room=None, later=later12))
check("upcoming reservations are passed to the plugin",
      '["d"] = {{%d, 16, 64000}},' % (t0 + 7200) in lua_r)

print("\n13. stale pins are released, and only sjp's own")
real_ac, real_apply = slurm.admin_comment, slurm.apply
comments = {"7": "sjp:pin=n16;from=slim1,slim2;job=4c,16G,4.0G/c;v=3", "8": ""}
slurm.admin_comment = lambda jid: comments[jid]
applied = []
slurm.apply = lambda argv, timeout=10.0: applied.append(argv) or True
try:
    now = time.time()
    jobs = [dict(jobid="7", state="PD", req_nodes="n16", submit=now - 120, reason="Resources"),
            dict(jobid="8", state="PD", req_nodes="n03", submit=now - 120, reason="Resources"),
            dict(jobid="9", state="PD", req_nodes="n16", submit=now - 5, reason="Resources")]
    for mode in ("observe", "enforce"):
        cfg = config.defaults(); cfg["general"]["mode"] = mode
        d = daemon.Daemon(cfg); d.log_fh = io.StringIO()
        d.release_pins(jobs, now)
        recs = [json.loads(l) for l in d.log_fh.getvalue().splitlines()]
        rel = [r for r in recs if r.get("action") == "release_pin"]
        if mode == "observe":
            check("observe: logs the release it would do, runs nothing",
                  [r["jobid"] for r in rel] == ["7"] and not rel[0]["executed"] and applied == [],
                  json.dumps(rel)[:200])
        else:
            check("enforce: releases the stale sjp pin and restores its partitions",
                  applied == [["scontrol", "update", "jobid=7", "reqnodelist="],
                              ["scontrol", "update", "jobid=7", "partition=slim1,slim2",
                               "admincomment=sjp:released=n16;from=slim1,slim2;job=4c,16G,4.0G/c;v=3"]],
                  str(applied))
    check("a user's own --nodelist (job 8) and a fresh pin (job 9) are left alone",
          all("jobid=8" not in a and "jobid=9" not in a for a in applied))
    tries = []
    def flaky(argv, timeout=10.0):
        tries.append(argv)
        if len(tries) == 1:
            raise slurm.SlurmError("rc=1 Resource temporarily unavailable for job 7")
        return True
    slurm.apply = flaky
    check("Slurm's EAGAIN on a job update is retried",
          daemon.Daemon.apply_retrying(["scontrol"], wait=0) and len(tries) == 2)
    slurm.apply = lambda argv, timeout=10.0: (_ for _ in ()).throw(
        slurm.SlurmError("rc=1 Job is no longer pending execution for job 7"))
    check("a job that started meanwhile counts as released",
          daemon.Daemon.apply_retrying(["scontrol"], wait=0))
finally:
    slurm.admin_comment, slurm.apply = real_ac, real_apply
    slurm.set_actuation(False)

print("\n14. powered-down nodes are usable; drained and down ones are not")
check("power saving is not down", slurm._up("IDLE+CLOUD+POWERED_DOWN") and slurm._up("IDLE~"))
check("down, drained and failing are down",
      not any(slurm._up(x) for x in ("DOWN*", "MIXED+DRAIN", "IDLE+DRAIN", "FAILING", "INVAL")))

print("\n15. output paths follow the ones given on the command line")
def paths(state=None, log=None, text=None):
    g = dict(config.defaults()["general"])
    daemon.cli_paths(g, state, log, text)
    return g["state_dir"], g["log_file"], g["text_log"]
check("--state-dir alone puts everything there",
      paths(state="/h/u/dry") == ("/h/u/dry", "/h/u/dry/decisions.jsonl", "/h/u/dry/sjp.log"))
check("--log-file alone puts the text log beside it",
      paths(state="/h/u/s", log="/h/u/l/d.jsonl")[1:] == ("/h/u/l/d.jsonl", "/h/u/l/sjp.log"))
check("--text-log alone puts the JSON log beside it",
      paths(text="/h/u/t/x.log")[1:] == ("/h/u/t/decisions.jsonl", "/h/u/t/x.log"))
check("--text-log '' still turns the text log off", paths(state="/h/u/s", text="")[2] == "")
check("nothing given keeps the config's paths",
      paths() == ("/run/sjp", "/var/log/sjp/decisions.jsonl", "/var/log/sjp/sjp.log"))

check("no test started a process", subprocess.run is _no_processes)

print(f"\n{'ALL PASS' if not fails else 'FAILURES: ' + ', '.join(fails)}")
sys.exit(1 if fails else 0)
