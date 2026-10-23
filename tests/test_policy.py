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

print("\n10h. with [limits] count_pending, jobs waiting for room count against the idle share")
real_q = slurm.qos_cpu_limits
slurm.qos_cpu_limits = lambda qos: (864, 1760)
try:
    shares = {}
    for on in (False, True):
        cfg = config.defaults()
        cfg["limits"]["mode"], cfg["limits"]["count_pending"] = "global", on
        cfg["general"]["state_dir"] = tempfile.mkdtemp()
        d = daemon.Daemon(cfg); d.log_fh = io.StringIO()
        d.sh.nodes, d.sh.parts = nodes, {p: dict(tier=1, state="UP") for p in TOTAL}
        total_cpu = sum(c for v in TOTAL.values() for c, _ in v)
        d.sh.pending = [dict(jobid=str(i), user="u", cpus=total_cpu // 10, mem=8192, req_mem=8192,
                             qos="normal", reason=r, partition="slim1", state="PD", req_nodes="")
                        for i, r in enumerate(["Resources"] * 6 + ["Priority"] * 6
                                              + ["Dependency", "QOSMaxCpuPerUserLimit"])]
        d.act_once()
        shares[on] = json.load(open(d.status_path))["idle_fraction"]
    check("an idle cluster with a long queue waiting for room is not idle with count_pending",
          shares[False] > 0.5 and shares[True] == 0.0, str(shares))
finally:
    slurm.qos_cpu_limits = real_q

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
    return dict(dict(jobid=jid, id=jid, state="PD", reason=reason, partition=part, cpus=cpus,
                     mem=mem, req_mem=mem, eligible=now - waited_h * 3600, gpu=False,
                     req_nodes=""), **kw)
jobs = [pjob("1", "slim1", 5),                     # slim, over its 4 h budget
        pjob("2", "slim1", 2),                     # within budget
        pjob("3", "fat1", 2, cpus=1, mem=65536),   # fat, over its 1 h budget
        pjob("4", "slim1", 9, reason="QOSMaxCpuPerUserLimit"),   # widening cannot help
        pjob("5_3", "slim1", 9, id="5", array="5"),  # an array task: a job like any other
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
      sorted(w) == ["1", "3", "5_3"], str(sorted(w)))
check("an array task is widened on its own, by its task ID",
      w["5_3"]["cmd"].startswith("scontrol update jobid=5_3 partition="), w["5_3"]["cmd"])
check("widened to every partition that can hold the job",
      w["1"]["after"] == ",".join(sorted(policy.feasible(8, 32768, TOTAL))), w["1"]["after"])
check("a dry run only says what it would do", not w["1"]["executed"]
      and w["1"]["cmd"].startswith("scontrol update jobid=1 partition="))
d.widen_starving(jobs, now)
check("each job is considered once", d.log_fh.getvalue().count("widen_partitions") == 3)

print("\n10g. the recheck moves pending jobs whose choice changed, as many as fit")
cfg = config.defaults()
d = daemon.Daemon(cfg); d.log_fh = io.StringIO()
d.snap = dict(total=TOTAL, table={}, cap={}, later=None, node_free={
    "s1": (0, 0, ("slim1",)), "s2": (32, 100000, ("slim2",)), "f1": (24, 200000, ("fat1",))})
real_acs, real_lookup = slurm.admin_comments, policy.plugin_lookup
slurm.admin_comments = lambda: {str(k): "" if k == 3 else "sjp:placed=slim1" for k in range(1, 9)}
policy.plugin_lookup = lambda *a: (["fat1"], (0, 0, 0), False)
def rjob(jid, part="slim1", waited_min=10, prio=10, **kw):
    return dict(dict(jobid=jid, id=jid, state="PD", reason="Resources", partition=part, cpus=8,
                     mem=32768, req_mem=32768, eligible=now - waited_min * 60, gpu=False,
                     req_nodes="", priority=prio, timelimit=60), **kw)
jobs = [rjob("6", prio=10),                    # no room left once 1, 2 and 4 have moved
        rjob("1", prio=30), rjob("4", prio=20),
        rjob("2", waited_min=0, reason="None", prio=25),  # just submitted: rechecked too
        rjob("3"),                             # placed by the site's own rule
        rjob("5", part="fat1"),                # already where sjp would put it
        rjob("7", reason="Dependency"),        # cannot start yet anyway
        rjob("8", waited_min=300)]             # past its budget: the starvation guard's
try:
    d.recheck(jobs, now)
finally:
    slurm.admin_comments, policy.plugin_lookup = real_acs, real_lookup
recs = [json.loads(l) for l in d.log_fh.getvalue().splitlines()]
mv = {r["jobid"]: r for r in recs if r.get("action") == "move_partitions"}
check("pending jobs move, highest priority first, only as many as fit",
      sorted(mv) == ["1", "2", "4"], str(sorted(mv)))
check("the move is marked in AdminComment",
      "admincomment=sjp:placed=slim1;moved=slim1>fat1" in mv["1"]["cmd"] and mv["1"]["node"] == "f1",
      mv["1"]["cmd"])
d = daemon.Daemon(config.defaults()); d.log_fh = io.StringIO()
d.snap = dict(total=TOTAL, table={}, cap={}, later=None, node_free={
    "s1": (0, 0, ("slim1",)), "f1": (24, 200000, ("fat1",))})
slurm.admin_comments = lambda: {"3": "sjp:skipped=no room;job=8c,32G,4.0G/c", "4": "",
                                "5": "sjp:skipped=multi-node"}
policy.plugin_lookup = lambda *a: (["fat1"], (0, 0, 0), False)
try:
    d.recheck([rjob("3"), rjob("4"), rjob("5")], now)
finally:
    slurm.admin_comments, policy.plugin_lookup = real_acs, real_lookup
mv = {r["jobid"]: r for r in map(json.loads, d.log_fh.getvalue().splitlines())
      if r.get("action") == "move_partitions"}
check("a job the plugin skipped for want of room is taken over once room opens, and marked "
      "sjp's; not one it never saw, nor one skipped for another reason", sorted(mv) == ["3"]
      and "admincomment=sjp:placed=fat1;job=8c,32G,4.0G/c;moved=slim1>fat1" in mv["3"]["cmd"],
      str({k: v["cmd"] for k, v in mv.items()}))

print("\n10i. an error is reported until the same read works again")
sh = daemon.Shared()
first = sh.note_error("queue", "timed out")
again = sh.note_error("queue", "timed out")
sh.clear_error("queue")
check("each distinct failure is logged once, and cleared once it works",
      first and not again and sh.errors == {} and sh.note_error("queue", "timed out"))

print("\n10f. a setting's old name keeps working")
import tomllib as _t
old = config._merge(config.DEFAULTS, config.upgrade(_t.loads("[limits]\nraise_above = 0.1\n")))
check("raise_above becomes min_idle_share, and sjpd is told",
      old["limits"]["min_idle_share"] == 0.1 and "raise_above" not in old["limits"]
      and old["_renamed"] == ["[limits] raise_above is now min_idle_share"])

print("\n10e. flex mode moves capped jobs that fit now to the flex QOS, and back")
cfg = config.defaults()
cfg["limits"]["mode"] = "flex"
cfg["general"]["state_dir"] = tempfile.mkdtemp()
d = daemon.Daemon(cfg); d.log_fh = io.StringIO(); d.mode = "enforce"
notes = {"1": "sjp:placed", "2": "", "3": "", "4": "", "5": "sjp:placed;qos=normal>flex",
         "6": ""}
ran = []
real_ac, real_apply = slurm.admin_comment, slurm.apply
real_uq, real_cn, real_ql = slurm.user_qos, slurm.cluster_name, slurm.qos_cpu_limits
flex_caps = {"flex": (None, None)}
slurm.qos_cpu_limits = lambda qos: flex_caps.get(qos, (None, None))
assoc = {("u", "a"): {"normal", "flex"}, ("v", "a"): {"normal"}}
slurm.user_qos = lambda cluster: {k: set(v) for k, v in assoc.items()}
slurm.cluster_name = lambda: "c"
slurm.admin_comment = lambda jid: notes[jid]
slurm.apply = lambda argv, timeout=10.0: ran.append(argv) or True
def fj(jid, cpus, prio, reason="QOSMaxCpuPerUserLimit", qos="normal", **kw):
    return dict(dict(jobid=jid, id=jid, cpus=cpus, mem=cpus * 4000, req_mem=cpus * 4000,
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
    # The flex QOS's own caps: a job it has no room for is not moved into it,
    # where it would only wait and be moved back; once the caps are raised, it is.
    d = daemon.Daemon(cfg); d.log_fh = io.StringIO(); d.mode = "enforce"
    flex_caps["flex"] = (40, None)
    ran.clear()
    notes.update({"20": "", "21": "", "22": ""})
    running = [dict(fj("19", 32, 0, qos="flex"), state="R")]
    d.flex([fj("20", 16, 9), fj("21", 8, 8), fj("22", 4, 7)], nf10, 2000.0, None, running)
    check("only what the flex QOS's per-user cap still has room for moves",
          [a[2] for a in ran] == ["jobid=21"], str([a[2] for a in ran]))
    d.qos_caps.clear(); flex_caps["flex"] = (80, None); ran.clear()
    d.flex([fj("20", 16, 9), fj("22", 4, 7)], nf10, 2001.0, None, running)
    check("raise the flex caps and the waiting jobs move, counting the one moved before",
          [a[2] for a in ran] == ["jobid=20", "jobid=22"], str([a[2] for a in ran]))
    # Held by the cap while pinned to a full node to wait for room: the job
    # moves where there is room now, and its pin goes. The user's own
    # --nodelist stays.
    d = daemon.Daemon(cfg); d.log_fh = io.StringIO(); d.mode = "enforce"
    flex_caps["flex"] = (None, None); ran.clear()
    notes.update({"30": "sjp:pin=c;from=slim1,slim2;eta=236h;job=8c", "31": ""})
    nf30 = dict(nf10, c=(-8, 0, ["slim2"]))
    d.flex([fj("30", 8, 9, req_nodes="c", partition="slim2"),
            fj("31", 8, 8, req_nodes="a")], nf30, 3000.0)
    check("a job sjp pinned to a full node moves to the flex QOS, without the pin",
          [a[2:] for a in ran] == [
              ["jobid=30", "reqnodelist="],
              ["jobid=30", "partition=slim1,slim2",
               "admincomment=sjp:released=c;from=slim1,slim2;job=8c"],
              ["jobid=30", "qos=flex",
               "admincomment=sjp:released=c;from=slim1,slim2;job=8c;qos=normal>flex"]],
          str(ran))
    reads = []
    slurm.admin_comment = lambda jid: reads.append(jid) or notes[jid]
    d.flex([fj("31", 8, 8, req_nodes="a")], nf30, 3001.0)
    check("a --nodelist on a job sjp did not place is left alone, and not read again each poll",
          len(ran) == 3 and reads == [], str(reads))
    slurm.admin_comment = lambda jid: notes[jid]
    # One on a job sjp placed is sjp's to drop ([policy] user_nodes = ignore),
    # or the user's to keep (follow).
    notes["32"] = "sjp:placed=slim1;job=8c"
    for how, want in (("follow", []), ("ignore", [["jobid=32", "reqnodelist="]])):
        cfg["policy"]["user_nodes"] = how
        d = daemon.Daemon(cfg); d.log_fh = io.StringIO(); d.mode = "enforce"; ran.clear()
        d.flex([fj("32", 8, 9, req_nodes="c")], nf30, 4000.0)
        check(f"user_nodes = {how}: a --nodelist on a job sjp placed is "
              + ("kept" if how == "follow" else "dropped with the move"),
              [a[2:] for a in ran][:1] == want, str(ran))
finally:
    slurm.admin_comment, slurm.apply = real_ac, real_apply
    slurm.user_qos, slurm.cluster_name, slurm.qos_cpu_limits = real_uq, real_cn, real_ql
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
check("an array task waiting on its own can be pinned like any other job",
      policy.pin_eligible(dict(jobid="5_1", nnodes=1, req_nodes="", reason="None")) is None)
check("multi-node, user nodelists and held jobs are never pinned",
      all(policy.pin_eligible(dict(jobid=i, nnodes=n, req_nodes=r, reason=why))
          for i, n, r, why in (("5", 2, "", "None"),
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

print("\n12c. the 3D view: what the page is given, and the demo")
from sjp import viz
vnodes = {"s1": dict(cpus=96, mem=393216, partitions=["slim"], gpu=False, up=True),
          "g1": dict(cpus=64, mem=262144, partitions=["gpu"], gpu=True, up=True),
          "i1": dict(cpus=64, mem=262144, partitions=["interactive"], gpu=False, up=True)}
vparts = {"slim": dict(tier=10), "gpu": dict(tier=1), "interactive": dict(tier=1)}
vjobs = [dict(jobid="7", user="u", name="a", qos="normal", partition="slim", timelimit=60,
              state="R", nodelist="s1", cpus=4, mem=8192, req_mem=8192, end=1000.0),
         dict(jobid="8", user="u", name="b", qos="normal", partition="gpu", timelimit=60,
              state="R", nodelist="g1", cpus=4, mem=8192, req_mem=8192, end=1000.0),
         dict(jobid="9", user="u", name="c", qos="normal", partition="slim", timelimit=60,
              state="PD", nodelist="", cpus=2, mem=4096, req_mem=4096, reason="Priority",
              priority=5.0)]
vnodes["g1"].update(gpus=4, gpu_type="a10")
vjobs[1]["gpus"] = 2
vs = viz.build_state(config.defaults(), vnodes, vparts, vjobs, 500.0)
check("the view gets every node, those sjp does not place on (GPU, interactive) marked",
      {n["name"]: n["managed"] for n in vs["nodes"]} == {"g1": False, "i1": False, "s1": True})
check("and every running and waiting job, marked the same way",
      {j["id"]: j["managed"] for j in vs["running"] + vs["pending"]} == {"7": True, "8": False, "9": True})
check("with a node's GPUs and those a job holds there",
      next(n for n in vs["nodes"] if n["name"] == "g1")["gpus"] == 4
      and next(j for j in vs["running"] if j["id"] == "8")["gpus"] == 2)
check("GPUs counted from CfgTRES, else from Gres, with their type",
      slurm._gpus("gpu:a10:4(S:0-1)", "cpu=64") == dict(gpus=4, gpu_type="a10")
      and slurm._gpus("(null)", "cpu=64,gres/gpu=2,gres/gpu:a10=2") == dict(gpus=2, gpu_type="")
      and slurm._gpus("(null)", "cpu=64") == dict(gpus=0, gpu_type=""))
pj = [dict(vjobs[2], jobid=str(i), priority=float(i), req_nodes=rn)
      for i, rn in ((20, "s1"), (21, ""), (22, "g1"), (23, "s1,s2"))]
vs = viz.build_state(config.defaults(), vnodes, vparts, pj, 500.0)
pin = {j["id"]: j["pinned"] for j in vs["pending"]}
check("a waiting job tied to one node is drawn there; others queue as usual",
      pin == {"20": "s1", "21": "", "22": "g1", "23": ""} and vs["queued_total"] == 2, str(pin))
demo = viz.Demo(config.defaults(), speed=900, seed=2)
for _ in range(300):
    demo.step(0.1)
ds = demo.get()
over = [n["name"] for n in ds["nodes"]
        if sum(j["cpus"] for j in ds["running"] if j["node"] == n["name"]) > n["cpus"]
        or sum(j["mem"] for j in ds["running"] if j["node"] == n["name"]) > n["mem"]]
check("the demo keeps its cluster busy without overfilling a node",
      len(ds["running"]) > 20 and not over, f"{len(ds['running'])} running, overfull: {over}")
demo = viz.Demo(config.defaults(), speed=900, seed=2, interactive=1, gpu=1)
for _ in range(300):
    demo.step(0.1)
ds = demo.get()
on = lambda n: [j for j in ds["running"] if j["node"] == n["name"]]
others = [n for n in ds["nodes"] if not n["managed"]]
check("the demo's interactive and GPU nodes run their own jobs, within their CPUs and GPUs",
      sorted(n["name"] for n in others) == ["gpu01", "int01"]
      and all(on(n) and sum(j["cpus"] for j in on(n)) <= n["cpus"]
              and sum(j["gpus"] for j in on(n)) <= n["gpus"] for n in others)
      and all(not j["managed"] for n in others for j in on(n))
      and not any(j["node"] in ("gpu01", "int01") for j in ds["running"] if j["managed"]),
      str([(n["name"], len(on(n))) for n in others]))

vn = {"a": dict(cpus=64, threads=2), "b": dict(cpus=64, threads=1), "c": dict(cpus=64)}
shown = {"a": {}, "b": {}}
viz.reservations(shown, vn, [
    dict(name="maint", start=0, end=200, nodes={"a": None}),
    dict(name="course", start=0, end=200, nodes={"b": 16}),
    dict(name="later", start=150, end=300, nodes={"a": None, "c": None}),
    dict(name="over", start=0, end=50, nodes={"b": None})],
    [dict(state="R", reservation="course", nodelist="b", cpus=8)], 100)
check("the visualizer is told each node's reservations, now or to come, and what they hold free",
      [(r["name"], r["cpus"], r["whole"], r["active"], r["left"]) for r in shown["a"]["resv"]]
      == [("maint", 64, True, True, 64), ("later", 64, True, False, 64)]
      and [(r["name"], r["cpus"], r["left"]) for r in shown["b"]["resv"]] == [("course", 16, 8)],
      str(shown))
q = [dict(id="12", priority=900), dict(id="9_[1-4]", priority=1500),
     dict(id="10", priority=900), dict(id="9_7", priority=1500), dict(id="100", priority=900)]
check("waiting jobs are listed as squeue lists them: priority, then job ID",
      [j["id"] for j in sorted(q, key=viz.queue_order)] == ["9_7", "9_[1-4]", "10", "12", "100"],
      str([j["id"] for j in sorted(q, key=viz.queue_order)]))
check("the 2D page is shipped too, and can be driven frame by frame",
      os.path.exists(viz.HTML_2D) and "window.sjpViz" in open(viz.HTML_2D).read())

print("\n12g. what running jobs use: sstat, read and kept as a history")
real_run = slurm._run
sstat_out = ("7.extern|energy=0|energy=0\n"
             "7.batch|cpu=00:01:40,energy=0,mem=1073741824,pages=0|cpu=00:01:40,mem=2147483648\n"
             "7.0|cpu=1-00:00:00,mem=1073741824|cpu=1-00:00:00,mem=1610612736\n"
             "12_3.batch|cpu=02:03.5,mem=524288000|cpu=02:03.5,mem=524288000\n")
asked = []
slurm._run = lambda args, timeout=10.0: asked.append(args) or sstat_out
try:
    use = slurm.usage(["7", "12_3", "99"])
finally:
    slurm._run = real_run
check("sstat: CPU time and memory summed over a job's steps, the peak its largest step's",
      use == {"7": (86500.0, 2048, 2048), "12_3": (123.5, 500, 500)}
      and asked[0][:2] == ["sstat", "-a"] and "7,12_3,99" in asked[0], str(use))
check("sstat is a read-only command, allowed in a dry run", slurm._read_only(["sstat", "-j", "1"]))
ul = viz.UsageLog()
ul.add(1000.0, {"7": (100.0, 512, 600)}, {"7": 900.0})
check("one sample: the average since the start, no history yet",
      ul.summary("7") == dict(now=None, avg=1.0, mem=512, peak=600) and ul.history("7") == [])
ul.add(1060.0, {"7": (340.0, 700, 650)}, {"7": 900.0})
check("the next: CPUs kept busy in between, and the peak seen so far",
      ul.history("7") == [(1060, 4.0, 700)] and ul.summary("7")["peak"] == 650
      and ul.summary("7")["now"] == 4.0, str(ul.history("7")))
for i in range(2 * viz.USE_POINTS):
    ul.add(1120.0 + 60 * i, {"7": (400.0 + 60 * i, 700, 650)}, {"7": 900.0})
check("every sample is kept; a long job's graph gets them averaged, up to the newest",
      len(ul.jobs["7"]["t"]) == 2 * viz.USE_POINTS + 1 and len(ul.history("7")) == viz.USE_POINTS
      and ul.history("7")[-1][:2] == (1120 + 60 * (2 * viz.USE_POINTS - 1), 1.0))
ul.add(9e9, {}, {})
check("a job that ended is forgotten", ul.summary("7") is None)
# Kept in a file: read back at the start, ended jobs dropped from it.
upath = os.path.join(tempfile.mkdtemp(), "usage.jsonl")
ul = viz.UsageLog(upath)
for t, used in ((1000.0, {"7": (100.0, 512, 600), "8": (5.0, 64, 64)}),
                (1060.0, {"7": (340.0, 700, 650), "8": (35.0, 64, 64)})):
    ul.add(t, used, {"7": 900.0, "8": 950.0})
back = viz.UsageLog(upath)
check("the history outlives a restart", back.history("7") == ul.history("7") == [(1060, 4.0, 700)]
      and back.summary("8") == ul.summary("8"), str(back.history("7")))
back.add(1120.0, {"7": (400.0, 700, 650)}, {"7": 900.0})
check("jobs that ended are dropped from the file",
      all(set(json.loads(l)["used"]) == {"7"} for l in open(upath))
      and viz.UsageLog(upath).history("7") == back.history("7"))
os.chmod(os.path.dirname(upath), 0o500)
try:
    ro = viz.UsageLog(os.path.join(os.path.dirname(upath), "new.jsonl"))
    import contextlib
    with contextlib.redirect_stderr(io.StringIO()):
        ro.add(1000.0, {"7": (100.0, 512, 600)}, {"7": 900.0})
    check("a file that cannot be written leaves the history in memory",
          ro.summary("7") is not None and ro.path == "" and "writing" in ro.error)
finally:
    os.chmod(os.path.dirname(upath), 0o700)
dj = next(j for j in ds["running"] if j["use"]["avg"] is not None)
hist = demo.history(dj["id"])
check("the demo makes up usage within each job's allocation, the same at every look",
      0 <= dj["use"]["avg"] <= dj["cpus"] and dj["use"]["peak"] <= dj["mem"]
      and len(hist) > 1 and all(0 <= c <= dj["cpus"] and m <= dj["mem"] for _, c, m in hist)
      and demo.history(dj["id"]) == hist)
check("the page's usage panel is shipped and loaded by both views",
      os.path.exists(viz.USE_JS) and all('src="usage.js"' in open(p).read()
                                          for p in (viz.HTML, viz.HTML_2D)))

print("\n12d. a burst of pins: pending jobs tied to a node count as taken there")
cn = {"a": rnode(96, 384000), "b": rnode(96, 384000)}
daemon.Daemon.claim(cn, [
    dict(state="PD", req_nodes="a", nnodes=1, reason="Resources", cpus=8, mem=32000, req_mem=32000),
    dict(state="PD", req_nodes="a", nnodes=1, reason="Dependency", cpus=8, mem=32000, req_mem=32000),
    dict(state="PD", req_nodes="a,b", nnodes=1, reason="None", cpus=8, mem=32000, req_mem=32000),
    dict(state="R", req_nodes="b", nnodes=1, reason="None", cpus=8, mem=32000, req_mem=32000)])
check("a pending job pinned to one node is counted there; held, waiting or spread ones are not",
      cn["a"]["alloc_cpus"] == 8 and cn["a"]["alloc_mem"] == 32000 and cn["b"]["alloc_cpus"] == 0)
check("the table tells the plugin which queue read the free space includes",
      "queue_at = 1234," in policy.render_lua(tbl, CFG, time.time(), 1, TOTAL,
                                              dict(nodes=nf, tiers=TIERS, room=None, queue_at=1234.5)))

print("\n12e. always_place: jobs no node has room for are placed by shape and wait for a node")
from sjp import narrate
now = 1_000_000.0
nf = {"a": (4, 16000, ["slim1"]), "b": (-8, -32000, ["slim1"]), "c": (0, 0, ["fat1"])}
ends = {"a": [(now + 3600, 8, 32000)], "b": [(now + 600, 12, 48000)],
        "c": [(now + 60, 32, 900000)]}
node, parts, eta = policy.soonest_node(8, 32000, ["slim1"], nf, ends, now, {"slim1": 10})
check("the node expected to have room first, counting the jobs already waiting there",
      (node, parts, eta) == ("a", ["slim1"], 3600), str((node, parts, eta)))
node, parts, eta = policy.soonest_node(8, 32000, ["slim1", "fat1"], nf, ends, now,
                                       {"slim1": 10, "fat1": 5})
check("whichever partition it is in", (node, eta) == ("c", 60), str((node, eta)))
check("no node at all when none can ever hold it",
      policy.soonest_node(64, 32000, ["slim1"], nf, ends, now, {})[0] is None)
ej = [dict(state="R", nodelist="a,b", cpus=16, mem=8000, req_mem=8000, end=now + 125),
      dict(state="R", nodelist="a", cpus=4, mem=1000, req_mem=1000, end=now + 130),
      dict(state="PD", nodelist="", cpus=4, mem=1000, end=None)]
check("running jobs' end times per node, split over their nodes, by the minute",
      daemon.Daemon.ends(ej, now) == {"a": [(1000080, 12, 9000)], "b": [(1000080, 8, 8000)]},
      str(daemon.Daemon.ends(ej, now)))
full = policy.build_table(CFG, TOTAL, TOTAL)
lua = policy.render_lua(tbl, CFG, now, 1, TOTAL, dict(nodes=nf, tiers=TIERS, room=None,
                                                    ends=ends), full)
check("the table carries the by-shape choices and the end times when asked",
      "always = true," in lua and "tf = {" in lua and "wait = true," in lua
      and '["b"] = {{1000600, 12, 48000}},' in lua)
check("and not otherwise", "always" not in policy.render_lua(tbl, CFG, now, 1, TOTAL))

cfg = config.defaults()
cfg["general"]["mode"] = "enforce"
cfg["policy"]["always_place"] = cfg["pin"]["wait_for_room"] = True
d = daemon.Daemon(cfg); d.log_fh = io.StringIO()
check("wait_for_room is in effect only with always_place, pins and rechecks", d.wait_active())
d.snap = dict(total=TOTAL, table={}, full={}, cap={}, later=None, tiers={"slim1": 10, "fat1": 5},
              # a: job 2 waits there already, and counts against it
              node_free={"a": (-8, -32768, ("slim1",)), "b": (0, 0, ("slim1",)),
                         "f": (16, 200000, ("fat1",))},
              ends={"a": [(now + 7200, 8, 64000)], "b": [(now + 1800, 8, 64000)]})
lookups = {"1": ["slim1"], "2": ["slim1"], "3": ["fat1"]}
real_acs, real_lookup, real_apply = slurm.admin_comments, policy.plugin_lookup, slurm.apply
notes = {"1": "sjp:placed=slim1;job=8c,32G,4.0G/c;v=3;room=no",
         "2": "sjp:pin=a;from=slim1;job=8c,32G,4.0G/c;v=3;room=no;eta=2.0h",
         "3": "sjp:pin=b;from=slim1;job=8c,32G,4.0G/c;v=3;room=no;eta=0.5h"}
slurm.admin_comments = lambda: notes
cur = {}
policy.plugin_lookup = lambda table, cap, cfg_, cpus, mem, minutes: (cur["would"], (0, 0, 0), False)
applied = []
slurm.apply = lambda argv, timeout=10.0: applied.append(argv) or True
def wjob(jid, req, prio):
    return dict(jobid=jid, id=jid, state="PD", reason="Resources", partition="slim1", cpus=8,
                mem=32768, req_mem=32768, eligible=now - 600, gpu=False, req_nodes=req,
                priority=prio, timelimit=60)
try:
    cur["would"] = ["slim1"]
    d.recheck([wjob("1", "", 30), wjob("2", "a", 20)], now)
    recs = [json.loads(l) for l in d.log_fh.getvalue().splitlines()]
    pins = {r["jobid"]: r for r in recs if r.get("action") == "pin_waiting"}
    check("a job with no room waits on the node expected to have room first; one already "
          "waiting where it should stays",
          {k: v["node"] for k, v in pins.items()} == {"1": "b"},
          str({k: v["node"] for k, v in pins.items()}))
    check("the pin is marked where the placement was, with the wait",
          applied[-1][-1] == "admincomment=sjp:pin=b;job=8c,32G,4.0G/c;v=3;room=no;from=slim1;eta=0.5h",
          applied[-1][-1])
    d.log_fh = io.StringIO(); applied.clear()
    cur["would"] = ["fat1"]
    d.recheck([wjob("3", "b", 10)], now)
    recs = [json.loads(l) for l in d.log_fh.getvalue().splitlines()]
    mv = [r for r in recs if r.get("action") == "move_partitions"]
    check("once room opens where sjp now chooses, the job moves there and stops waiting",
          len(mv) == 1 and mv[0]["after"] == "fat1" and applied[0][-1] == "reqnodelist="
          and applied[1][-1] == "admincomment=sjp:released=b;from=slim1;job=8c,32G,4.0G/c;v=3;room=no;moved=slim1>fat1",
          str(applied))
    # The table still chooses slim1 (for its bucket's typical shape), where no
    # node has room; fat1 holds the job and has room now.
    d.log_fh = io.StringIO(); applied.clear()
    d.snap["cap"] = {"slim1": (64, 256000), "fat1": (64, 1024000)}
    cur["would"] = ["slim1"]
    d.recheck([wjob("2", "a", 20)], now)
    recs = [json.loads(l) for l in d.log_fh.getvalue().splitlines()]
    mv = [r for r in recs if r.get("action") == "move_partitions"]
    check("a job waiting where the table chose moves to any partition with room for it now",
          len(mv) == 1 and mv[0]["after"] == "fat1" and mv[0]["node"] == "f", str(recs))
    check("and none can hold it: it keeps waiting",
          policy.room_elsewhere(8, 32768, {"slim1": (64, 256000), "fat1": (4, 1024000)},
                                d.snap["node_free"]) == [])
finally:
    slurm.admin_comments, policy.plugin_lookup, slurm.apply = real_acs, real_lookup, real_apply

ac = {"9": "sjp:pin=a;from=slim1;job=8c,32G,4.0G/c;v=3;room=no;eta=2.0h"}
real_ac, real_apply = slurm.admin_comment, slurm.apply
slurm.admin_comment = lambda jid: ac[jid]
slurm.apply = lambda argv, timeout=10.0: True
try:
    d = daemon.Daemon(cfg); d.log_fh = io.StringIO()
    j9 = dict(wjob("9", "a", 1), submit=now - 600)
    d.release_pins([j9], now)
    check("a job waiting for room keeps its pin", "release_pin" not in d.log_fh.getvalue()
          and "9" in d.waiting)
    d.release_pins([dict(j9, eligible=now - 5 * 3600)], now)
    check("until it is past its starvation budget",
          "release_pin" in d.log_fh.getvalue())
finally:
    slurm.admin_comment, slurm.apply = real_ac, real_apply

r = dict(ts=now, jobid="5", user="u", name="n", cpus=65, mem_mb=300 * 1024, minutes=2880,
         state="PD", reason="Priority", submit=now, actual="zen5,zen3", acted=True,
         placed=False, room=False, sjp_placed=False, pin=None, pin_why="not placed")
check("the log says who chose the partitions, and why sjp did not",
      "(your job_submit.lua; sjp did not place it: no node that could take it had room)"
      in narrate.job(r), narrate.job(r))
r.update(sjp_placed=True, sjp_by_shape=True, sjp_pin="n1", sjp_from="zen5", sjp_eta="3.2h",
         actual="zen5")
check("and for a job sjp placed by shape and pinned to wait",
      "(sjp's choice at submission; no node had room, so by its shape)" in narrate.job(r)
      and "expected to have room first (in 3.2h)" in narrate.job(r), narrate.job(r))

print("\n12f. jobs asking for nodes are placed among those nodes")
nfx = {"a": (8, 1, ["slim1"]), "b": (8, 1, ["slim1", "slim2"]), "c": (8, 1, ["fat1"])}
check("the nodes a job may use: those asked for, less those excluded",
      sorted(policy.eligible(nfx, skip=["a"])) == ["b", "c"]
      and sorted(policy.eligible(nfx, want=["b"])) == ["b"])
check("and its partitions: those holding them, every named node in each",
      policy.restrict(["fat1"], policy.eligible(nfx, want=["b"]), ["b"]) == ["slim1", "slim2"]
      and policy.restrict(["slim1", "fat1"], policy.eligible(nfx, skip=["c"])) == ["slim1"]
      and policy.restrict(["slim1"], {}) == [])
check("the plugin is told whether to follow a job's own nodes; by default it does not",
      'user_nodes = "ignore",' in policy.render_lua({}, CFG, 0, 1))

print("\n13. stale pins are released, and only sjp's own")
real_ac, real_apply = slurm.admin_comment, slurm.apply
comments = {"7": "sjp:pin=n16;from=slim1,slim2;job=4c,16G,4.0G/c;v=3", "8": ""}
slurm.admin_comment = lambda jid: comments[jid]
applied = []
slurm.apply = lambda argv, timeout=10.0: applied.append(argv) or True
try:
    now = time.time()
    jobs = [dict(jobid=i, id=i, state="PD", req_nodes=n, submit=now - t, reason="Resources")
            for i, n, t in (("7", "n16", 120), ("8", "n03", 120), ("9", "n16", 5))]
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
check("so are nodes not responding, in maintenance, rebooting, badly registered or unknown",
      not any(slurm._up(x) for x in ("IDLE+NOT_RESPONDING", "IDLE*", "IDLE+MAINTENANCE",
                                     "DOWN+REBOOT_ISSUED", "IDLE+INVALID_REG", "UNKNOWN",
                                     "FUTURE", "MIXED+BLOCKED", "ERROR")))
check("the visualizer names only what keeps jobs off a node",
      slurm.blocking("IDLE+CLOUD+DRAIN+POWERED_DOWN") == "DRAIN" and slurm.blocking("IDLE*")
      == "NOT_RESPONDING" and slurm.blocking("MIXED+RESERVED") == "")
check("but not reserved, rebooting later, completing, powering up or planned",
      all(slurm._up(x) for x in ("IDLE+RESERVED", "MIXED+REBOOT_REQUESTED",
                                 "MIXED+COMPLETING", "IDLE+POWERING_UP", "IDLE+PLANNED")))

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

print("\n16. job arrays: each task is a job of its own")
def qrow(rid, aid, state="PD", req="", node="", reason="Resources"):
    return (f"{rid}|u|a|4|16G|1:00:00|100|{reason}|normal|slim1|x|{state}|cpu=4,mem=16G,node=1|"
            f"{req}|{node}|2026-10-14T10:00:00|1|2026-10-14T10:00:00|(null)|N/A|(null)|(null)|{aid}|")
squeue = "\n".join([qrow("30", "30"), qrow("32", "1_0", "R", node="n1"),
                    qrow("1", "1_17"), qrow("1", "1_18"), qrow("18", "1_16", req="n3")])
scontrol = "\n".join(["JobId=32 ArrayJobId=1 ArrayTaskId=0 AdminComment=a",
                      "JobId=18 ArrayJobId=1 ArrayTaskId=16 AdminComment=sjp:pin=n3",
                      "JobId=1 ArrayJobId=1 ArrayTaskId=17-18 AdminComment=sjp:placed=slim1"])
asked = []
real_run = slurm._run
slurm._run = lambda args, timeout=10.0: asked.append(args) or (
    squeue if args[0] == "squeue" else scontrol)
try:
    q = slurm.queue()
    note1, notes = slurm.admin_comment("1"), slurm.admin_comments()
finally:
    slurm._run = real_run
check("squeue lists each array task (-r), named as scontrol updates it, with the record it is in",
      "-r" in asked[0] and [(j["jobid"], j["id"], j["array"]) for j in q] ==
      [("30", "30", ""), ("1_0", "32", "1"), ("1_17", "1", "1"), ("1_18", "1", "1"),
       ("1_16", "18", "1")], str([(j["jobid"], j["id"], j["array"]) for j in q]))
check("a record's AdminComment is its own, though scontrol lists the whole array",
      note1 == "sjp:placed=slim1" and notes["18"] == "sjp:pin=n3" and sorted(notes) == ["1", "18", "32"])

def ajob(t, **kw):
    return rjob(f"1_{t}", id="1", array="1", **kw)
d = daemon.Daemon(config.defaults()); d.log_fh = io.StringIO()
d.snap = dict(total=TOTAL, table={}, cap={}, later=None, node_free={
    "f1": (16, 200000, ("fat1",)), "s1": (0, 0, ("slim1",))})
real_acs, real_lookup = slurm.admin_comments, policy.plugin_lookup
slurm.admin_comments = lambda: {"1": "sjp:placed=slim1"}
policy.plugin_lookup = lambda *a: (["fat1"], (0, 0, 0), False)
try:
    d.recheck([ajob(t) for t in range(4)], time.time())
finally:
    slurm.admin_comments, policy.plugin_lookup = real_acs, real_lookup
mv = [r["jobid"] for r in map(json.loads, d.log_fh.getvalue().splitlines())
      if r.get("action") == "move_partitions"]
check("an array's waiting tasks move one by one, only as many as fit",
      mv == ["1_0", "1_1"], str(mv))

cfg = config.defaults()
cfg["general"]["mode"] = "enforce"
cfg["policy"]["always_place"] = cfg["pin"]["wait_for_room"] = True
d = daemon.Daemon(cfg); d.log_fh = io.StringIO()
now = time.time()
d.snap = dict(total=TOTAL, table={}, full={}, cap={}, later=None, tiers={"slim1": 10},
              node_free={"a": (0, 0, ("slim1",)), "b": (0, 0, ("slim1",))},
              ends={"a": [(now + 3600, 8, 32768)], "b": [(now + 1800, 8, 32768)]})
real_acs, real_lookup, real_apply = slurm.admin_comments, policy.plugin_lookup, slurm.apply
slurm.admin_comments = lambda: {"1": "sjp:placed=slim1;job=8c,32G,4.0G/c;v=3;room=no"}
policy.plugin_lookup = lambda *a: (["slim1"], (0, 0, 0), False)
applied = []
slurm.apply = lambda argv, timeout=10.0: applied.append(argv) or True
try:
    d.recheck([ajob(1, prio=20), ajob(2, prio=20)], now)
finally:
    slurm.admin_comments, policy.plugin_lookup, slurm.apply = real_acs, real_lookup, real_apply
pins = {r["jobid"]: r["node"] for r in map(json.loads, d.log_fh.getvalue().splitlines())
        if r.get("action") == "pin_waiting"}
check("waiting tasks are pinned one by one, each where room is expected for it first, "
      "counting those before it: once b's room is taken, the next waits for a",
      pins == {"1_1": "b", "1_2": "a"}
      and ["scontrol", "update", "jobid=1_1", "reqnodelist=b",
           "admincomment=sjp:pin=b;job=8c,32G,4.0G/c;v=3;room=no;from=slim1;eta=0.5h"] in applied,
      str(pins))

# The recheck read a newer queue than the node figures: they do not count the
# pin it made last time yet, so there is nothing of its own to give back.
d = daemon.Daemon(cfg); d.log_fh = io.StringIO()
d.snap = dict(d.snap if d.snap else {}, total=TOTAL, table={}, full={}, cap={}, later=None,
              tiers={"slim1": 10}, node_free={"b": (0, 0, ("slim1",))},
              ends={"b": [(now + 1800, 8, 32768)]}, claimed=set())
slurm.admin_comments = lambda: {"1": "sjp:pin=b;from=slim1;job=8c,32G,4.0G/c;v=3;room=no;eta=0.5h"}
policy.plugin_lookup = lambda *a: (["slim1"], (0, 0, 0), False)
try:
    d.snap["node_free"]["c"] = (0, 0, ("slim1",))         # as soon as b, but empty of pins
    d.snap["ends"]["c"] = [(now + 1800, 8, 32768)]
    d.recheck([ajob(1, req_nodes="b"), ajob(2, prio=5)], now)
    pins = [r["node"] for r in map(json.loads, d.log_fh.getvalue().splitlines())
            if r.get("action") == "pin_waiting"]
    check("and the next job waits elsewhere, not for the room that pin has taken",
          pins == ["c"], str(pins))
    d.log_fh = io.StringIO()
    d.snap["node_free"].pop("c"); d.snap["ends"].pop("c")
    d.snap["claimed"] = {"1_1"}
    d.snap["node_free"] = {"b": (-8, -32768, ("slim1",))}
    d.recheck([ajob(1, req_nodes="b")], now)
finally:
    slurm.admin_comments, policy.plugin_lookup = real_acs, real_lookup
acts = [r["action"] for r in map(json.loads, d.log_fh.getvalue().splitlines()) if r["event"] == "action"]
check("a pin the node figures do not count yet is counted, not given back as room: "
      "the job stays pinned", acts == [], str(acts))
d = daemon.Daemon(cfg); d.log_fh = io.StringIO()
d.snap = dict(d.snap if d.snap else {}, total=TOTAL, table={}, full={}, cap={}, later=None,
              tiers={"slim1": 10}, claimed={"1_1"},
              node_free={"a": (0, 0, ("slim1",)), "b": (-8, -32768, ("slim1",))},
              ends={"a": [(now + 1800, 8, 32768)], "b": [(now + 1800, 8, 32768)]})
slurm.admin_comments = lambda: {"1": "sjp:pin=b;from=slim1;job=8c,32G,4.0G/c;v=3;room=no;eta=0.5h"}
policy.plugin_lookup = lambda *a: (["slim1"], (0, 0, 0), False)
try:
    d.recheck([ajob(1, req_nodes="b")], now)
finally:
    slurm.admin_comments, policy.plugin_lookup = real_acs, real_lookup
acts = [r["action"] for r in map(json.loads, d.log_fh.getvalue().splitlines()) if r["event"] == "action"]
check("a waiting job stays on its node when another would have room no sooner", acts == [], str(acts))

# Small tasks share a node while it has room for them: by space, not spread for its own sake.
cfg = config.defaults()
cfg["general"]["mode"] = "enforce"
cfg["policy"]["always_place"] = cfg["pin"]["wait_for_room"] = True
d = daemon.Daemon(cfg); d.log_fh = io.StringIO()
d.snap = dict(total=TOTAL, table={}, full={}, cap={}, later=None, tiers={"slim1": 10}, claimed=set(),
              node_free={"a": (0, 0, ("slim1",)), "b": (0, 0, ("slim1",))},
              ends={"a": [(now + 1800, 24, 98304)], "b": [(now + 3600, 64, 262144)]})
slurm.admin_comments = lambda: {"1": "sjp:placed=slim1;job=8c,32G,4.0G/c;v=3;room=no"}
policy.plugin_lookup = lambda *a: (["slim1"], (0, 0, 0), False)
slurm.apply = lambda argv, timeout=10.0: True
try:
    d.recheck([ajob(t, prio=20) for t in range(1, 5)], now)
finally:
    slurm.admin_comments, policy.plugin_lookup, slurm.apply = real_acs, real_lookup, real_apply
pins = {r["jobid"]: r["node"] for r in map(json.loads, d.log_fh.getvalue().splitlines())
        if r.get("action") == "pin_waiting"}
check("small tasks wait together on the node with room for them first, as many as it has room "
      "for; only the next goes elsewhere",
      pins == {"1_1": "a", "1_2": "a", "1_3": "a", "1_4": "b"}, str(pins))

cfg = config.defaults()
cfg["limits"]["mode"] = "flex"
cfg["general"]["state_dir"] = tempfile.mkdtemp()
d = daemon.Daemon(cfg); d.log_fh = io.StringIO(); d.mode = "enforce"
ran = []
real_ac, real_apply = slurm.admin_comment, slurm.apply
real_uq, real_cn, real_ql = slurm.user_qos, slurm.cluster_name, slurm.qos_cpu_limits
slurm.user_qos = lambda cluster: {}
slurm.cluster_name = lambda: "c"
slurm.qos_cpu_limits = lambda qos: (None, None)
slurm.admin_comment = lambda jid: {"40": "sjp:placed=slim1"}[jid]
slurm.apply = lambda argv, timeout=10.0: ran.append(argv) or True
try:
    d.flex([fj(f"40_{t}", 16, 9, id="40", array="40") for t in (1, 2, 3)],
           {"a": (40, 512000, ["slim1"])}, 100.0)
finally:
    slurm.admin_comment, slurm.apply = real_ac, real_apply
    slurm.user_qos, slurm.cluster_name, slurm.qos_cpu_limits = real_uq, real_cn, real_ql
check("flex moves an array's capped tasks one by one, as many as fit",
      [a[2] for a in ran] == ["jobid=40_1", "jobid=40_2"], str(ran))

cfg = config.defaults()
cfg["general"]["state_dir"] = tempfile.mkdtemp()
d = daemon.Daemon(cfg); d.log_fh = io.StringIO()
d.score_once({f"{p}{i}": dict(cpus=c, mem=m, alloc_cpus=0, alloc_mem=0, partitions=[p], up=True)
              for p, v in TOTAL.items() for i, (c, m) in enumerate(v)},
             {p: dict(tier=1, state="UP") for p in TOTAL}, time.time())
aj = dict(user="u", name="x", cpus=4, mem=16384, req_mem=16384, qos="normal", reason="Resources",
          partition="slim1", timelimit=60, state="PD", array="5", id="5")
d.seen = set()
d.shadow([dict(aj, jobid="5_2"), dict(aj, jobid="5_3"), dict(aj, jobid="5_0", id="6", state="R",
                                                            nodelist="slim10"),
          dict(aj, jobid="9", id="9", array="")])
d.shadow([dict(aj, jobid="5_3"), dict(aj, jobid="5_2", id="7", state="R", nodelist="slim11")])
pl = [r for r in map(json.loads, d.log_fh.getvalue().splitlines()) if r["event"] == "placement"]
check("an array is shadowed once, as the one submission the plugin saw; it pins none",
      [r["jobid"] for r in pl] == ["5_2", "9"] and pl[0]["pin_why"].startswith("an array"),
      str([(r["jobid"], r["pin_why"]) for r in pl]))

check("no test started a process", subprocess.run is _no_processes)

print(f"\n{'ALL PASS' if not fails else 'FAILURES: ' + ', '.join(fails)}")
sys.exit(1 if fails else 0)
