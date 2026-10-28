"""Talking to Slurm.

Deliberately parses `scontrol ... --oneliner` key=value output rather than
`--json`. The JSON schemas move between releases; the key=value form has been
stable for many years and costs nothing to parse, so the same code works on
24.11 and 26.05 alike.
"""
from __future__ import annotations
import subprocess, shutil, shlex, time, re


class SlurmError(RuntimeError):
    pass


# The only commands that may run while actuation is off. Every process sjp
# starts goes through _run, so this is the last line of defence for a dry run
# under an account that Slurm would otherwise let change anything.
_READ_ONLY = {("scontrol", "show"), ("squeue",), ("sacctmgr", "-nP", "show"), ("sacct",),
              ("sstat",)}


def _read_only(args) -> bool:
    return any(tuple(args[:len(p)]) == p for p in _READ_ONLY)


def _run(args, timeout=10.0) -> str:
    if not _actuate and not _read_only(args):
        raise SlurmError(f"refused, actuation is off: {shlex.join(args)}")
    try:
        r = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    except (subprocess.TimeoutExpired, OSError) as e:
        raise SlurmError(f"{args[0]}: {e}") from e
    if r.returncode != 0:
        raise SlurmError(f"{' '.join(args)}: rc={r.returncode} {r.stderr.strip()[:200]}")
    return r.stdout


def _kv(line: str) -> dict:
    """'NodeName=a CPUTot=8 Reason=foo bar' -> dict. Last key may contain spaces."""
    out = {}
    for m in re.finditer(r"(\w+)=([^ ]*(?: (?![\w]+=)[^ ]*)*)", line):
        out[m.group(1)] = m.group(2).strip()
    return out


def available() -> bool:
    return shutil.which("scontrol") is not None


def nodes() -> dict:
    """name -> dict(cpus, mem, alloc_cpus, alloc_mem, partitions, up)."""
    out = {}
    for line in _run(["scontrol", "show", "nodes", "--oneliner"]).splitlines():
        d = _kv(line)
        name = d.get("NodeName")
        if not name:
            continue
        state = d.get("State", "")
        try:
            cpus = int(d.get("CPUTot", 0))
            mem = int(d.get("RealMemory", 0)) - int(d.get("MemSpecLimit", 0) or 0)
            out[name] = dict(
                cpus=cpus, mem=mem, threads=int(d.get("ThreadsPerCore", 1) or 1),
                alloc_cpus=int(d.get("CPUAlloc", 0)),
                alloc_mem=int(d.get("AllocMem", 0)),
                partitions=[p for p in d.get("Partitions", "").split(",") if p],
                gpu=_has_gpu(d.get("Gres", ""), d.get("CfgTRES", "")),
                **_gpus(d.get("Gres", ""), d.get("CfgTRES", "")),
                up=_up(state), state=state,
                # why an admin took it out, without Slurm's "[user@time]"
                reason=re.sub(r"\s*\[[^]]*\]\s*$", "", d.get("Reason", "")).strip(),
            )
        except ValueError:
            continue
    if not out:
        raise SlurmError("scontrol show nodes returned nothing parsable")
    return out


# Node states in which a node takes no new jobs, as `scontrol show node` prints
# them: a base state, then +flags. Matched as whole words: a substring test
# would count POWERED_DOWN (idle under power saving, resumed on demand) as
# DOWN. Not here: power saving states (Slurm resumes the node for a job),
# REBOOT_REQUESTED (it keeps taking jobs until the reboot), COMPLETING, PLANNED,
# and RESERVED, whose cores sjpd counts per reservation. The short forms are
# what sinfo, and older versions, print.
_UNUSABLE = {"DOWN", "ERROR", "FUTURE", "UNKNOWN",
             "DRAIN", "FAIL", "INVALID_REG", "NOT_RESPONDING", "MAINTENANCE",
             "REBOOT_ISSUED", "BLOCKED", "DYNAMIC_FUTURE",
             "DRAINED", "DRAINING", "FAILING", "INVAL", "MAINT"}


def blocking(state: str) -> str:
    """The parts of a node state that keep jobs off it: "IDLE+CLOUD+DRAIN" ->
    "DRAIN", "IDLE*" -> "NOT_RESPONDING"; "" for a node that takes jobs."""
    state = state.upper()
    words = [w for w in state.rstrip("*~#!%$@^-").split("+") if w in _UNUSABLE]
    if state.endswith("*") and "NOT_RESPONDING" not in words:
        words.append("NOT_RESPONDING")
    return "+".join(words)


def _up(state: str) -> bool:
    """'IDLE+CLOUD+POWERED_DOWN' -> True, 'MIXED+DRAIN' -> False, 'IDLE*'
    (not responding, in the short form) -> False."""
    state = state.upper()
    return not (state.endswith("*") or
                set(state.rstrip("*~#!%$@^-").split("+")) & _UNUSABLE)


def _has_gpu(gres: str, cfg_tres: str) -> bool:
    """'gpu:a10:1(S:0)' in Gres, or 'gres/gpu=1' in CfgTRES."""
    return (any(g.split(":")[0].strip().lower() == "gpu" for g in gres.split(","))
            or "gres/gpu" in cfg_tres)


def _gpu_tres(tres: dict) -> int:
    """GPUs in a TRES list as a dict: 'gres/gpu=2', or by type 'gres/gpu:a10=2'
    (summed) when there is no plain count."""
    try:
        if "gres/gpu" in tres:
            return int(tres["gres/gpu"])
        return sum(int(v) for k, v in tres.items() if k.startswith("gres/gpu:"))
    except ValueError:
        return 0


def _gpus(gres: str, cfg_tres: str) -> dict:
    """A node's GPUs: how many (from CfgTRES, else Gres 'gpu:a10:4(S:0)') and
    their type ('a10', or '' when not given)."""
    kinds = [g.split("(")[0].strip().split(":") for g in gres.split(",")]
    kinds = [k for k in kinds if k[0].lower() == "gpu"]
    gtype = next((k[1] for k in kinds if len(k) == 3), "")
    n = _gpu_tres(dict(kv.split("=", 1) for kv in cfg_tres.split(",") if "=" in kv))
    if not n:
        n = sum(int(k[-1]) for k in kinds if len(k) > 1 and k[-1].isdigit())
    return dict(gpus=n, gpu_type=gtype)


def partitions() -> dict:
    """name -> dict(tier, nodes, state)."""
    out = {}
    for line in _run(["scontrol", "show", "partitions", "--oneliner"]).splitlines():
        d = _kv(line)
        name = d.get("PartitionName")
        if not name:
            continue
        out[name] = dict(tier=int(d.get("PriorityTier", 1) or 1),
                         state=d.get("State", "UP"))
    return out


PENDING_FMT = ("JobID:|,UserName:|,Account:|,NumCPUs:|,MinMemory:|,"
               "TimeLimit:|,PriorityLong:|,Reason:|,QOS:|,Partition:|,Name:|")
# The rest is appended so the indices above stay put. tres-alloc is the
# *requested* TRES for a job that has not started; its mem is the job's total,
# which MinMemory is not when the job used --mem-per-cpu. JobID is the job's
# own record, which an array's waiting tasks share; JobArrayID names the job
# or array task ("12", "12_3").
QUEUE_FMT = PENDING_FMT + (",StateCompact:|,tres-alloc:|,ReqNodes:|,NodeList:|,"
                           "SubmitTime:|,NumTasks:|,EligibleTime:|,Feature:|,EndTime:|,Reservation:|,"
                           "ExcNodes:|,JobArrayID:|,LastSchedEval:|,StartTime:|")


def queue(states: str = "PD,R,CF") -> list[dict]:
    """Jobs in the given states, with the Reason that is nowhere in the accounting database.

    Each array task is a job of its own (squeue -r), whether or not Slurm has
    given it a record of its own yet:
      jobid   what scontrol updates it by: "12", or an array task "12_3" --
              never an array's own "12", which would change all its tasks
      id      the job's record, which `scontrol show job` prints as JobId: an
              array's waiting tasks share one, the array's job ID
      array   the array's job ID, "" for a job that is not one
    """
    txt = _run(["squeue", "-h", "-r", "-t", states, "-O", QUEUE_FMT], timeout=20.0)
    out = []
    for line in txt.splitlines():
        f = [x.strip() for x in line.split("|")]
        if len(f) < 23:
            continue
        try:
            tres = dict(kv.split("=", 1) for kv in f[12].split(",") if "=" in kv)
            nnodes = max(1, int(tres.get("node", 1) or 1))
            array, task, _ = f[22].partition("_")
            out.append(dict(jobid=f[22] or f[0], id=f[0], array=array if task else "",
                            user=f[1], account=f[2],
                            cpus=int(f[3] or 1), mem=_mem_mb(f[4]),
                            req_mem=_mem_mb(tres.get("mem", "")) // nnodes,
                            timelimit=_mins(f[5]), priority=int(float(f[6] or 0)),
                            reason=f[7], qos=f[8], partition=f[9], name=f[10],
                            state=f[11], nnodes=nnodes,
                            gpu=any(k.startswith("gres/gpu") for k in tres),
                            gpus=_gpu_tres(tres),
                            req_nodes=f[13], nodelist=f[14], submit=_epoch(f[15]),
                            ntasks=int(f[16] or 1), eligible=_epoch(f[17]),
                            features="" if f[18] == "(null)" else f[18],
                            end=_epoch(f[19]),
                            reservation="" if f[20] == "(null)" else f[20],
                            exc_nodes="" if f[21] == "(null)" else f[21],
                            # when Slurm's scheduler last looked at it (None: not yet)
                            last_eval=_epoch(f[23]) if len(f) > 23 else None,
                            # a running job's start (a waiting one's is Slurm's guess)
                            start=_epoch(f[24]) if len(f) > 24 else None))
        except ValueError:
            continue
    if txt.strip() and not out:
        raise SlurmError(f"squeue returned {len(txt.splitlines())} lines, none parsable")
    return out


def reservations() -> list[dict]:
    """Reservations that hold nodes: name, start, end (epoch seconds), and
    nodes: {node: reserved cores, or None for the whole node}. A reservation of
    part of a node lists its cores per node (NodeName=... CoreIDs=...)."""
    out = []
    for line in _run(["scontrol", "show", "reservations", "--oneliner"]).splitlines():
        d = _kv(line)
        start, end = _epoch(d.get("StartTime", "")), _epoch(d.get("EndTime", ""))
        names = expand_hostlist(d.get("Nodes", "")) if d.get("Nodes", "(null)") != "(null)" else []
        if not names or start is None or end is None:
            continue
        cores = {n: _count_ids(ids) for n, ids in
                 re.findall(r"NodeName=(\S+) CoreIDs=(\S+)", line)}
        out.append(dict(name=d.get("ReservationName", ""), start=start, end=end,
                        nodes={n: cores.get(n) for n in names},
                        **{k: "" if d.get(f, "(null)") == "(null)" else d[f] for k, f in
                           (("users", "Users"), ("accounts", "Accounts"), ("flags", "Flags"),
                            ("partition", "PartitionName"))}))
    return out


def _count_ids(ids: str) -> int:
    """'0-3,8' -> 5."""
    n = 0
    for r in ids.split(","):
        a, _, b = r.partition("-")
        n += int(b or a) - int(a) + 1
    return n


def expand_hostlist(s: str) -> list[str]:
    """'n[01-03,07],gpu1' -> ['n01', 'n02', 'n03', 'n07', 'gpu1']."""
    out = []
    for m in re.finditer(r"([^,\[]+)(?:\[([^\]]*)\])?([^,]*)", s):
        head, ranges, tail = m.groups()
        if not ranges:
            out.append(head + tail)
            continue
        for r in ranges.split(","):
            a, _, b = r.partition("-")
            for i in range(int(a), int(b or a) + 1):
                out.append(f"{head}{i:0{len(a)}d}{tail}")
    return out


def _epoch(s: str) -> float | None:
    """Slurm's local '2026-09-24T12:23:58' -> epoch seconds."""
    try:
        return time.mktime(time.strptime(s, "%Y-%m-%dT%H:%M:%S"))
    except ValueError:
        return None


def sjp_marks(comment: str) -> dict:
    """The marks sjp keeps in a job's AdminComment, from "sjp:" on:
    "sjp:pin=n1;from=a,b;job=4c,16G,4.0G/c" -> {"pin": "n1", "from": "a,b", "job": ...}."""
    i = comment.find("sjp:")
    if i < 0:
        return {}
    out = {}
    for part in comment[i + 4:].split(";"):
        k, _, v = part.partition("=")
        if k:
            out[k] = v
    return out


def set_mark(comment: str, key: str, value: str, drop=()) -> str:
    """comment with key=value in its sjp marks, replacing any earlier value of
    key, and without the keys in drop."""
    i = comment.find("sjp:")
    if i < 0:
        return f"{comment};sjp:{key}={value}" if comment else f"sjp:{key}={value}"
    gone = {key, *drop}
    parts = [p for p in comment[i + 4:].split(";") if p and p.partition("=")[0] not in gone]
    return comment[:i + 4] + ";".join(parts + [f"{key}={value}"])


def rename_mark(comment: str, old: str, new: str, value: str | None = None,
                drop=()) -> str:
    """comment with its sjp mark old renamed to new, in the same place (and
    given a new value, if one is given), and without the keys in drop:
    "sjp:pin=n1;from=a" -> "sjp:released=n1;from=a"."""
    i = comment.find("sjp:")
    if i < 0:
        return comment
    out = []
    for p in comment[i + 4:].split(";"):
        k, _, v = p.partition("=")
        if k in drop or not p:
            continue
        out.append(f"{new}={v if value is None else value}" if k == old else p)
    return comment[:i + 4] + ";".join(out)


def admin_comment(jobid: str) -> str:
    """A job's AdminComment, which squeue cannot print, by its record's job ID
    (a queue() job's id). For an array's job ID scontrol lists every task; the
    comment is the one on that record, which its waiting tasks share."""
    for line in _run(["scontrol", "show", "job", jobid, "--oneliner"]).splitlines():
        kv = _kv(line)
        if kv.get("JobId") == jobid:
            return kv.get("AdminComment", "")
    return ""


def admin_comments() -> dict:
    """Every job record's AdminComment, by its job ID (a queue() job's id): one
    call for the whole queue rather than one per job."""
    out = {}
    for line in _run(["scontrol", "show", "jobs", "--oneliner"], timeout=20.0).splitlines():
        kv = _kv(line)
        if "JobId" in kv:
            out[kv["JobId"]] = kv.get("AdminComment", "")
    return out


# Pending reasons, exactly as squeue prints them (checked against the 24.11 and
# 26.05 sources). CAP_REASONS are the holds the per-user/per-account CPU caps
# cause -- MaxTRESPU and MaxTRESPA -- which are the ones a limit pulse releases.
CAP_REASONS = {"QOSMaxCpuPerUserLimit", "MaxCpuPerAccount"}
LIMIT_REASONS = CAP_REASONS | {"AssocGrpCpuLimit", "QOSGrpCpuLimit",
                               "AssocMaxJobsLimit", "QOSMaxJobsPerUserLimit"}


def _mem_mb(s: str) -> int:
    if not s:
        return 0
    mult = {"K": 1 / 1024, "M": 1, "G": 1024, "T": 1024 * 1024}
    if s[-1].upper() in mult:
        return int(float(s[:-1]) * mult[s[-1].upper()])
    return int(float(s))


def _mins(s: str) -> int:
    if not s or s in ("UNLIMITED", "INVALID"):
        return 14 * 24 * 60
    days, _, rest = s.partition("-")
    if rest:
        h, m, *sec = rest.split(":")
        return int(days) * 1440 + int(h) * 60 + int(m)
    p = s.split(":")
    if len(p) == 3:
        return int(p[0]) * 60 + int(p[1])
    if len(p) == 2:
        return int(p[0])
    return int(float(s))


# ---------------------------------------------------------------- read-only
def cluster_name() -> str:
    for line in _run(["scontrol", "show", "config"]).splitlines():
        k, _, v = line.partition("=")
        if k.strip() == "ClusterName":
            return v.strip()
    return ""


def user_qos(cluster: str) -> dict:
    """(user, account) -> the QOS that user may use under that account, on this
    cluster, as sacctmgr shows them (inherited ones included)."""
    txt = _run(["sacctmgr", "-nP", "show", "assoc", "where", f"cluster={cluster}",
                "format=Account,User,QOS"], timeout=30.0)
    out = {}
    for line in txt.splitlines():
        acct, user, qos = (line.split("|") + ["", "", ""])[:3]
        if user:
            out[(user, acct)] = set(q for q in qos.split(",") if q)
    return out


def qos_exists(qos: str) -> bool:
    return bool(_run(["sacctmgr", "-nP", "show", "qos", f"name={qos}", "format=Name"],
                     timeout=30.0).strip())


def accounts() -> list[str]:
    """Every account in the Slurm database, sorted."""
    txt = _run(["sacctmgr", "-nP", "show", "account", "format=Account"])
    return sorted({a.strip() for a in txt.splitlines() if a.strip()})


def _seconds(s: str) -> float:
    """'1-02:03:04', '02:03:04' or '03:04.5' -> seconds."""
    days, _, rest = s.rpartition("-")
    secs = 0.0
    for part in rest.split(":"):
        secs = secs * 60 + float(part)
    return secs + (int(days) * 86400 if days else 0)


def usage(jobids: list[str], chunk: int = 200) -> dict:
    """job ID -> (CPU seconds used so far, memory in use MB, peak memory MB),
    over its running steps (the peak is its largest step's), from sstat. Needs
    JobAcctGatherType other than none; jobs without a running step are missing.
    Job IDs are the jobs' own (queue()'s id): sstat names an array task by
    one, never as "12_3". An array's job ID lists all its tasks, so a step
    may be listed twice; it counts once."""
    out, seen = {}, set()
    for i in range(0, len(jobids), chunk):
        txt = _run(["sstat", "-a", "-n", "-P", "--noconvert", "-j", ",".join(jobids[i:i + chunk]),
                    "-o", "JobID,TRESUsageInTot,TRESUsageInMax"], timeout=30.0)
        for line in txt.splitlines():
            step, _, rest = line.partition("|")
            tot, _, top = rest.partition("|")
            t = dict(kv.split("=", 1) for kv in tot.split(",") if "=" in kv)
            if "cpu" not in t or step in seen:
                continue                 # the extern step: nothing measured
            seen.add(step)
            m = dict(kv.split("=", 1) for kv in top.split(",") if "=" in kv)
            jid = step.rpartition(".")[0] or step
            cpu, mem, peak = out.get(jid, (0.0, 0, 0))
            out[jid] = (cpu + _seconds(t["cpu"]), mem + int(t.get("mem", 0)) // 2**20,
                        max(peak, int(m.get("mem", 0)) // 2**20))
    return out


def qos_cpu_limits(qos: str) -> tuple[int | None, int | None]:
    """Current (MaxTRESPU cpu, MaxTRESPA cpu) on a QOS; None where unset."""
    txt = _run(["sacctmgr", "-nP", "show", "qos", f"name={qos}",
                "format=MaxTRESPU,MaxTRESPA"], timeout=30.0)
    line = (txt.strip().splitlines() or [""])[0]
    u, _, a = line.partition("|")

    def cpu(s):
        m = re.search(r"(?:^|,)cpu=(\d+)", s)
        return int(m.group(1)) if m else None
    return cpu(u), cpu(a)


# ---------------------------------------------------------------- actions
# Every command that changes cluster state is built by a cmd_* function and run
# only through apply(). Actuation is OFF until the daemon turns it on in enforce
# mode, so a dry run cannot touch the cluster even if a caller forgets to check
# the mode: apply() then does nothing and reports that it did nothing.
_actuate = False


def set_actuation(on: bool) -> None:
    global _actuate
    _actuate = bool(on)


def actuation() -> bool:
    return _actuate


def apply(argv: list[str], timeout: float = 10.0) -> bool:
    """Run a state-changing command if actuation is on. True if it ran."""
    if not _actuate:
        return False
    _run(argv, timeout)
    return True


def cmdline(argv: list[str]) -> str:
    return shlex.join(argv)


def cmd_set_job_partitions(jobid: str, parts: list[str]) -> list[str]:
    return ["scontrol", "update", f"jobid={jobid}", f"partition={','.join(parts)}"]


def cmd_set_job_qos(jobid: str, qos: str, note: str | None = None) -> list[str]:
    argv = ["scontrol", "update", f"jobid={jobid}", f"qos={qos}"]
    return argv + [f"admincomment={note}"] if note is not None else argv


def cmd_pin(jobid: str, node: str, parts: str, note: str) -> list[list[str]]:
    """Pin a pending job to another node: drop any node requirement, set the
    partitions the node is in, then require the node. In that order, because
    Slurm checks each change against the job's other settings."""
    return [["scontrol", "update", f"jobid={jobid}", "reqnodelist="],
            ["scontrol", "update", f"jobid={jobid}", f"partition={parts}"],
            ["scontrol", "update", f"jobid={jobid}", f"reqnodelist={node}",
             f"admincomment={note}"]]


def cmd_release_pin(jobid: str, parts: str, note: str) -> list[list[str]]:
    """Undo a pin: drop the node requirement, then restore the partitions.

    Two commands, because Slurm checks new partitions against the node
    requirement still in place and refuses ones the pinned node is not in."""
    return [["scontrol", "update", f"jobid={jobid}", "reqnodelist="],
            ["scontrol", "update", f"jobid={jobid}", f"partition={parts}",
             f"admincomment={note}"]]


def cmd_set_qos_cpu_limits(qos: str, per_user: int, per_account: int) -> list[str]:
    """The global elastic lever. One number, the same for everyone."""
    return ["sacctmgr", "-i", "modify", "qos", qos, "set",
            f"MaxTRESPU=cpu={int(per_user)}", f"MaxTRESPA=cpu={int(per_account)}"]


