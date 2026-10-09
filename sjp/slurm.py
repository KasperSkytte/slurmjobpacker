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
_READ_ONLY = {("scontrol", "show"), ("squeue",), ("sacctmgr", "-nP", "show"), ("sacct",)}


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
# which MinMemory is not when the job used --mem-per-cpu.
QUEUE_FMT = PENDING_FMT + (",StateCompact:|,tres-alloc:|,ReqNodes:|,NodeList:|,"
                           "SubmitTime:|,NumTasks:|,EligibleTime:|,Feature:|,EndTime:|,Reservation:|,"
                           "ExcNodes:|")


def queue(states: str = "PD,R,CF") -> list[dict]:
    """Jobs in the given states, with the Reason that is nowhere in the accounting database."""
    txt = _run(["squeue", "-h", "-t", states, "-O", QUEUE_FMT], timeout=20.0)
    out = []
    for line in txt.splitlines():
        f = [x.strip() for x in line.split("|")]
        if len(f) < 22:
            continue
        try:
            tres = dict(kv.split("=", 1) for kv in f[12].split(",") if "=" in kv)
            nnodes = max(1, int(tres.get("node", 1) or 1))
            out.append(dict(jobid=f[0], user=f[1], account=f[2],
                            cpus=int(f[3] or 1), mem=_mem_mb(f[4]),
                            req_mem=_mem_mb(tres.get("mem", "")) // nnodes,
                            timelimit=_mins(f[5]), priority=int(float(f[6] or 0)),
                            reason=f[7], qos=f[8], partition=f[9], name=f[10],
                            state=f[11], nnodes=nnodes,
                            gpu=any(k.startswith("gres/gpu") for k in tres),
                            req_nodes=f[13], nodelist=f[14], submit=_epoch(f[15]),
                            ntasks=int(f[16] or 1), eligible=_epoch(f[17]),
                            features="" if f[18] == "(null)" else f[18],
                            end=_epoch(f[19]),
                            reservation="" if f[20] == "(null)" else f[20],
                            exc_nodes="" if f[21] == "(null)" else f[21]))
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
    """A job's AdminComment, which squeue cannot print."""
    txt = _run(["scontrol", "show", "job", jobid, "--oneliner"])
    return _kv(txt).get("AdminComment", "") if txt.strip() else ""


def admin_comments() -> dict:
    """Every job's AdminComment, by job ID as squeue prints it ("12", "12_3",
    "12_[4-9]"): one call for the whole queue rather than one per job."""
    out = {}
    for line in _run(["scontrol", "show", "jobs", "--oneliner"], timeout=20.0).splitlines():
        kv = _kv(line)
        if "JobId" not in kv:
            continue
        task = kv.get("ArrayTaskId", "")
        jid = kv["JobId"] if not task else \
            f"{kv['ArrayJobId']}_{task if task.isdigit() else '[' + task + ']'}"
        out[jid] = kv.get("AdminComment", "")
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


