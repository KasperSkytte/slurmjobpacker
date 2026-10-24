"""sjp-viz: the cluster in 3D, live in a web browser.

Every node is a box: CPUs across, memory up, time into the depth. Every running
job is a box inside it, as wide as its CPUs, as tall as its memory and as deep
as the time it has left. The jobs on a node are stacked corner to corner, so a
node is full when the stack reaches its far corner, and a node whose jobs ask
for the wrong mix of CPUs and memory runs out of one while the other is left.
Waiting jobs stand in a queue in front.

    python3 -m sjp.viz                      # the live cluster, on 127.0.0.1:8650
    python3 -m sjp.viz --demo               # a simulated cluster, sped up

It only reads from Slurm (scontrol show, squeue, sstat), like a dry run of sjpd, and
by default answers only on localhost: the page shows users and job names. To
see it from your own machine, use an SSH tunnel:

    ssh -L 8650:localhost:8650 <controller>      # then open http://localhost:8650
"""
from __future__ import annotations
import argparse, json, math, os, random, sys, threading, time
from array import array
from urllib.parse import parse_qs, urlparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import __version__, config, describe, policy, slurm

HTML = os.path.join(os.path.dirname(os.path.abspath(__file__)), "viz.html")
HTML_2D = os.path.join(os.path.dirname(os.path.abspath(__file__)), "viz2d.html")
USE_JS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "usage.js")
HORIZON = 14 * 86400          # the depth of a node: two weeks, Slurm's usual MaxTime
TOP = 50                      # waiting jobs the page shows unless asked for all (A)
MAX_WAITING = 5000            # waiting jobs sent to the page at most, highest priority first
USE_POINTS = 400              # a job's graph gets at most this many points, averaged


def queue_order(j):
    """squeue's order: highest priority first, then lowest job ID."""
    head = str(j["id"]).partition("_")[0]
    return -(j.get("priority") or 0), int(head) if head.isdigit() else 0, str(j["id"])


def shown_partitions(cfg, parts: dict, nodes: dict) -> list[str]:
    """The partitions sjp would place jobs in, by the same [topology] rules."""
    t = cfg["topology"]
    out = []
    for p in (t["batch_partitions"] or parts):
        members = [d for d in nodes.values() if p in d["partitions"]]
        if (p not in parts or p in t["exclude_partitions"]
                or (t["exclude_interactive"] and p.lower() == "interactive")):
            continue
        if t["exclude_gpu_nodes"] and members and all(d.get("gpu") for d in members):
            continue
        out.append(p)
    return out


def build_state(cfg, nodes: dict, parts: dict, jobs: list, now: float, mode="live",
                resvs=(), notes=None, version=__version__) -> dict:
    """What the page draws, from what Slurm reports. notes: job ID -> AdminComment,
    of which the page shows sjp's marks.

    Every node is sent; managed says whether sjp places jobs on it (its
    partitions by the [topology] rules), and the page shows the others too,
    such as an interactive or a GPU partition. A managed node is grouped by the
    partitions sjp uses; any other by its own partitions, leaving out those
    that also hold managed nodes (an "all" partition over the whole cluster),
    unless it has no others."""
    keep = shown_partitions(cfg, parts, nodes)
    is_managed = {name: any(p in keep for p in d["partitions"])
                  and not (cfg["topology"]["exclude_gpu_nodes"] and d.get("gpu"))
                  for name, d in nodes.items()}
    spans = {p for name, d in nodes.items() if is_managed[name] for p in d["partitions"]}
    shown = {}
    for name, d in sorted(nodes.items()):
        managed = is_managed[name]
        if managed:
            ps = [p for p in d["partitions"] if p in keep]
        else:
            own = [p for p in d["partitions"] if p in parts] or d["partitions"]
            ps = [p for p in own if p not in spans] or own
        if not ps:
            continue
        shown[name] = dict(name=name, cpus=d["cpus"], mem=d["mem"], partitions=ps,
                           tier=max(parts.get(p, {}).get("tier", 1) for p in ps),
                           managed=managed, gpus=d.get("gpus", 0), gpu_type=d.get("gpu_type", ""),
                           up=d["up"], state=d.get("state", ""),
                           why=slurm.blocking(d.get("state", "")), reason=d.get("reason", ""))
    running, pending = [], []
    for j in jobs:
        note = (notes or {}).get(j.get("id", j["jobid"]), "")
        asked = [p for p in (j["partition"] or "").split(",") if p]
        info = dict(id=j["jobid"], user=j["user"], account=j.get("account", ""),
                    name=j["name"], qos=j["qos"],
                    managed=not asked or any(p in keep for p in asked),
                    partition=j["partition"], minutes=j["timelimit"],
                    reservation=j.get("reservation") or "", features=j.get("features") or "",
                    sjp=note[note.find("sjp:"):] if "sjp:" in note else "")
        if j["state"] in ("R", "CF"):
            on = [n for n in slurm.expand_hostlist(j.get("nodelist") or "") if n in shown]
            if not on:
                continue
            nn = max(len(slurm.expand_hostlist(j["nodelist"])), 1)
            mem = j.get("req_mem") or j["mem"]
            end = j.get("end") or now + j["timelimit"] * 60
            for n in on:
                running.append(dict(info, node=n, cpus=j["cpus"] / nn, mem=mem,
                                    gpus=j.get("gpus", 0) / nn, end=end, nodes=nn))
        elif j["state"] == "PD":
            # pinned: waiting for one node (sjp's pin, or the user's --nodelist)
            req = slurm.expand_hostlist(j.get("req_nodes") or "")
            pending.append(dict(info, cpus=j["cpus"], mem=j.get("req_mem") or j["mem"],
                                gpus=j.get("gpus", 0) / max(j.get("nnodes", 1), 1),
                                reason=j["reason"], priority=j["priority"],
                                pinned=req[0] if len(req) == 1 and req[0] in shown else ""))
    pending.sort(key=queue_order)
    reservations(shown, nodes, resvs, jobs, now)
    # The page shows the top TOP of the queue and of those waiting for a node,
    # or all of them; pinned ones are drawn at their node.
    queued = [p for p in pending if not p["pinned"]]
    return dict(mode=mode, version=version, now=now, horizon=HORIZON,
                nodes=list(shown.values()), running=running, pending=pending[:MAX_WAITING],
                pending_total=len(pending), queued_total=len(queued), top=TOP)


def reservations(shown, nodes, resvs, jobs, now):
    """Each shown node's reservations, now or to come: name, start, end, the CPUs
    reserved there (all of them for a whole node, else its cores x threads),
    and, for one in effect, how many of them its own jobs leave free."""
    used = {}
    for j in jobs:
        if j["state"] in ("R", "CF") and j.get("reservation"):
            ns = slurm.expand_hostlist(j.get("nodelist") or "")
            for n in ns:
                key = (j["reservation"], n)
                used[key] = used.get(key, 0) + j["cpus"] / max(len(ns), 1)
    for r in resvs:
        if r["end"] <= now:
            continue
        for n, cores in r["nodes"].items():
            if n not in shown:
                continue
            d = nodes[n]
            cpus = d["cpus"] if cores is None else min(d["cpus"], cores * d.get("threads", 1))
            active = r["start"] <= now
            shown[n].setdefault("resv", []).append(dict(
                name=r["name"], start=r["start"], end=r["end"], cpus=cpus,
                whole=cores is None, active=active,
                left=max(0, round(cpus - used.get((r["name"], n), 0))) if active else cpus,
                nodes=len(r["nodes"]),
                **{k: r.get(k, "") for k in ("users", "accounts", "flags", "partition")}))


class UsageLog:
    """What each running job uses, from samples of its CPU time and memory:
    per job, (time, CPUs busy, memory in use MB) since the last sample, its
    peak memory, and the CPUs it kept busy on average since it started.

    With a path, every reading is appended to that file as one JSON line, and
    read back at the start, so the history outlives a restart. Once a day the
    lines of jobs that ended are dropped. A file that cannot be written leaves
    the history in memory only."""

    def __init__(self, path: str = ""):
        self.jobs: dict = {}
        self.lock = threading.Lock()
        self.path, self.error, self.compacted = path, "", None
        if path and os.path.exists(path):
            try:
                with open(path) as f:
                    for line in f:
                        r = json.loads(line)
                        self.add(r["t"], r["used"], r["starts"], save=False)
            except (OSError, ValueError, KeyError) as e:
                self.fail(f"reading {path}: {e}")

    def fail(self, why: str):
        self.error, self.path = why, ""
        print(f"sjp-viz: usage history kept in memory only: {why}", file=sys.stderr, flush=True)

    def add(self, now: float, used: dict, starts: dict, save: bool = True):
        """used: job ID -> (CPU seconds so far, memory MB, peak MB), as
        slurm.usage() reads it; starts: job ID -> start time, for every job
        running. Jobs no longer running are forgotten."""
        with self.lock:
            for jid, (cpu_s, mem, peak) in used.items():
                r = self.jobs.get(jid)
                if r is None:
                    r = self.jobs[jid] = dict(last=None, t=array("d"), cpu=array("f"),
                                              mem=array("q"), peak=0)
                if r["last"] and now > r["last"][0]:
                    r["t"].append(now)
                    r["cpu"].append(max(0.0, (cpu_s - r["last"][1]) / (now - r["last"][0])))
                    r["mem"].append(int(mem))
                start = starts.get(jid)
                r.update(last=(now, cpu_s), now_mem=mem, peak=max(r["peak"], peak),
                         avg=round(cpu_s / (now - start), 2) if start and now > start else None)
            for jid in set(self.jobs) - set(starts):
                del self.jobs[jid]
            if save and self.path:
                try:
                    with open(self.path, "a") as f:
                        f.write(json.dumps(dict(t=now, used=used, starts=starts)) + "\n")
                except OSError as e:
                    self.fail(f"writing {self.path}: {e}")
            if save and self.path and (self.compacted is None or now - self.compacted > 86400):
                self.compact(now)                # at the first reading, then daily

    def compact(self, now: float):
        """Drop from the file the jobs that are no longer running."""
        self.compacted, keep, tmp = now, set(self.jobs), self.path + ".new"
        try:
            with open(self.path) as f, open(tmp, "w") as out:
                for line in f:
                    r = json.loads(line)
                    used = {k: v for k, v in r["used"].items() if k in keep}
                    if used:
                        out.write(json.dumps(dict(t=r["t"], used=used, starts={
                            k: v for k, v in r["starts"].items() if k in keep})) + "\n")
            os.replace(tmp, self.path)
        except (OSError, ValueError, KeyError) as e:
            self.fail(f"compacting {self.path}: {e}")

    def summary(self, jid: str):
        """CPUs busy now and on average, memory in use and its peak; None if
        not sampled yet."""
        with self.lock:
            r = self.jobs.get(jid)
            if not r:
                return None
            return dict(now=round(r["cpu"][-1], 2) if r["cpu"] else None, avg=r["avg"],
                        mem=r["now_mem"], peak=r["peak"])

    def history(self, jid: str, points: int = USE_POINTS) -> list:
        """The job's samples, (time, CPUs busy, memory MB); averaged into at
        most `points` of them for a long job."""
        with self.lock:
            r = self.jobs.get(jid)
            if not r:
                return []
            t, c, m = list(r["t"]), list(r["cpu"]), list(r["mem"])
        n = len(t)
        if n <= points:
            return [(round(t[i]), round(c[i], 2), m[i]) for i in range(n)]
        out = []
        for k in range(points):
            a, b = k * n // points, (k + 1) * n // points
            out.append((round(t[b - 1]), round(sum(c[a:b]) / (b - a), 2),
                        round(sum(m[a:b]) / (b - a))))
        return out


class Live:
    """Reads the cluster every few seconds; the page gets the latest read. With
    use_interval, also what running jobs use, from sstat, that often."""

    version = __version__        # the version shown; main() asks git for it

    def __init__(self, cfg, interval: float, use_interval: float = 60.0, use_file: str = ""):
        self.cfg, self.interval, self.use_interval = cfg, interval, use_interval
        self.state = dict(mode="live", now=time.time(), horizon=HORIZON, nodes=[],
                          running=[], pending=[], pending_total=0, error="starting")
        self.lock = threading.Lock()
        self.use = UsageLog(use_file if use_interval else "")
        self.use_error = ""

    def run(self):
        accounts, accounts_at = [], 0.0
        while True:
            try:
                if time.time() - accounts_at > 600:  # the accounts, for the filter
                    try:
                        accounts, accounts_at = slurm.accounts(), time.time()
                    except slurm.SlurmError:
                        accounts_at = time.time() - 540     # try again in a minute
                try:
                    notes = slurm.admin_comments()   # sjp's marks, for a job's details
                except slurm.SlurmError:
                    notes = {}
                s = build_state(self.cfg, slurm.nodes(), slurm.partitions(),
                                slurm.queue(), time.time(), resvs=slurm.reservations(),
                                notes=notes, version=self.version)
                s["poll_ms"] = int(self.interval * 1000)
                s["accounts"] = accounts
                if self.use_interval:
                    for r in s["running"]:
                        r["use"] = self.use.summary(r["id"])
                    s["usage"] = dict(interval=self.use_interval, error=self.use_error)
            except slurm.SlurmError as e:
                with self.lock:
                    self.state = dict(self.state, error=str(e))
            else:
                with self.lock:
                    self.state = s
            time.sleep(self.interval)

    def get(self) -> dict:
        with self.lock:
            return self.state

    def sample_usage(self):
        """Every use_interval, what each running job uses (one sstat call for
        all of them). sstat asks the nodes, so this is kept apart from the
        cluster reads, and less often."""
        while self.use_interval:
            with self.lock:
                running = self.state.get("running", [])
            starts = {r["id"]: r["end"] - r["minutes"] * 60 for r in running}
            try:
                if starts:
                    self.use.add(time.time(), slurm.usage(sorted(starts)), starts)
                self.use_error = ""
            except slurm.SlurmError as e:
                self.use_error = str(e)
            time.sleep(self.use_interval)

    def history(self, jid: str) -> list:
        return self.use.history(jid)


def demo_nodes(slim: int = 3, fat: int = 3) -> list:
    """A demo cluster of slim and fat nodes, like many shared clusters:
    (name, CPUs, memory MB, partition, PriorityTier)."""
    return ([("slim%02d" % i, 96, 384 * 1024, "slim", 10) for i in range(1, slim + 1)]
            + [("fat%02d" % i, 96, 1536 * 1024, "fat", 9) for i in range(1, fat + 1)])


def demo_other_nodes(interactive: int = 0, gpu: int = 0) -> list:
    """Nodes of the demo cluster that sjp does not place jobs on: an interactive
    partition, and GPU nodes. (name, CPUs, memory MB, partition, GPUs, GPU type)."""
    return ([("int%02d" % i, 64, 512 * 1024, "interactive", 0, "") for i in range(1, interactive + 1)]
            + [("gpu%02d" % i, 64, 512 * 1024, "gpu", 4, "a10") for i in range(1, gpu + 1)])
# Jobs in those partitions: CPUs, GB per CPU and time limits (minutes) to pick
# from, and GPUs per job; how busy each partition is kept.
DEMO_OTHER = {
    "interactive": dict(cpus=[(1, 3), (2, 3), (4, 3), (8, 1)], gbc=(2, 8), limits=[(60, 2), (240, 3), (480, 2)],
                        gpus=[(0, 1)], load=0.55, names=["rstudio", "jupyter", "bash", "vscode"]),
    "gpu": dict(cpus=[(4, 2), (8, 3), (16, 2)], gbc=(4, 12), limits=[(240, 2), (720, 2), (1440, 2), (2880, 1)],
                gpus=[(1, 4), (2, 2), (4, 1)], load=1.3, names=["dorado", "alphafold", "train", "basecall"]),
}
DEMO_USERS = ["anna", "bo", "carlos", "dina", "erik", "fatima", "gustav", "hiro"]
DEMO_ACCOUNTS = {"anna": "genomics", "bo": "genomics", "carlos": "ecology", "dina": "ecology",
                 "erik": "microbio", "fatima": "microbio", "gustav": "teaching", "hiro": "genomics"}
DEMO_NAMES = ["assembly", "align", "polish", "binning", "blast", "phylo", "kraken",
              "mapping", "qc", "annotate"]
# How demo jobs use what they were given: (share of their CPUs kept busy, share
# of their memory in use at the end), and how often each kind comes.
DEMO_USE = [((0.85, 0.98), (0.5, 0.9), 6),      # as intended
            ((0.4, 0.7), (0.3, 0.7), 2),        # partly parallel
            ((0.02, 0.15), (0.05, 0.3), 1),     # mostly idle, or waiting on I/O
            (None, (0.1, 0.4), 2),              # one thread on many CPUs
            ((0.85, 0.98), (0.02, 0.12), 2)]    # far more memory than it needs


def demo_use(j: dict, now: float, points: int = 0):
    """A demo job's usage: its summary, or with points that many samples from
    its start to now, (time, CPUs busy, memory MB). Smooth wiggles from its
    seed, so every look at it agrees."""
    cpu_share, mem_share, seed = j["use"]
    start = j["end"] - j["minutes"] * 60
    def at(t):
        x = (t - start) / max(j.get("runtime", j["minutes"] * 60), 1)
        wig = 0.6 * math.sin(t / 2900 + seed) + 0.4 * math.sin(t / 830 + 3 * seed)
        warm = min(1.0, 0.15 + x / 0.1)        # the first tenth: reading its input
        busy = (1.0 if cpu_share is None else j["cpus"] * cpu_share) * warm * (1 + 0.15 * wig)
        swing = 0.12 * math.sin(t / 1700 + 2 * seed) + 0.06 * math.sin(t / 470 + seed)
        mem = j["mem"] * mem_share * min(1.0, 0.3 + 2.5 * x) * (1 + swing)
        return min(busy, j["cpus"]), int(min(mem, j["mem"]))
    if points:
        step = max(60.0, (now - start) / points)
        ts = [start + step * (i + 1) for i in range(int((now - start) / step))]
        return [(round(t), round(at(t)[0], 2), at(t)[1]) for t in ts]
    busy, mem = at(now)
    if now < start + 60:
        return dict(now=None, avg=None, mem=mem, peak=mem)
    ts = [start + (now - start) * (i + 0.5) / 60 for i in range(60)]
    return dict(now=round(busy, 2), avg=round(sum(at(t)[0] for t in ts) / 60, 2), mem=mem,
                peak=max(at(t)[1] for t in ts + [now]))


class Demo:
    """A simulated cluster, sped up. Jobs arrive, wait, and are placed the way
    sjp places them: on the node where they destroy the least placeable
    capacity, the closest match in memory per CPU breaking ties."""
    version = __version__        # the version shown; main() asks git for it

    def __init__(self, cfg, speed: float = 900.0, seed: int = 1, load: float = 1.4,
                 time_aware: bool = True, slim: int = 3, fat: int = 3, pinned: float = 0.5,
                 interactive: int = 0, gpu: int = 0):
        self.cfg, self.speed, self.time_aware = cfg, speed, time_aware
        self.pinned = pinned        # share of jobs that wait pinned to a node ([pin] wait_for_room)
        self.rng = random.Random(seed)
        self.demand = [tuple(x) for x in cfg["policy"]["demand"]]
        self.now = 1_800_000_000.0
        nodes = demo_nodes(slim, fat)
        self.nodes = {n: dict(name=n, cpus=c, mem=m, partitions=[p], tier=t, up=True, managed=True,
                              gpus=0, gpu_type="")
                      for n, c, m, p, t in nodes}
        for n, c, m, p, ng, gt in demo_other_nodes(interactive, gpu):
            self.nodes[n] = dict(name=n, cpus=c, mem=m, partitions=[p], tier=1, up=True,
                                 managed=False, gpus=ng, gpu_type=gt)
        # Arrivals per simulated second in each of those partitions.
        self.other_rate = {}
        for p, d in DEMO_OTHER.items():
            ns = [x for x in self.nodes.values() if p in x["partitions"]]
            if not ns:
                continue
            mean = lambda t: sum(v * w for v, w in t) / sum(w for _, w in t)
            busy = (sum(x["gpus"] for x in ns) / max(mean(d["gpus"]), 1e-9) if mean(d["gpus"])
                    else sum(x["cpus"] for x in ns) / mean(d["cpus"]))
            self.other_rate[p] = d["load"] * busy / (mean(d["limits"]) * 60 * 0.6)
        self.running: list[dict] = []
        self.pending: list[dict] = []
        self.next_id = 4211
        cap = sum(c for _, c, *_ in nodes)
        # Arrivals per simulated second that keep the cluster about `load` busy.
        self.rate = load * cap / (self._mean_cpus() * self._mean_runtime())
        self.lock = threading.Lock()
        for _ in range(int(cap / 14)):              # start part-full, not empty
            self.pending.append(self._job())
        for p, rate in self.other_rate.items():
            for _ in range(max(2, int(rate * 3 * 3600))):
                self.pending.append(self._other_job(p))
        self._place()

    def _mean_cpus(self):
        return sum(c * w for c, w in self.CPUS) / sum(w for _, w in self.CPUS)

    def _mean_runtime(self):
        tot = sum(w for _, w in self.LIMITS)
        return sum(m * 60 * 0.6 * w for m, w in self.LIMITS) / tot

    CPUS = [(1, 2), (2, 2), (4, 4), (8, 4), (16, 3), (24, 1), (32, 2)]
    LIMITS = [(60, 3), (240, 3), (720, 2), (1440, 2), (4320, 1), (10080, 1)]

    def _pick(self, table):
        r = self.rng.uniform(0, sum(w for _, w in table))
        for v, w in table:
            r -= w
            if r <= 0:
                return v
        return table[-1][0]

    def _job(self) -> dict:
        cpus = self._pick(self.CPUS)
        mpc = self._pick([(q, w) for q, w in self.demand]) * self.rng.uniform(0.7, 1.3)
        minutes = self._pick(self.LIMITS)
        self.next_id += 1
        user = self.rng.choice(DEMO_USERS)
        cpu_r, mem_r, _ = self._pick([(u, u[2]) for u in DEMO_USE])
        use = (self.rng.uniform(*cpu_r) if cpu_r else None, self.rng.uniform(*mem_r),
               self.rng.uniform(0, 6.3))
        return dict(use=use, id=str(self.next_id), user=user, account=DEMO_ACCOUNTS[user],
                    name=self.rng.choice(DEMO_NAMES), qos="normal", partition="",
                    cpus=cpus, mem=int(cpus * mpc // 1024 * 1024) or 1024,
                    minutes=minutes, submitted=self.now, reason="Resources",
                    runtime=minutes * 60 * self.rng.uniform(0.3, 0.95),
                    waits=self.rng.random() < self.pinned, gpus=0, target="", managed=True)

    def _other_job(self, part: str) -> dict:
        """A job for a partition sjp does not place in: it goes there by itself."""
        d = DEMO_OTHER[part]
        j = self._job()
        cpus = self._pick(d["cpus"])
        minutes = self._pick(d["limits"])
        return dict(j, cpus=cpus, mem=int(cpus * self.rng.uniform(*d["gbc"])) * 1024,
                    minutes=minutes, runtime=minutes * 60 * self.rng.uniform(0.3, 0.95),
                    name=self.rng.choice(d["names"]), gpus=self._pick(d["gpus"]),
                    target=part, partition=part, managed=False, waits=False,
                    qos="interactive" if part == "interactive" else "normal")

    def _free(self, managed: bool = True):
        """Free CPUs, memory and partitions of the nodes sjp places on (or, with
        managed=False, of the others, with their free GPUs too)."""
        used = {n: [0, 0, 0] for n in self.nodes}
        for j in self.running:
            used[j["node"]][0] += j["cpus"]
            used[j["node"]][1] += j["mem"]
            used[j["node"]][2] += j.get("gpus", 0)
        if not managed:
            return {n: (d["cpus"] - used[n][0], d["mem"] - used[n][1], d["partitions"],
                        d["gpus"] - used[n][2]) for n, d in self.nodes.items() if not d["managed"]}
        return {n: (d["cpus"] - used[n][0], d["mem"] - used[n][1], d["partitions"])
                for n, d in self.nodes.items() if d["managed"]}

    def _place(self):
        """Start every waiting job that fits now, oldest first: partitions as
        sjp's placement table would choose them, then the node within them."""
        tiers = {p: d["tier"] for d in self.nodes.values() for p in d["partitions"] if d["managed"]}
        total = {}
        for d in self.nodes.values():
            for p in d["partitions"]:
                if d["managed"]:
                    total.setdefault(p, []).append((d["cpus"], d["mem"]))
        for j in [j for j in self.pending if j.get("target")]:     # first fit, as Slurm would
            free = self._free(managed=False)
            node = next((n for n, (fc, fm, ps, fg) in sorted(free.items())
                         if j["target"] in ps and fc >= j["cpus"] and fm >= j["mem"] and fg >= j["gpus"]), None)
            if node:
                self.pending.remove(j)
                self.running.append(dict(j, node=node, nodes=1, end=self.now + j["minutes"] * 60,
                                         done=self.now + j["runtime"], pinned=""))
        for j in [j for j in self.pending if not j.get("target")]:
            free = self._free()
            by_part = {p: [(free[n][0], free[n][1]) for n, d in self.nodes.items()
                           if p in d["partitions"]] for p in total}
            allowed = policy.choose(j["cpus"], j["mem"], total, by_part, self.demand,
                                    self.cfg["policy"]["tolerance"])
            j["allowed"] = allowed
            fits = {n: f for n, f in free.items()
                    if f[0] >= j["cpus"] and f[1] >= j["mem"] and set(f[2]) & set(allowed)}
            if not fits:
                continue
            busy = None
            if self.time_aware:
                busy = {n: max([r["end"] - self.now for r in self.running if r["node"] == n],
                               default=0) for n in self.nodes}
            node, _, _ = policy.pick_node(j["cpus"], j["mem"], allowed, fits, tiers,
                                          self.demand, busy=busy, minutes=j["minutes"])
            if node is None:        # equally good, or only one: as Slurm would
                top = max(tiers[p] for n in fits for p in fits[n][2])
                node = min((n for n in fits if any(tiers[p] == top for p in fits[n][2])),
                           key=lambda n: (policy.phi_node(*fits[n][:2], self.demand)
                                          - policy.phi_node(fits[n][0] - j["cpus"],
                                                            fits[n][1] - j["mem"],
                                                            self.demand), n))
            self.pending.remove(j)
            self.running.append(dict(j, node=node, nodes=1, partition=",".join(allowed),
                                     end=self.now + j["minutes"] * 60,
                                     done=self.now + j["runtime"], pinned=""))
        self._pin(tiers)

    def _pin(self, tiers):
        """Jobs that wait pinned, as with [pin] wait_for_room: each on the node
        expected to have room for it first, chosen again every step; the jobs
        ahead of it waiting there count against that node."""
        free = self._free()
        ends = {}
        for r in self.running:
            ends.setdefault(r["node"], []).append((r["end"], r["cpus"], r["mem"]))
        for e in ends.values():
            e.sort()
        for j in sorted(self.pending, key=queue_order):
            j["pinned"] = ""
            if not j.get("waits") or not j.get("allowed") or j.get("target"):
                continue
            node, _, _ = policy.soonest_node(j["cpus"], j["mem"], j["allowed"], free, ends,
                                             self.now, tiers)
            if node:
                j["pinned"] = node
                fc, fm, ps = free[node]
                free[node] = (fc - j["cpus"], fm - j["mem"], ps)

    def step(self, real_seconds: float):
        with self.lock:
            dt = real_seconds * self.speed
            self.now += dt
            self.running = [j for j in self.running if j["done"] > self.now]
            for _ in range(self._poisson(self.rate * dt)):
                self.pending.append(self._job())
            for p, rate in self.other_rate.items():
                for _ in range(self._poisson(rate * dt)):
                    self.pending.append(self._other_job(p))
            self._place()

    def _poisson(self, lam: float) -> int:
        n, p, limit = 0, 1.0, pow(2.718281828, -lam)
        while True:
            p *= self.rng.random()
            if p <= limit:
                return n
            n += 1

    def run(self, tick: float = 0.1):
        while True:
            t0 = time.time()
            time.sleep(tick)
            self.step(time.time() - t0)

    def history(self, jid: str) -> list:
        with self.lock:
            j = next((j for j in self.running if j["id"] == jid), None)
            return demo_use(j, self.now, points=120) if j else []

    def get(self) -> dict:
        with self.lock:
            hide = ("runtime", "done", "submitted", "waits", "allowed", "target")
            pending = sorted(self.pending, key=queue_order)
            queued = [j for j in pending if not j.get("pinned")]
            return dict(mode="demo", version=self.version, now=self.now, horizon=HORIZON,
                        speed=self.speed,
                        poll_ms=250, nodes=list(self.nodes.values()),
                        running=[dict({k: v for k, v in j.items() if k not in hide + ("use",)},
                                      use=demo_use(j, self.now)) for j in self.running],
                        pending=[{k: v for k, v in j.items() if k not in hide + ("use",)}
                                 for j in pending[:MAX_WAITING]],
                        usage=dict(interval=60, error=""),
                        pending_total=len(pending), queued_total=len(queued), top=TOP,
                        accounts=sorted(set(DEMO_ACCOUNTS.values())))


def serve(source, bind: str, port: int):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            url = urlparse(self.path)
            pages = {"/": HTML, "/index.html": HTML, "/2d": HTML_2D}
            if url.path == "/state.json":
                body, kind = json.dumps(source.get()).encode(), "application/json"
            elif url.path == "/usage.json":      # one running job's samples
                jid = parse_qs(url.query).get("job", [""])[0]
                body, kind = json.dumps(source.history(jid)).encode(), "application/json"
            elif url.path in pages or url.path == "/usage.js":
                page = USE_JS if url.path == "/usage.js" else pages[url.path]
                with open(page, "rb") as f:
                    body = f.read()
                kind = ("text/javascript" if page == USE_JS else "text/html") + "; charset=utf-8"
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", kind)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass

    httpd = ThreadingHTTPServer((bind, port), Handler)
    print(f"sjp-viz: http://{bind}:{port}/", flush=True)
    httpd.serve_forever()


def main(argv=None):
    ap = argparse.ArgumentParser(prog="sjp-viz", description=__doc__.split("\n\n")[0])
    ap.add_argument("-c", "--config", help="sjp.toml, for [topology] and [policy]")
    ap.add_argument("--bind", default="127.0.0.1",
                    help="address to answer on (default 127.0.0.1: this machine only)")
    ap.add_argument("--port", type=int, default=8650)
    ap.add_argument("--interval", type=float, default=5.0,
                    help="seconds between reads of the cluster (default 5)")
    ap.add_argument("--usage-interval", type=float, default=60.0,
                    help="seconds between reads of what running jobs use, with sstat "
                         "(default 60; 0: off). Needs job accounting (JobAcctGatherType)")
    ap.add_argument("--usage-file", default="/var/lib/sjp-viz/usage.jsonl",
                    help="where the usage history is kept across restarts (default "
                         "/var/lib/sjp-viz/usage.jsonl; empty: in memory only)")
    ap.add_argument("--demo", action="store_true", help="a simulated cluster instead")
    ap.add_argument("--speed", type=float, default=900.0,
                    help="demo: simulated seconds per real second (default 900)")
    ap.add_argument("--seed", type=int, default=1, help="demo: random seed")
    ap.add_argument("--slim", type=int, default=3, help="demo: slim nodes (default 3)")
    ap.add_argument("--fat", type=int, default=3, help="demo: fat nodes (default 3)")
    ap.add_argument("--interactive", type=int, default=0,
                    help="demo: nodes in an interactive partition sjp does not place in (default 0)")
    ap.add_argument("--gpu", type=int, default=0,
                    help="demo: GPU nodes, with 4 GPUs each, which sjp does not place on (default 0)")
    ap.add_argument("--pinned", type=float, default=0.5,
                    help="demo: share of jobs that wait pinned to a node, as with "
                         "[pin] wait_for_room (default 0.5)")
    ap.add_argument("--load", type=float, default=1.4,
                    help="demo: how much work arrives, relative to the cluster (default 1.4)")
    a = ap.parse_args(argv)
    cfg = config.load(a.config)
    slurm.set_actuation(False)                  # read-only, whatever happens
    source = (Demo(cfg, a.speed, a.seed, a.load, slim=a.slim, fat=a.fat, pinned=a.pinned,
                   interactive=a.interactive, gpu=a.gpu)
              if a.demo
              else Live(cfg, a.interval, a.usage_interval, a.usage_file))
    source.version = describe()          # the release, or where a checkout is
    threading.Thread(target=source.run, daemon=True).start()
    if not a.demo:
        threading.Thread(target=source.sample_usage, daemon=True).start()
    serve(source, a.bind, a.port)


if __name__ == "__main__":
    main()
