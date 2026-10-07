"""sjp-viz: the cluster in 3D, live in a web browser.

Every node is a box: CPUs across, memory up, time into the depth. Every running
job is a box inside it, as wide as its CPUs, as tall as its memory and as deep
as the time it has left. The jobs on a node are stacked corner to corner, so a
node is full when the stack reaches its far corner, and a node whose jobs ask
for the wrong mix of CPUs and memory runs out of one while the other is left.
Waiting jobs stand in a queue in front.

    python3 -m sjp.viz                      # the live cluster, on 127.0.0.1:8650
    python3 -m sjp.viz --demo               # a simulated cluster, sped up

It only reads from Slurm (scontrol show, squeue), like a dry run of sjpd, and
by default answers only on localhost: the page shows users and job names. To
see it from your own machine, use an SSH tunnel:

    ssh -L 8650:localhost:8650 <controller>      # then open http://localhost:8650
"""
from __future__ import annotations
import argparse, json, os, random, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import __version__, config, policy, slurm

HTML = os.path.join(os.path.dirname(os.path.abspath(__file__)), "viz.html")
HTML_2D = os.path.join(os.path.dirname(os.path.abspath(__file__)), "viz2d.html")
HORIZON = 14 * 86400          # the depth of a node: two weeks, Slurm's usual MaxTime
TOP = 50                      # waiting jobs shown in the queue
PINNED = 300                  # and at most this many waiting pinned to a node


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
                resvs=(), notes=None) -> dict:
    """What the page draws, from what Slurm reports. notes: job ID -> AdminComment,
    of which the page shows sjp's marks."""
    keep = shown_partitions(cfg, parts, nodes)
    shown = {}
    for name, d in sorted(nodes.items()):
        ps = [p for p in d["partitions"] if p in keep]
        if not ps or (cfg["topology"]["exclude_gpu_nodes"] and d.get("gpu")):
            continue
        shown[name] = dict(name=name, cpus=d["cpus"], mem=d["mem"], partitions=ps,
                           tier=max(parts.get(p, {}).get("tier", 1) for p in ps),
                           up=d["up"], state=d.get("state", ""))
    running, pending = [], []
    for j in jobs:
        note = (notes or {}).get(j["jobid"], "")
        info = dict(id=j["jobid"], user=j["user"], name=j["name"], qos=j["qos"],
                    partition=j["partition"], minutes=j["timelimit"],
                    reservation=j.get("reservation") or "",
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
                                    end=end, nodes=nn))
        elif j["state"] == "PD":
            # pinned: waiting for one node (sjp's pin, or the user's --nodelist)
            req = slurm.expand_hostlist(j.get("req_nodes") or "")
            pending.append(dict(info, cpus=j["cpus"], mem=j.get("req_mem") or j["mem"],
                                reason=j["reason"], priority=j["priority"],
                                pinned=req[0] if len(req) == 1 and req[0] in shown else ""))
    pending.sort(key=queue_order)
    reservations(shown, nodes, resvs, jobs, now)
    # The queue's top, and the jobs waiting for a node, which are drawn at that node.
    queued = [p for p in pending if not p["pinned"]]
    pinned = [p for p in pending if p["pinned"]]
    return dict(mode=mode, version=__version__, now=now, horizon=HORIZON,
                nodes=list(shown.values()), running=running,
                pending=queued[:TOP] + pinned[:PINNED],
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


class Live:
    """Reads the cluster every few seconds; the page gets the latest read."""

    def __init__(self, cfg, interval: float):
        self.cfg, self.interval = cfg, interval
        self.state = dict(mode="live", now=time.time(), horizon=HORIZON, nodes=[],
                          running=[], pending=[], pending_total=0, error="starting")
        self.lock = threading.Lock()

    def run(self):
        while True:
            try:
                try:
                    notes = slurm.admin_comments()   # sjp's marks, for a job's details
                except slurm.SlurmError:
                    notes = {}
                s = build_state(self.cfg, slurm.nodes(), slurm.partitions(),
                                slurm.queue(), time.time(), resvs=slurm.reservations(),
                                notes=notes)
                s["poll_ms"] = int(self.interval * 1000)
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


def demo_nodes(slim: int = 3, fat: int = 3) -> list:
    """A demo cluster of slim and fat nodes, like many shared clusters:
    (name, CPUs, memory MB, partition, PriorityTier)."""
    return ([("slim%02d" % i, 96, 384 * 1024, "slim", 10) for i in range(1, slim + 1)]
            + [("fat%02d" % i, 96, 1536 * 1024, "fat", 9) for i in range(1, fat + 1)])
DEMO_USERS = ["anna", "bo", "carlos", "dina", "erik", "fatima", "gustav", "hiro"]
DEMO_NAMES = ["assembly", "align", "polish", "binning", "blast", "phylo", "kraken",
              "mapping", "qc", "annotate"]


class Demo:
    """A simulated cluster, sped up. Jobs arrive, wait, and are placed the way
    sjp places them: on the node where they destroy the least placeable
    capacity, the closest match in memory per CPU breaking ties."""

    def __init__(self, cfg, speed: float = 900.0, seed: int = 1, load: float = 1.4,
                 time_aware: bool = True, slim: int = 3, fat: int = 3, pinned: float = 0.5):
        self.cfg, self.speed, self.time_aware = cfg, speed, time_aware
        self.pinned = pinned        # share of jobs that wait pinned to a node ([pin] wait_for_room)
        self.rng = random.Random(seed)
        self.demand = [tuple(x) for x in cfg["policy"]["demand"]]
        self.now = 1_800_000_000.0
        nodes = demo_nodes(slim, fat)
        self.nodes = {n: dict(name=n, cpus=c, mem=m, partitions=[p], tier=t, up=True)
                      for n, c, m, p, t in nodes}
        self.running: list[dict] = []
        self.pending: list[dict] = []
        self.next_id = 4211
        cap = sum(c for _, c, *_ in nodes)
        # Arrivals per simulated second that keep the cluster about `load` busy.
        self.rate = load * cap / (self._mean_cpus() * self._mean_runtime())
        self.lock = threading.Lock()
        for _ in range(int(cap / 14)):              # start part-full, not empty
            self.pending.append(self._job())
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
        return dict(id=str(self.next_id), user=self.rng.choice(DEMO_USERS),
                    name=self.rng.choice(DEMO_NAMES), qos="normal", partition="",
                    cpus=cpus, mem=int(cpus * mpc // 1024 * 1024) or 1024,
                    minutes=minutes, submitted=self.now, reason="Resources",
                    runtime=minutes * 60 * self.rng.uniform(0.3, 0.95),
                    waits=self.rng.random() < self.pinned)

    def _free(self):
        used = {n: [0, 0] for n in self.nodes}
        for j in self.running:
            used[j["node"]][0] += j["cpus"]
            used[j["node"]][1] += j["mem"]
        return {n: (d["cpus"] - used[n][0], d["mem"] - used[n][1], d["partitions"])
                for n, d in self.nodes.items()}

    def _place(self):
        """Start every waiting job that fits now, oldest first: partitions as
        sjp's placement table would choose them, then the node within them."""
        tiers = {p: d["tier"] for d in self.nodes.values() for p in d["partitions"]}
        total = {}
        for d in self.nodes.values():
            for p in d["partitions"]:
                total.setdefault(p, []).append((d["cpus"], d["mem"]))
        for j in list(self.pending):
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
            if not j.get("waits") or not j.get("allowed"):
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

    def get(self) -> dict:
        with self.lock:
            hide = ("runtime", "done", "submitted", "waits", "allowed")
            pending = sorted(self.pending, key=queue_order)
            queued = [j for j in pending if not j.get("pinned")]
            pinned = [j for j in pending if j.get("pinned")]
            return dict(mode="demo", version=__version__, now=self.now, horizon=HORIZON,
                        speed=self.speed,
                        poll_ms=250, nodes=list(self.nodes.values()),
                        running=[{k: v for k, v in j.items() if k not in hide}
                                 for j in self.running],
                        pending=[{k: v for k, v in j.items() if k not in hide}
                                 for j in queued[:TOP] + pinned[:PINNED]],
                        pending_total=len(pending), queued_total=len(queued), top=TOP)


def serve(source, bind: str, port: int):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path.split("?")[0] == "/state.json":
                body, kind = json.dumps(source.get()).encode(), "application/json"
            elif self.path.split("?")[0] in ("/", "/index.html", "/2d"):
                page = HTML_2D if self.path.split("?")[0] == "/2d" else HTML
                with open(page, "rb") as f:
                    body, kind = f.read(), "text/html; charset=utf-8"
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
    ap.add_argument("--demo", action="store_true", help="a simulated cluster instead")
    ap.add_argument("--speed", type=float, default=900.0,
                    help="demo: simulated seconds per real second (default 900)")
    ap.add_argument("--seed", type=int, default=1, help="demo: random seed")
    ap.add_argument("--slim", type=int, default=3, help="demo: slim nodes (default 3)")
    ap.add_argument("--fat", type=int, default=3, help="demo: fat nodes (default 3)")
    ap.add_argument("--pinned", type=float, default=0.5,
                    help="demo: share of jobs that wait pinned to a node, as with "
                         "[pin] wait_for_room (default 0.5)")
    ap.add_argument("--load", type=float, default=1.4,
                    help="demo: how much work arrives, relative to the cluster (default 1.4)")
    a = ap.parse_args(argv)
    cfg = config.load(a.config)
    slurm.set_actuation(False)                  # read-only, whatever happens
    source = (Demo(cfg, a.speed, a.seed, a.load, slim=a.slim, fat=a.fat, pinned=a.pinned)
              if a.demo
              else Live(cfg, a.interval))
    threading.Thread(target=source.run, daemon=True).start()
    serve(source, a.bind, a.port)


if __name__ == "__main__":
    main()
