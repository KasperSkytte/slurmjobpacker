"""sjpd - the control loop.

Two planes. This process decides slowly and out of band; job_submit.lua applies
the decision instantly and in band, with one table lookup. If this process dies,
the table goes stale and the plugin falls straight through to the site's static
rule -- degrading to exactly today's behaviour, not to an outage.

Threads exist for I/O overlap, not parallel compute. Scoring the whole decision
surface is ~300 buckets and takes single-digit milliseconds; what must never
block is the tick, and a slow `scontrol` call otherwise would.
"""
from __future__ import annotations
import argparse, collections, json, os, pwd, signal, sys, threading, time

from . import config, limits, narrate, policy, slurm


def hours(sec: float) -> str:
    """Seconds as hours, the way the plugin writes them in AdminComment."""
    h = sec / 3600
    return f"{h:.1f}h" if h < 10 else f"{h:.0f}h"


class Shared:
    """Latest snapshots, guarded by one lock. Readers never block on Slurm."""
    def __init__(self):
        self.lock = threading.Lock()
        self.nodes: dict = {}
        self.nodes_at: float = 0.0
        self.parts: dict = {}
        self.pending: list = []
        self.pending_at: float = 0.0
        self.claims_at: float = 0.0       # the queue read the node figures include
        self.jobs: list = []          # pending and running, from the last queue poll
        self.stop = threading.Event()
        self.errors: dict = {}

    def note_error(self, where, exc) -> bool:
        """Record an error; True if it differs from the last one seen here."""
        with self.lock:
            new = self.errors.get(where, " ").split(" ", 1)[1] != str(exc)
            self.errors[where] = f"{time.time():.0f} {exc}"
            return new


class Daemon:
    def __init__(self, cfg):
        self.cfg = cfg
        self.sh = Shared()
        self.mode = cfg["general"]["mode"]
        self.state_dir = cfg["general"]["state_dir"]
        self.table_path = os.path.join(self.state_dir, "policy.lua")
        # Where a dry run puts the table it would have written: inspectable, but
        # not the path the plugin reads.
        self.dryrun_table_path = os.path.join(self.state_dir, "policy.dryrun.lua")
        self.status_path = os.path.join(self.state_dir, "status.json")
        self.limiters: dict = {}     # QOS -> LimitPulse, for each QOS that has held jobs
        self.qos_caps: dict = {}     # QOS -> (per_user, per_account, read at), a cache
        self.pulse_record: dict = {} # QOS -> base caps while a pulse is on (state_file)
        self.widened: set = set()    # jobs the starvation guard already widened
        self.recheck_at = 0.0        # when pending jobs' partitions were last rechecked
        self.flexed: dict = {}       # jobid -> (when, node, cpus, mem): moved to the flex
                                     # QOS this run, not seen running yet
        self.flex_seen: dict = {}    # jobid -> when flex mode last moved it either way
        self.flex_users = None       # (read at, {(user, account): {qos}}), a cache
        self.flex_warned: set = set()  # (user, account) already named as missing it
        self.cluster = ""
        self.version = 0
        self.last_rendered = None
        self.last_sig = None      # content signature, excluding the timestamp
        self.last_write = 0.0
        self.demand = [tuple(x) for x in cfg["policy"]["demand"]]
        self.log_fh = None
        self.text_fh = None
        self.last_shape = None    # (i, j) -> partitions, for logging what changed
        self.last_pin_sig = None
        self.snap = None          # latest decision surface, for shadowing (a dict)
        self.released: set = set()   # pinned jobs already released or not ours
        self.waiting: set = set()    # jobs pinned to wait for room (wait_for_room)
        self.uids: dict = {}
        self.seen = None          # job ids already shadowed; None until the first poll
        # Belt and braces: the mode checks below decide what is *attempted*, and
        # slurm.apply() refuses to run anything unless this is on.
        slurm.set_actuation(self.mode == "enforce")

    # ---------------------------------------------------------------- helpers
    def disabled(self) -> bool:
        return os.path.exists(self.cfg["general"]["disable_file"])

    def log(self, event: str, **kw):
        now = time.time()
        rec = dict(ts=round(now, 3),
                   time=time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(now)),
                   event=event, mode=self.mode, **kw)
        line = json.dumps(rec, separators=(",", ":"))
        if self.log_fh:
            try:
                self.log_fh.write(line + "\n"); self.log_fh.flush()
            except OSError:
                pass
        else:
            print(line, flush=True)
        if self.text_fh and (text := narrate.render(rec)):
            try:
                self.text_fh.write(text + "\n"); self.text_fh.flush()
            except OSError:
                pass

    def open_logs(self):
        for key, attr in (("log_file", "log_fh"), ("text_log", "text_fh")):
            path = self.cfg["general"].get(key)
            if not path:
                continue
            try:
                os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
                setattr(self, attr, open(path, "a"))
            except OSError as e:
                print(f"sjpd: cannot open {path}: {e}. As an ordinary user, give "
                      "--state-dir (or --log-file/--text-log) a directory you can write.",
                      file=sys.stderr)

    def blocked(self, modes) -> str | None:
        """Why an action may not be carried out now, or None if it may."""
        if self.mode not in modes:
            return f"mode={self.mode}"
        if self.disabled():
            return "disable_file present"
        return None

    def intend(self, action, cmd, why, blocked, run, quiet=False, **kw):
        """Log an intended change, and carry it out only if nothing blocks it.

        Every change sjpd would make goes through here, so the decision log is
        also a complete list of what a dry run would have done.
        """
        executed, extra = False, {}
        if blocked is None:
            try:
                executed = bool(run())
                if not executed:
                    blocked = "actuation off"
            except Exception as e:
                extra["error"] = repr(e)
        if blocked:
            extra["blocked"] = blocked
        if quiet and "error" not in extra:
            return
        self.log("action", action=action, executed=executed, **extra,
                 why=why, cmd=cmd, **kw)

    def batch_partitions(self, parts: dict, nodes: dict | None = None) -> list[str]:
        return self.partition_filter(parts, nodes)[0]

    def usable_node(self, d) -> bool:
        return not (self.cfg["topology"]["exclude_gpu_nodes"] and d.get("gpu"))

    def partition_filter(self, parts: dict, nodes: dict | None = None):
        """(partitions the packer may assign, {excluded partition: reason})."""
        t = self.cfg["topology"]
        skip = set(t["exclude_partitions"])
        keep, dropped = [], {}
        for p in (t["batch_partitions"] or parts):
            members = [d for d in (nodes or {}).values() if p in d["partitions"]]
            if p not in parts:
                dropped[p] = "listed in batch_partitions but not on this cluster"
            elif p in skip:
                dropped[p] = "exclude_partitions"
            elif t["exclude_interactive"] and p.lower() == "interactive":
                dropped[p] = "named interactive (exclude_interactive)"
            elif members and not any(self.usable_node(d) for d in members):
                dropped[p] = "every node has a GPU (exclude_gpu_nodes)"
            else:
                keep.append(p)
        return keep, dropped

    def shapes(self, nodes, parts):
        """(total shape per partition, free shape per partition)."""
        by_part_total, by_part_free = {}, {}
        for p in self.batch_partitions(parts, nodes):
            tot, free = [], []
            for n, d in nodes.items():
                if p not in d["partitions"] or not d["up"] or not self.usable_node(d):
                    continue
                tot.append((d["cpus"], d["mem"]))
                free.append((max(0, d["cpus"] - d["alloc_cpus"]),
                             max(0, d["mem"] - d["alloc_mem"])))
            if tot:
                by_part_total[p] = tot
                by_part_free[p] = free
        return by_part_total, by_part_free

    # Pending reasons under which a job tied to one node is about to start there.
    # A job pinned to a busy node waits with "ReqNodeNotAvail, May be reserved
    # for other job"; with UnavailableNodes the node itself is down.
    BUSY_NODE = "ReqNodeNotAvail, May be reserved for other job"
    CLAIM_REASONS = ("None", "Resources", "Priority", "", BUSY_NODE)

    @staticmethod
    def claim(nodes, jobs):
        """Count pending jobs tied to one node (pinned, or the user's own
        --nodelist) as taken on that node: Slurm will start them there, and
        until it does, the node's own figures don't show them. Without this, a
        burst of submissions is pinned to the same free space again and again."""
        for j in jobs:
            if (j["state"] != "PD" or not j.get("req_nodes") or j.get("nnodes", 1) > 1
                    or j.get("reason") not in Daemon.CLAIM_REASONS):
                continue
            ns = slurm.expand_hostlist(j["req_nodes"])
            if len(ns) == 1 and ns[0] in nodes:
                d = nodes[ns[0]]
                d["alloc_cpus"] += j["cpus"]
                d["alloc_mem"] += j.get("req_mem") or j["mem"]

    @staticmethod
    def reserve(nodes, resvs, jobs, now):
        """Count reservations like jobs planned on their nodes. Memory cannot be
        reserved, so a reservation's share of it is taken at the node's own
        memory per CPU. An active reservation's cores, less those its own jobs
        use, are added to the node's allocation, so every decision sees them as
        taken. An upcoming one is kept in the node's "later" list as
        (start, cpus, mem), for jobs that would still run when it starts."""
        used = collections.Counter()               # (reservation, node) -> CPUs
        for j in jobs:
            if j["state"] in ("R", "CF") and j.get("reservation"):
                ns = slurm.expand_hostlist(j.get("nodelist") or "")
                for n in ns:
                    used[(j["reservation"], n)] += j["cpus"] / max(len(ns), 1)
        for d in nodes.values():
            d["later"] = []
        for r in resvs:
            if r["end"] <= now:
                continue
            for n, cores in r["nodes"].items():
                d = nodes.get(n)
                if d is None or not d["cpus"]:
                    continue
                cpus = d["cpus"] if cores is None else min(d["cpus"], cores * d["threads"])
                per_cpu = d["mem"] / d["cpus"]
                if r["start"] <= now:
                    left = max(0, round(cpus - used[(r["name"], n)]))
                    d["alloc_cpus"] += left
                    d["alloc_mem"] += int(left * per_cpu)
                else:
                    d["later"].append((r["start"], cpus, int(cpus * per_cpu)))

    def pin_active(self) -> bool:
        """Pins need sjpd to release the ones that do not start, which is an
        enforce-mode action. observe shows what enforce would do; advise never pins."""
        return self.cfg["pin"]["enabled"] and self.mode != "advise"

    def wait_active(self) -> bool:
        """[pin] wait_for_room is in effect: it needs always_place, pins, and the
        recheck that moves those pins."""
        return (self.cfg["pin"]["wait_for_room"] and self.cfg["policy"]["always_place"]
                and self.pin_active() and bool(self.cfg["recheck"]["interval"]))

    def node_free(self, nodes, parts) -> dict:
        """name -> (free cpus, free mem, [batch partitions]) for nodes a job could
        use. Below zero where jobs wait pinned to a node that has no room yet."""
        batch = set(self.batch_partitions(parts, nodes))
        out = {}
        for n, d in nodes.items():
            ps = sorted(p for p in d["partitions"] if p in batch)
            if ps and d["up"] and self.usable_node(d):
                out[n] = (d["cpus"] - d["alloc_cpus"], d["mem"] - d["alloc_mem"], ps)
        return out

    @staticmethod
    def ends(jobs, now) -> dict:
        """node -> [(end, cpus, mem)]: what its running jobs release, and when,
        from their time limits, in order. A job without a limit ends in a year."""
        out = collections.defaultdict(collections.Counter)
        for j in jobs:
            if j["state"] not in ("R", "CF"):
                continue
            ns = slurm.expand_hostlist(j.get("nodelist") or "")
            end = int(j.get("end") or now + 365 * 86400) // 60 * 60
            for n in ns:
                out[n][(end, "c")] += j["cpus"] / max(len(ns), 1)
                out[n][(end, "m")] += j.get("req_mem") or j["mem"]
        return {n: [(e, round(c[(e, "c")]), c[(e, "m")])
                    for e in sorted({e for e, _ in c})] for n, c in out.items()}

    @staticmethod
    def busy_until(jobs, now) -> dict:
        """node -> when its running jobs end (epoch seconds), from their time
        limits. A job without a limit counts as running for a year."""
        out = {}
        for j in jobs:
            if j["state"] not in ("R", "CF"):
                continue
            end = j.get("end") or now + 365 * 86400
            for n in slurm.expand_hostlist(j.get("nodelist") or ""):
                out[n] = max(out.get(n, 0), end)
        return out

    def uid(self, user):
        if user not in self.uids:
            try:
                self.uids[user] = pwd.getpwnam(user).pw_uid
            except KeyError:
                self.uids[user] = None
        return self.uids[user]

    def qos_cap(self, qos):
        """The per-user CPU cap a QOS has now: raised during its pulse, else read
        from Slurm (cached for a minute). None if it has none."""
        lim = self.limiters.get(qos)
        if lim and lim.raised:
            return lim.cur_u
        cached = self.qos_caps.get(qos)
        if not cached or time.time() - cached[2] > 60:
            try:
                u, a = slurm.qos_cpu_limits(qos)
            except slurm.SlurmError:
                u, a = None, None
            cached = self.qos_caps[qos] = (u, a, time.time())
        return cached[0]

    def user_room(self, jobs) -> dict | None:
        """CPUs each user has left under the per-user cap of each QOS they run in,
        and the QOS most of their running CPUs are in (for jobs that name none)."""
        if self.cfg["limits"]["mode"] != "global":
            return None
        used = collections.defaultdict(collections.Counter)      # qos -> user -> CPUs
        for j in jobs:
            if j["state"] in ("R", "CF") and j["qos"] not in ("", "(null)"):
                used[j["qos"]][j["user"]] += j["cpus"]
        by_qos, main = {}, {}
        for qos, users in used.items():
            cap = self.qos_cap(qos)
            if not cap:
                continue
            by_qos[qos] = {u: int(cap) - c for user, c in users.items()
                           if (u := self.uid(user)) is not None}
        for user in {u for users in used.values() for u in users}:
            uid = self.uid(user)
            if uid is not None:
                main[uid] = max(used, key=lambda q: used[q][user])
        return dict(by_qos=by_qos, user_qos=main)

    def poll_nodes(self):
        iv = self.cfg["cadence"]["node_poll_interval"]
        while not self.sh.stop.wait(0):
            t0 = time.time()
            try:
                n = slurm.nodes()
                p = slurm.partitions()
                r = slurm.reservations()
                with self.sh.lock:
                    jobs, queue_at = list(self.sh.jobs), self.sh.pending_at
                self.reserve(n, r, jobs, time.time())
                self.claim(n, jobs)
                with self.sh.lock:
                    self.sh.nodes, self.sh.parts, self.sh.nodes_at = n, p, time.time()
                    self.sh.claims_at = queue_at
            except Exception as e:                      # never let a poll kill the loop
                if self.sh.note_error("nodes", e):   # log each distinct failure once
                    self.log("error", where="nodes", detail=repr(e))
            if self.sh.stop.wait(max(0.0, iv - (time.time() - t0))):
                return

    def poll_queue(self):
        iv = self.cfg["cadence"]["queue_poll_interval"]
        while not self.sh.stop.wait(0):
            t0 = time.time()
            try:
                q = slurm.queue()
                with self.sh.lock:
                    self.sh.jobs = q
                    self.sh.pending = [j for j in q if j["state"] == "PD"]
                    self.sh.pending_at = t0      # what squeue showed was as of then
                self.shadow(q)
                if self.cfg["pin"]["enabled"]:
                    self.release_pins(q, time.time())
                if self.cfg["starvation"]["enabled"]:
                    self.widen_starving(q, time.time())
                iv_re = self.cfg["recheck"]["interval"]
                if iv_re and time.time() - self.recheck_at >= iv_re:
                    self.recheck_at = time.time()
                    self.recheck(q, time.time())
            except Exception as e:
                if self.sh.note_error("queue", e):   # log each distinct failure once
                    self.log("error", where="queue", detail=repr(e))
            if self.sh.stop.wait(max(0.0, iv - (time.time() - t0))):
                return

    def score(self):
        """Rebuild the decision surface and write it only if it changed."""
        iv = self.cfg["cadence"]["score_interval"]
        while not self.sh.stop.wait(0):
            t0 = time.time()
            try:
                with self.sh.lock:
                    nodes, parts, age = dict(self.sh.nodes), dict(self.sh.parts), \
                        time.time() - self.sh.nodes_at
                if nodes and age < self.cfg["cadence"]["policy_max_age"]:
                    self.score_once(nodes, parts, t0)
            except Exception as e:
                self.sh.note_error("score", e)
                self.log("error", where="score", detail=repr(e))
            if self.sh.stop.wait(max(0.0, iv - (time.time() - t0))):
                return

    def score_once(self, nodes, parts, now):
        total, free = self.shapes(nodes, parts)
        if not total:
            e = "no usable nodes in any batch partition (all down, drained or excluded?)"
            if self.sh.note_error("score", e):
                self.log("error", where="score", detail=e)
            return
        table = policy.build_table(self.cfg, total, free)
        # [policy] always_place: where jobs go when no node has room, by shape alone
        full = policy.build_table(self.cfg, total, total) \
            if self.cfg["policy"]["always_place"] else None
        # Compare the DECISIONS, not the rendered text: the text embeds a
        # timestamp, so comparing it would rewrite the file every tick and force
        # the plugin to reparse on every submission -- the exact churn the
        # write-on-change rule exists to prevent.
        sig = hash(tuple(sorted((k, v_) for k, vs in table.items()
                                for v_ in (",".join(vs),)))
                   + tuple(sorted((k, ",".join(v)) for k, v in (full or {}).items())))
        with self.sh.lock:
            jobs = list(self.sh.jobs)
        nf = self.node_free(nodes, parts)
        state = dict(nodes=nf,
                     later={n: nodes[n]["later"] for n in nf if nodes[n].get("later")},
                     queue_at=self.sh.claims_at,
                     tiers={p: parts[p]["tier"] for p in total},
                     room=self.user_room(jobs), enabled=self.pin_active(),
                     busy=self.busy_until(jobs, now) if self.cfg["pin"]["time_aware"] else None,
                     ends=self.ends(jobs, now) if self.wait_active() else None)
        # Free space goes out always: the plugin places a job only where some
        # node has room for it now, and pins from it when pinning is on.
        pin = state
        pin_sig = hash(repr(pin))
        self.snap = dict(table=table, full=full, cap=policy.caps(total), total=total,
                         free=free, version=self.version, node_free=state["nodes"],
                         tiers=state["tiers"], room=state["room"], busy=state["busy"],
                         later=state["later"], ends=state["ends"])
        # Rewrite when the partition decisions or the free space change, and
        # often enough that the plugin never sees the table or its free space
        # as stale: policy max_age/3, or pin max_age/2.
        age = now - self.last_write
        refresh = age > min(self.cfg["cadence"]["policy_max_age"] / 3,
                            self.cfg["pin"]["max_age"] / 2)
        if sig == self.last_sig and pin_sig == self.last_pin_sig and not refresh:
            return
        changed = sig != self.last_sig
        self.last_sig, self.last_pin_sig = sig, pin_sig
        self.last_write = now
        self.version += 1
        rendered = policy.render_lua(table, self.cfg, now, self.version, total, pin, full)
        self.last_rendered = rendered
        if not changed:                      # free space or refresh only: say nothing
            self.intend("write_policy_table", f"write {self.table_path}", "",
                        self.blocked(("advise", "enforce")),
                        lambda: self._write_atomic(self.table_path, rendered), quiet=True)
            if self.mode == "observe":
                self._write_atomic(self.dryrun_table_path, rendered)
            return
        free_cpu = sum(fc for v in free.values() for fc, _ in v)
        phi = sum(policy.phi_node(fc, fm, self.demand)
                  for v in free.values() for fc, fm in v)
        strand = sum(policy.stranded(fc, fm, self.cfg["policy"]["demand_median"])
                     for v in free.values() for fc, fm in v)
        self.log("policy", version=self.version, free_cpu=round(free_cpu),
                 phi=round(phi, 1), stranded=round(strand, 1),
                 distinct=len({tuple(v) for v in table.values()}),
                 reason="changed" if changed else "refresh")
        why = self.table_why(table, total, free, changed)
        self.intend("write_policy_table", f"write {self.table_path} (v{self.version})",
                    why, self.blocked(("advise", "enforce")),
                    lambda: self._write_atomic(self.table_path, rendered),
                    version=self.version, changes=self.table_changes)
        if self.mode == "observe":
            self._write_atomic(self.dryrun_table_path, rendered)

    def table_why(self, table, total, free, changed) -> str:
        """Explain a table write; leaves the per-bucket diff in self.table_changes."""
        shape = {(i, j): parts for (i, j, k), parts in table.items() if k == 0}
        prev, self.last_shape = self.last_shape, shape
        self.table_changes = []
        if prev is None:
            return f"first table since start ({len(shape)} shape buckets)"
        for key, parts in sorted(shape.items()):
            if prev.get(key) == parts:
                continue
            cpus, mem = policy.bucket_shape(self.cfg, *key)
            cost = policy.score_partitions(cpus, mem, total, free, self.demand)
            self.table_changes.append(dict(
                bucket=f"{key[0]},{key[1]},*", shape=f"{cpus}c x {mem // cpus} MB/CPU",
                before=",".join(prev.get(key, [])), after=",".join(parts),
                cost={p: round(c, 1) for c, p in cost}))
        return (f"{len(self.table_changes)} of {len(shape)} shape buckets changed as "
                "free capacity moved (cost = placeable capacity destroyed; "
                "partitions within tolerance of the cheapest are admitted)")

    def shadow(self, jobs):
        """Log what sjp would do with each newly seen job, next to what Slurm did:
        its partitions, and whether and where it would pin the node. The plugin
        acts at submission, which a dry run cannot intercept, so this is how its
        effect is made visible."""
        ids = {j["jobid"] for j in jobs}
        if self.seen is None:            # placed before we were watching
            self.seen = ids
            return
        if self.snap is None:            # no table yet; try these again next poll
            return
        s = self.snap
        new = []
        for j in jobs:
            if j["jobid"] in self.seen:
                continue
            actual = [p for p in j["partition"].split(",") if p]
            # GPU, --constraint, multi-node, reservation, interactive and other
            # partitions: the plugin leaves them to the site's rules; sjp has no
            # opinion on them.
            if (j.get("gpu") or j.get("features") or j.get("nnodes", 1) > 1
                    or j.get("reservation") or not set(actual) & set(s["cap"])):
                continue
            new.append((j, actual))
        # Once the plugin acts, its marks say which jobs it placed: read them all at once.
        notes = None
        if new and self.mode != "observe":
            try:
                notes = slurm.admin_comments()
            except slurm.SlurmError as e:
                self.log("error", where="shadow", detail=repr(e))
                notes = {}
        for j, actual in new:
            self.log("placement", **self.evaluate(j, actual, s, notes))
        self.seen = ids

    def evaluate(self, j, actual, s, notes=None) -> dict:
        """What sjp would do with one job, as a placement record. notes: job ID ->
        AdminComment, to tell the jobs the plugin placed from the others."""
        cpus = max(1, j["cpus"])
        mem = max(j.get("req_mem") or j["mem"], 512)
        running = j["state"] in ("R", "CF")
        acted = self.mode != "observe"
        would, key, refit = policy.plugin_lookup(s["table"], s["cap"], self.cfg, cpus, mem,
                                                 j["timelimit"])
        table_parts = ",".join(s["table"].get(key, []))

        # The node. A running job's own allocation is added back to its node, so
        # the choice is made against the cluster as it was just before it started.
        node = j.get("nodelist") if running else ""
        # Reservations that start before this job would end are taken as well.
        nf = policy.free_for(s["node_free"], s.get("later"),
                             (j.get("submit") or time.time()) if running else time.time(),
                             j["timelimit"])
        if node in nf:
            fc, fm, ps = nf[node]
            nf[node] = (fc + cpus, fm + mem, ps)
        # Only the nodes it may use (--nodelist, --exclude), and the partitions
        # holding them, as the plugin narrows them ([policy] user_nodes = follow).
        follow = self.cfg["policy"]["user_nodes"] != "ignore"
        want = slurm.expand_hostlist(j.get("req_nodes") or "") if follow and not running else []
        exc = slurm.expand_hostlist(j.get("exc_nodes") or "") if follow else []
        if want or exc:
            nf = policy.eligible(nf, want, exc)
            would = policy.restrict(would, nf, want) or would
        # The plugin places a job only if some node in those partitions has room
        # for it now; with [policy] always_place, otherwise by its shape alone.
        room = policy.has_room(cpus, mem, would, nf)
        by_shape = not room and s.get("full") is not None
        if by_shape:
            would, key, refit = policy.plugin_lookup(s["full"], s["cap"], self.cfg, cpus, mem,
                                                     j["timelimit"])
            if want or exc:
                would = policy.restrict(would, nf, want) or would
        if running:
            verdict = "allowed" if actual[0] in would else "excluded"
        else:
            verdict = "same" if set(actual) == set(would) else "different"
        cost = policy.score_partitions(cpus, mem, s["total"], s["free"], self.demand)
        why = (f"{cpus}c x {mem // cpus} MB/CPU, {j['timelimit']} min -> bucket "
               f"{key[0]},{key[1]},{key[2]} of table v{s['version']}: {table_parts}")
        if by_shape:
            why += f"; no node there has room now, so by shape (always_place): {','.join(would)}"
        if refit:
            why += f"; refit to the job's real size -> {','.join(would)}"

        # In advise/enforce the plugin has already acted; its marks say what it did.
        marks = {}
        if acted:
            if notes is not None:
                note = notes.get(j["jobid"], "")
            elif j.get("req_nodes"):
                try:
                    note = slurm.admin_comment(j["jobid"])
                except slurm.SlurmError:
                    note = ""
            else:
                note = ""
            marks = slurm.sjp_marks(note)
        sjp_pin, sjp_from = marks.get("pin", ""), marks.get("from", "")
        sjp_placed = bool(marks.keys() & {"placed", "pin", "released"})
        sjp_skipped = marks.get("skipped", "")
        # Recompute against what the job was given: sjp's partitions before the
        # pin, or, once the plugin acts, the partitions it set.
        allowed = sjp_from.split(",") if sjp_from else (actual if acted else would)

        pin, pin_parts = None, []
        uroom = ((s["room"] or {}).get("by_qos", {}).get(j["qos"]) or {}).get(self.uid(j["user"]))
        placed = sjp_placed if acted and notes is not None else \
            (sjp_pin != "" or room or by_shape)
        if not placed:
            would, verdict = [], "not placed"
            why += "; no node there has room now, so sjp leaves it to the site's rule" \
                if not room else "; see the sjp line in the slurmctld log for why"
            pin_why = "not placed"
        elif not self.cfg["pin"]["enabled"]:
            pin_why = "pinning is off"
        # A job the plugin pinned passed its checks, including the one-node limit
        # squeue cannot show.
        elif (reason := policy.pin_eligible(dict(j, req_nodes="", ntasks=1)
                                            if sjp_pin else j)):
            pin_why = reason
        elif uroom is not None and uroom < cpus:
            pin_why = f"the user is at the per-user CPU cap ({uroom} CPUs left)"
        elif not policy.has_room(cpus, mem, allowed, nf):
            pin_why = "no node has room for it now"
            if s.get("ends") is not None:
                pin, pin_parts, eta = policy.soonest_node(cpus, mem, allowed, nf, s["ends"],
                                                          time.time(), s["tiers"])
                pin_why = (f"no node has room now; {pin} is expected to have room first, "
                           f"in {eta / 3600:.1f} h (wait_for_room)" if pin else
                           "no node there can ever hold it")
        else:
            busy = None
            if s.get("busy") is not None:
                t = (j.get("submit") or time.time()) if running else time.time()
                busy = {n: until - t for n, until in s["busy"].items()}
            pin, pin_parts, info = policy.pick_node(cpus, mem, allowed, nf, s["tiers"],
                                                   self.demand, self.cfg["pin"]["min_gain"],
                                                   self.cfg["pin"]["min_ratio_gain"],
                                                   busy, j["timelimit"])
            pin_why = info["why"]
        if pin and node:
            pin_why += ("; Slurm chose the same node" if pin == node
                        else f"; Slurm chose {node}")
        return dict(jobid=j["jobid"], user=j["user"], name=j["name"], state=j["state"],
                    reason=j["reason"] if not running else "", submit=j.get("submit"),
                    node=node, cpus=cpus, mem_mb=mem, minutes=j["timelimit"],
                    actual=",".join(actual), would=",".join(would), verdict=verdict,
                    differences=self.differences(actual, would, cpus, mem, cost, s, running),
                    placed=placed, room=room, by_shape=by_shape,
                    pin=pin, pin_parts=",".join(pin_parts), pin_why=pin_why,
                    acted=acted, sjp_placed=sjp_placed, sjp_pin=sjp_pin, sjp_from=sjp_from,
                    sjp_eta=marks.get("eta", ""), sjp_by_shape=marks.get("room") == "no",
                    sjp_skipped=sjp_skipped,
                    why=why, cost={p: round(c, 1) for c, p in cost})

    @staticmethod
    def differences(actual, would, cpus, mem, cost, s, running) -> list[str]:
        """Each partition the two sides disagree on, with the reason in words."""
        room = {p for _, p in cost}

        def reason(p):
            if p not in s["total"]:
                return "not a partition sjp assigns"
            if not policy.feasible(cpus, mem, {p: s["total"][p]}):
                return "no node there is big enough"
            if p not in room:
                return "no node there has room now"
            return "a poorer fit for this job's shape"
        if running:
            p = actual[0]
            return [] if p in would else [f"sjp would not have allowed {p}: {reason(p)}"]
        out = [f"adds {p}" + (": it fits about as well now" if p in room else "")
               for p in sorted(set(would) - set(actual))]
        out += [f"drops {p}: {reason(p)}" for p in sorted(set(actual) - set(would))]
        return out

    def release_pins(self, jobs, now):
        """Drop the pin from any job the plugin pinned that did not start. Allowed
        in enforce mode even with the disable file present: a pin left behind
        can hold a job to one node, so undoing pins is always safe to do."""
        after = self.cfg["pin"]["release_after"]
        for j in jobs:
            jid = j["jobid"]
            if (j["state"] != "PD" or not j.get("req_nodes") or jid in self.released
                    or not j.get("submit") or now - j["submit"] < after
                    or (jid in self.waiting and not self.past_budget(j, now))):
                continue
            self.released.add(jid)
            try:
                note = slurm.admin_comment(jid)
            except slurm.SlurmError as e:
                self.log("error", where="release", detail=repr(e))
                continue
            marks = slurm.sjp_marks(note)
            if "pin" not in marks:
                continue                 # the user's own --nodelist: never touch it
            if "eta" in marks and not self.past_budget(j, now):
                # waiting for room (wait_for_room): the recheck moves it; it is
                # released only once the job is past its starvation budget
                self.released.discard(jid)
                self.waiting.add(jid)
                continue
            node, parts = marks["pin"], marks.get("from", "")
            argvs = slurm.cmd_release_pin(jid, parts, slurm.rename_mark(
                note, "pin", "released", drop=("eta",)))
            def release(jid=jid, argvs=argvs):
                # A release that fails is tried again at the next check.
                try:
                    done = all([self.apply_retrying(a) for a in argvs])
                except slurm.SlurmError:
                    self.released.discard(jid)
                    raise
                if not done:
                    self.released.discard(jid)
                return done
            self.intend("release_pin", " && ".join(slurm.cmdline(a) for a in argvs),
                        (f"waiting for room on {node} past its starvation budget "
                         f"({j['reason']})") if "eta" in marks else
                        (f"pinned at submission but still pending after "
                         f"{now - j['submit']:.0f} s ({j['reason']})"),
                        None if self.mode == "enforce" else f"mode={self.mode}",
                        release, jobid=jid, node=node, parts=parts)
        live = {j["jobid"] for j in jobs}
        self.released &= live
        self.waiting &= live

    def past_budget(self, j, now) -> bool:
        """Whether a pending job has waited longer than its [starvation] budget
        (never, with the starvation guard off)."""
        c = self.cfg["starvation"]
        if not c["enabled"] or not j.get("eligible"):
            return False
        cpus = max(1, j["cpus"])
        kind = "fat" if (j.get("req_mem") or j["mem"]) / cpus >= c["fat_ratio_threshold"] \
            else "slim"
        return now - j["eligible"] >= c["budget_hours"][kind] * 3600

    # Pending reasons a wider partition list can help with.
    WIDEN_REASONS = ("Resources", "Priority")

    def widen_starving(self, jobs, now):
        """The starvation guard. sjp may give a job fewer partitions than a static
        rule would, and a pending job keeps them. So a job sjp placed, pending
        for Resources or Priority longer than its class's budget, is widened to
        every batch partition that can hold it. Slurm's priority and backfill then decide as
        usual, now across all those partitions."""
        s = self.snap
        if s is None:
            return
        c = self.cfg["starvation"]
        total = s["total"]
        for j in jobs:
            jid = j["jobid"]
            if (j["state"] != "PD" or j["reason"] not in self.WIDEN_REASONS
                    or jid in self.widened or "_" in jid or j.get("gpu")
                    or j.get("nnodes", 1) > 1
                    or j.get("req_nodes") or not j.get("eligible")):
                continue
            cpus = max(1, j["cpus"])
            mem = j.get("req_mem") or j["mem"]
            kind = "fat" if mem / cpus >= c["fat_ratio_threshold"] else "slim"
            budget = c["budget_hours"][kind] * 3600
            waited = now - j["eligible"]
            if waited < budget:
                continue
            self.widened.add(jid)
            current = [p for p in j["partition"].split(",") if p]
            if not current or not set(current) <= set(total):
                continue                 # interactive, GPU or other: not sjp's to widen
            # Only jobs sjp placed: a job the site's own rule placed keeps its
            # partitions (the plugin marks its jobs in AdminComment).
            try:
                marks = slurm.sjp_marks(slurm.admin_comment(jid))
            except slurm.SlurmError:
                continue
            if not marks.keys() & {"placed", "pin", "released"}:
                continue
            wide = sorted(policy.feasible(cpus, mem, total))
            exc = slurm.expand_hostlist(j.get("exc_nodes") or "") \
                if self.cfg["policy"]["user_nodes"] != "ignore" else []
            if exc:                      # only partitions with a node it may use
                ok = policy.eligible(s["node_free"], (), exc)
                wide = [p for p in wide if p in {q for _, _, ps in ok.values() for q in ps}]
            if not wide or set(wide) <= set(current):
                continue                 # already everywhere it can go
            argv = slurm.cmd_set_job_partitions(jid, wide)
            self.intend("widen_partitions", slurm.cmdline(argv),
                        f"pending {waited / 3600:.1f} h ({j['reason']}), over the "
                        f"{budget / 3600:g} h budget for {kind} jobs; widened to every "
                        "partition that can hold it",
                        self.blocked(("enforce",)), lambda: self.apply_retrying(argv),
                        jobid=jid, before=",".join(current), after=",".join(wide))
        self.widened &= {j["jobid"] for j in jobs}

    # Pending reasons a recheck can help with: the job could start somewhere.
    RECHECK_REASONS = ("None", "Resources", "Priority", BUSY_NODE)
    # The plugin's reasons for skipping a job that sjp places once room opens.
    TAKE_OVER = ("no room", "stale")

    def recheck(self, jobs, now):
        """Make sjp's choice again for every pending job it placed: jobs that
        started, ended or were cancelled since have changed what fits where. A
        job whose choice changed moves there if a node there has room for it
        now, dropping any pin it waited on. With [pin] wait_for_room, a job
        that still cannot start is pinned to the node now expected to have room
        for it first. Highest priority first; each move is counted against a
        node's free space, so the moves never add up to more than the cluster
        can start. Jobs past their starvation budget are left to the
        starvation guard."""
        s = self.snap
        if s is None:
            return
        wait = s.get("ends") is not None
        nf = {n: list(v) for n, v in s["node_free"].items()}
        notes = None
        for j in sorted(jobs, key=lambda j: -j.get("priority", 0)):
            jid = j["jobid"]
            if (j["state"] != "PD" or j["reason"] not in self.RECHECK_REASONS
                    or "_" in jid or j.get("gpu") or j.get("features") or j.get("reservation")
                    or j.get("nnodes", 1) > 1 or jid in self.widened
                    or self.past_budget(j, now)):
                continue
            current = [p for p in j["partition"].split(",") if p]
            if not current or not set(current) <= set(s["total"]):
                continue
            if notes is None:            # every job's marks, read once
                try:
                    notes = slurm.admin_comments()
                except slurm.SlurmError as e:
                    self.log("error", where="recheck", detail=repr(e))
                    return
            note = notes.get(jid, "")
            marks = slurm.sjp_marks(note)
            ours = bool(marks.keys() & {"placed", "released", "pin"})
            # Jobs the plugin skipped for want of room (or of fresh data) are
            # sjp's to place once room opens; the site's own rule placed them only
            # as its fallback. Other skips (GPU, reservation, ...) are not sjp's.
            if not ours and marks.get("skipped") not in self.TAKE_OVER:
                continue
            pinned = j.get("req_nodes") or ""
            if pinned and "eta" not in marks:
                continue                 # the user's own --nodelist, or a pin release_pins handles
            # sjp's own choice, before any pin narrowed it to one node's partitions
            base = [p for p in marks.get("from", "").split(",") if p] \
                if pinned or "pin" in marks else current
            base = base or current
            if not pinned and "pin" in marks:
                # the pin went, but an earlier move did not finish: tidy the mark
                note = slurm.rename_mark(note, "pin", "released", drop=("eta",))
                marks = slurm.sjp_marks(note)
            cpus = max(1, j["cpus"])
            mem = max(j.get("req_mem") or j["mem"], 512)
            if pinned in nf:             # its own claim on the node it waits for
                nf[pinned][0] += cpus
                nf[pinned][1] += mem
            would = policy.plugin_lookup(s["table"], s["cap"], self.cfg, cpus, mem,
                                         j["timelimit"])[0]
            # only the nodes it may use (--exclude, if followed), and partitions holding them
            exc = slurm.expand_hostlist(j.get("exc_nodes") or "") \
                if self.cfg["policy"]["user_nodes"] != "ignore" else []
            mine = policy.eligible({n: tuple(v) for n, v in nf.items()}, (), exc)
            if exc:
                would = policy.restrict(would, mine)
                base = policy.restrict(base, mine) or base
            free = policy.free_for(mine, s.get("later"), now, j["timelimit"])
            fits = [n for n, (fc, fm, ps) in free.items()
                    if fc >= cpus and fm >= mem and set(ps) & set(would)]
            waited = now - (j.get("eligible") or now)
            if fits and (pinned or set(would) != set(current)):
                node = min(fits, key=lambda n: free[n][0])      # the tightest fit
                nf[node][0] -= cpus
                nf[node][1] -= mem
                moved = ",".join(base) + ">" + ",".join(would)
                if pinned:
                    note2 = slurm.set_mark(slurm.rename_mark(note, "pin", "released",
                                                             drop=("eta",)), "moved", moved)
                    argvs = slurm.cmd_release_pin(jid, ",".join(would), note2)
                else:
                    # a job sjp takes over is sjp's from now on
                    own = note if ours else slurm.rename_mark(note, "skipped", "placed",
                                                              ",".join(would))
                    argvs = [slurm.cmd_set_job_partitions(jid, would) + [
                        f"admincomment={slurm.set_mark(own, 'moved', moved)}"]]
                self.intend("move_partitions", " && ".join(slurm.cmdline(a) for a in argvs),
                            f"pending {waited / 60:.0f} min ({j['reason']}); sjp now chooses "
                            f"{','.join(would)}, and {cpus} CPUs x {mem} MB fit on {node} now"
                            + (f"; no longer waiting for {pinned}" if pinned else ""),
                            self.blocked(("enforce",)),
                            lambda argvs=argvs: all([self.apply_retrying(a) for a in argvs]),
                            jobid=jid, before=",".join(base), after=",".join(would), node=node)
                continue
            target = None
            if wait and not fits and not policy.pin_eligible(dict(j, req_nodes="")):
                target, parts, eta = policy.soonest_node(cpus, mem, base, free, s["ends"], now,
                                                         s["tiers"])
            if not target or target == pinned:
                if pinned in nf:         # stays where it waits
                    nf[pinned][0] -= cpus
                    nf[pinned][1] -= mem
                continue
            nf[target][0] -= cpus
            nf[target][1] -= mem
            # placed=a,b -> pin=node;from=a,b, where the mark was
            if "pin" in marks:
                note2 = slurm.rename_mark(note, "pin", "pin", target)
            elif ours:
                note2 = slurm.rename_mark(note, "released" if "released" in marks else "placed",
                                          "pin", target)
            else:                        # taken over: skipped for want of room
                note2 = slurm.rename_mark(note, "skipped", "pin", target)
            note2 = slurm.set_mark(slurm.set_mark(note2, "from", ",".join(base)),
                                   "eta", hours(eta))
            argvs = slurm.cmd_pin(jid, target, ",".join(parts), note2)
            self.intend("pin_waiting", " && ".join(slurm.cmdline(a) for a in argvs),
                        f"pending {waited / 60:.0f} min ({j['reason']}); no node in "
                        f"{','.join(base)} has room for {cpus} CPUs x {mem} MB now; {target} "
                        f"is expected to have room first, in {eta / 3600:.1f} h"
                        + (f" (was {pinned})" if pinned else ""),
                        self.blocked(("enforce",)),
                        lambda argvs=argvs: all([self.apply_retrying(a) for a in argvs]),
                        jobid=jid, before=pinned, node=target, parts=",".join(parts))

    @staticmethod
    def apply_retrying(argv, tries=4, wait=2.0) -> bool:
        """slurm.apply, for job updates. Slurm answers some updates with EAGAIN
        ("Resource temporarily unavailable") while it is busy with the job, and
        the same update succeeds a moment later. A job that has started in the
        meantime needs no further change."""
        for i in range(tries):
            try:
                return slurm.apply(argv)
            except slurm.SlurmError as e:
                if "no longer pending" in str(e):
                    return True
                if "temporarily unavailable" not in str(e) or i == tries - 1:
                    raise
                time.sleep(wait)
        return False

    def preflight(self):
        """Record what this run is able to change, and where the live cluster
        differs from what the config assumes."""
        info = dict(actuation=slurm.actuation(),
                    writes_table=self.blocked(("advise", "enforce")) is None,
                    table_path=self.table_path, limits_mode=self.cfg["limits"]["mode"],
                    disable_file_present=self.disabled(),
                    warnings=[f"{r}; please update sjp.toml" for r in self.cfg.get("_renamed", [])])
        try:
            nodes = slurm.nodes()
            keep, dropped = self.partition_filter(slurm.partitions(), nodes)
            info["batch_partitions"] = keep
            info["excluded_partitions"] = dropped
            info["excluded_gpu_nodes"] = sorted(n for n, d in nodes.items()
                                                if not self.usable_node(d))
            if not keep:
                info["warnings"].append("no partitions left to assign")
        except slurm.SlurmError as e:
            info["warnings"].append(f"partitions: {e}")
        lm = self.cfg["limits"]["mode"]
        if lm not in ("off", "global", "flex"):
            info["warnings"].append(f"limits.mode = {lm} is unknown: use off, global or flex")
        if lm == "flex":
            q = self.cfg["limits"]["flex_qos_name"]
            try:
                if not slurm.qos_exists(q):
                    info["warnings"].append(f"limits.mode = flex, but QOS {q} does not "
                                            "exist: jobs cannot be moved there")
            except slurm.SlurmError as e:
                info["warnings"].append(f"QOS {q}: {e}")
        self.log("preflight", **info)

    def act(self):
        """Elastic limits: the global cap pulse, or flex-QOS moves."""
        iv = self.cfg["cadence"]["act_interval"]
        while not self.sh.stop.wait(0):
            t0 = time.time()
            try:
                self.act_once()
            except Exception as e:
                self.sh.note_error("act", e)
                self.log("error", where="act", detail=repr(e))
            if self.sh.stop.wait(max(0.0, iv - (time.time() - t0))):
                return

    # Pending reasons of jobs that only wait for room or their turn.
    WAITING_REASONS = ("None", "Resources", "Priority")

    def act_once(self):
        with self.sh.lock:
            nodes, parts = dict(self.sh.nodes), dict(self.sh.parts)
            pend = list(self.sh.pending)
        if not nodes:
            return
        total, free = self.shapes(nodes, parts)
        if not total:
            return
        total_cpu = sum(c for v in total.values() for c, _ in v) or 1
        phi = sum(policy.phi_node(fc, fm, self.demand)
                  for v in free.values() for fc, fm in v)
        # [limits] count_pending: jobs waiting for room get the idle capacity
        # first, so their CPUs count as taken. Jobs pinned to a node already are.
        waiting = sum(j["cpus"] for j in pend if j["reason"] in self.WAITING_REASONS
                      and not j.get("req_nodes")) if self.cfg["limits"]["count_pending"] else 0
        idle_frac = max(0.0, phi - waiting) / total_cpu
        capped = [j for j in pend if j["reason"] in slurm.LIMIT_REASONS]
        # Jobs a pulse could release, per QOS: held by a CPU cap of their QOS,
        # and small enough for some node's free space right now.
        nf = self.node_free(nodes, parts)
        held = collections.Counter(
            j["qos"] for j in pend if j["reason"] in slurm.CAP_REASONS
            and any(fc >= j["cpus"] and fm >= (j.get("req_mem") or j["mem"])
                    for fc, fm, _ in nf.values()))

        if self.cfg["limits"]["mode"] == "flex":
            self.unflex(pend, time.time())
            if idle_frac >= self.cfg["limits"]["min_idle_share"]:
                later = {n: nodes[n]["later"] for n in nf if nodes[n].get("later")}
                self.flex(pend, nf, time.time(), later)
        if self.cfg["limits"]["mode"] == "global":
            for qos in set(held) | set(self.limiters):
                lim = self.limiter(qos)
                before = (lim.cur_u, lim.cur_a)
                change = lim.observe(idle_frac, held.get(qos, 0))
                if change:
                    change, before = self.follow_admin(qos, lim, change, before)
                if change:
                    self.set_caps(qos, change, before, idle_frac, held.get(qos, 0))
        self._write_atomic(self.status_path, json.dumps(dict(
            ts=time.time(), mode=self.mode, version=self.version,
            idle_fraction=round(idle_frac, 4), total_cpu=total_cpu, waiting_cpus=waiting,
            capped_jobs=len(capped), pending=len(pend),
            limits=[lim.state() for lim in self.limiters.values()],
            errors=self.sh.errors), indent=1))

    def flex(self, pend, nf, now, later=None):
        """Flex mode: move jobs held only by the CPU cap of their QOS to the flex
        QOS, highest priority first, as many as fit in the free space now. Each
        is counted against a node's free space, so the moves never add up to
        more than the cluster can start."""
        c = self.cfg["limits"]
        target = c["flex_qos_name"]
        free = {n: [fc, fm, set(ps)] for n, (fc, fm, ps) in nf.items()}
        # Jobs moved earlier that have not started yet still claim their space.
        for _, node, cpus, mem in self.flexed.values():
            if node in free:
                free[node][0] -= cpus
                free[node][1] -= mem
        for j in sorted(pend, key=lambda j: -j["priority"]):
            jid = j["jobid"]
            if (j["reason"] not in slurm.CAP_REASONS or j["qos"] == target
                    or "[" in jid or j.get("gpu") or j.get("req_nodes")
                    or j.get("nnodes", 1) > 1
                    or now - self.flex_seen.get(jid, float("-inf")) < c["cooldown_seconds"]):
                continue
            cpus, mem = max(1, j["cpus"]), j.get("req_mem") or j["mem"]
            parts = set(j["partition"].split(","))
            # Reservations that start before the job would end count against it.
            soon = policy.reserved_before(later, now, j.get("timelimit"))
            fits = [n for n, (fc, fm, ps) in free.items()
                    if fc - soon.get(n, (0, 0))[0] >= cpus
                    and fm - soon.get(n, (0, 0))[1] >= mem and ps & parts]
            if not fits or not self.flex_allowed(j["user"], j["account"], target):
                continue
            node = min(fits, key=lambda n: free[n][0])      # the tightest fit
            free[node][0] -= cpus
            free[node][1] -= mem
            self.flex_seen[jid] = now
            try:
                # qos=<own>><flex>: where it came from, so it can go back. The
                # "flex" key is what older versions wrote.
                note = slurm.set_mark(slurm.admin_comment(jid), "qos",
                                      f"{j['qos']}>{target}", drop=("flex",))
            except slurm.SlurmError as e:
                self.log("error", where="flex", detail=repr(e))
                continue
            argv = slurm.cmd_set_job_qos(jid, target, note)
            self.intend("flex_job", slurm.cmdline(argv),
                        f"held by the CPU cap of QOS {j['qos']} ({j['reason']}), and "
                        f"{cpus} CPUs x {mem} MB fit on {node} now",
                        self.blocked(("enforce",)), lambda: self.apply_retrying(argv),
                        jobid=jid, before=j["qos"], after=target, node=node)
            if self.mode == "enforce":
                self.flexed[jid] = (now, node, cpus, mem)

    def flex_allowed(self, user, account, target) -> bool:
        """Whether this user may use the flex QOS under this account, read from
        sacctmgr every ten minutes. If not, the job is not moved, and the user is
        named in the log once."""
        now = time.time()
        try:
            if self.flex_users is None or now - self.flex_users[0] > 600:
                if not self.cluster:
                    self.cluster = slurm.cluster_name()
                self.flex_users = (now, slurm.user_qos(self.cluster))
        except slurm.SlurmError as e:
            self.log("error", where="flex", detail=repr(e))
            return False
        have = self.flex_users[1].get((user, account))
        if have is None or target in have:
            return True                  # unknown association: let Slurm decide
        if (user, account) not in self.flex_warned:
            self.flex_warned.add((user, account))
            self.log("error", where="flex",
                     detail=f"user {user} (account {account}) may not use QOS {target}, "
                            f"so their jobs held by the CPU cap are not moved there; "
                            f"allow it with: sacctmgr modify user {user} set qos+={target}")
        return False

    def unflex(self, pend, now):
        """Put jobs back in their own QOS if they were moved to the flex QOS and
        have not started within flex_revert_after; and, after a restart, every
        one still pending there. Allowed in enforce even with the disable file
        present: it only undoes a change of sjp's."""
        c = self.cfg["limits"]
        for j in pend:
            jid = j["jobid"]
            if j["qos"] != c["flex_qos_name"]:
                continue
            moved = self.flexed.get(jid, (None,))[0]
            if moved is not None and now - moved < c["flex_revert_after"]:
                continue
            try:
                marks = slurm.sjp_marks(slurm.admin_comment(jid))
                orig = marks.get("qos", "").partition(">")[0] or marks.get("flex")
            except slurm.SlurmError as e:
                self.log("error", where="flex", detail=repr(e))
                continue
            self.flexed.pop(jid, None)
            if not orig:
                continue                 # put there by someone else: not sjp's to move
            self.flex_seen[jid] = now
            argv = slurm.cmd_set_job_qos(jid, orig)
            self.intend("unflex_job", slurm.cmdline(argv),
                        f"moved to QOS {j['qos']} but still pending ({j['reason']})"
                        + (f" after {now - moved:.0f} s" if moved else " when sjpd started"),
                        None if self.mode == "enforce" else f"mode={self.mode}",
                        lambda: self.apply_retrying(argv),
                        jobid=jid, before=j["qos"], after=orig)
        live = {j["jobid"] for j in pend}
        self.flexed = {k: v for k, v in self.flexed.items() if k in live}
        self.flex_seen = {k: v for k, v in self.flex_seen.items()
                          if now - v < c["cooldown_seconds"]}

    def follow_admin(self, qos, lim, change, before):
        """Caps an admin set by hand win. Before a pulse, its base is re-read from
        the QOS, so the raise starts from the caps as they are now. At the end of
        one, caps that differ from the raised ones were changed by hand during
        the pulse: they are kept, and become the base. Returns the (change,
        before) to apply; change is None when there is nothing to do."""
        try:
            live = slurm.qos_cpu_limits(qos)
        except slurm.SlurmError as e:
            self.log("error", where="limits", detail=repr(e))
            # Lowering with the caps we know is safe; raising blind is not.
            return (change if not lim.raised else None), before
        if not lim.raised:                       # the pulse just ended
            if live not in (before, (lim.base_u, lim.base_a)) and self.mode == "enforce":
                lim.set_base(*live)
                self.remember_pulse(qos, False)
                self.log("limits", qos=qos, per_user=live[0], per_account=live[1],
                         source="changed by hand during the pulse; kept as base")
                return None, before
            return change, before
        if live != (lim.base_u, lim.base_a):     # about to raise
            self.log("limits", qos=qos, per_user=live[0], per_account=live[1],
                     source="changed by hand; new base")
            raised_at = lim.raised_at
            lim.raised_at = None
            lim.set_base(*live)
            if not lim.known:
                return None, before
            lim.raised_at = raised_at
            lim.cur_u = int(lim.base_u * lim.ceiling)
            lim.cur_a = int(lim.base_a * lim.ceiling)
            change = (lim.cur_u, lim.cur_a)
        return change, live

    def set_caps(self, qos, caps, before, idle_frac=None, held=None):
        """Log and, in enforce, apply a change of the per-user/per-account caps.
        Going back to base is allowed even with the disable file present: it only
        ever makes the cluster more conservative."""
        per_user, per_acct = caps
        lim = self.limiter(qos)
        self.log("limits", qos=qos, idle_fraction=idle_frac, per_user=per_user,
                 per_account=per_acct, held_jobs=held)
        unset = float("inf")                    # a QOS with no cap set reports None
        lowering = per_user <= (before[0] or unset) and per_acct <= (before[1] or unset)
        blocked = self.blocked(("enforce",)) if not lowering else \
            (None if self.mode == "enforce" else f"mode={self.mode}")
        argv = slurm.cmd_set_qos_cpu_limits(qos, per_user, per_acct)

        def run():                      # only called when the change is really made
            if not lowering:
                self.remember_pulse(qos, True)
            done = slurm.apply(argv, timeout=30.0)
            if lowering and done:
                self.remember_pulse(qos, False)
            return done
        self.intend("set_qos_cpu_limits", slurm.cmdline(argv), lim.why, blocked, run,
                    qos=qos, before_user=before[0],
                    before_account=before[1], per_user=per_user, per_account=per_acct,
                    idle_fraction=idle_frac, held_jobs=held)

    def limiter(self, qos):
        """The pulse controller for a QOS, created on first use. Its base caps are
        the recorded ones if a previous run stopped mid-pulse, else the QOS's own."""
        lim = self.limiters.get(qos)
        if lim is None:
            rec = self.pulse_record.get(qos)
            if rec:
                base = (rec.get("per_user"), rec.get("per_account"))
            else:
                try:
                    base = slurm.qos_cpu_limits(qos)
                except slurm.SlurmError as e:
                    self.log("error", where="limits", detail=repr(e))
                    base = (None, None)
            lim = self.limiters[qos] = limits.LimitPulse(self.cfg, qos, base)
            if lim.known:
                self.log("limits", qos=qos, per_user=base[0], per_account=base[1],
                         source="state_file (a pulse was interrupted)" if rec else "the QOS")
            else:
                self.log("error", where="limits",
                         detail=f"QOS {qos} has no per-user and per-account CPU caps: "
                                "nothing to pulse there")
        return lim

    def resolve_limits(self):
        """Read the record of pulses a previous run left on (state_file)."""
        if self.cfg["limits"]["mode"] != "global":
            return
        try:
            with open(self.cfg["limits"]["state_file"]) as f:
                self.pulse_record = {q: r for q, r in json.load(f).items()
                                     if isinstance(r, dict)}
        except (OSError, ValueError, AttributeError):
            self.pulse_record = {}

    def remember_pulse(self, qos, raised: bool):
        """Keep a QOS's base caps on disk while its pulse is on, so a restart after
        a crash mid-pulse restores those rather than taking the raised ones as base."""
        lim = self.limiters[qos]
        if raised:
            self.pulse_record[qos] = dict(per_user=lim.base_u, per_account=lim.base_a)
        else:
            self.pulse_record.pop(qos, None)
        try:
            self._write_atomic(self.cfg["limits"]["state_file"], json.dumps(self.pulse_record))
        except OSError as e:
            self.log("error", where="limits", detail=repr(e))

    def restore_caps(self, when: str):
        """Put every QOS with a pulse on back to its base: at startup, those a
        previous run left raised; at shutdown, those raised now."""
        if self.cfg["limits"]["mode"] != "global" or self.mode != "enforce":
            return
        qoses = set(self.pulse_record) | {q for q, l in self.limiters.items() if l.raised}
        for qos in sorted(qoses):
            lim = self.limiter(qos)
            if not lim.known:
                continue
            try:
                live = slurm.qos_cpu_limits(qos)
            except slurm.SlurmError as e:
                self.log("error", where="limits", detail=repr(e))
                continue
            lim.reset()
            lim.why = f"restore base caps at {when}"
            if live != (lim.base_u, lim.base_a):
                self.set_caps(qos, (lim.base_u, lim.base_a), live)
            else:
                self.remember_pulse(qos, False)

    @staticmethod
    def _write_atomic(path, text):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = f"{path}.tmp.{os.getpid()}"
        with open(tmp, "w") as f:
            f.write(text)
            f.flush(); os.fsync(f.fileno())
        os.replace(tmp, path)               # atomic; a reader never sees a half file
        return True

    # ---------------------------------------------------------------- run
    def run(self):
        self.open_logs()
        self.log("start", version=__import__("sjp").__version__,
                 limits_mode=self.cfg["limits"]["mode"],
                 cadence=self.cfg["cadence"])
        self.preflight()
        self.resolve_limits()
        self.restore_caps("startup")
        threads = [threading.Thread(target=t, name=n, daemon=True)
                   for t, n in ((self.poll_nodes, "nodes"), (self.poll_queue, "queue"),
                                (self.score, "score"), (self.act, "act"))]
        # systemd and kill send SIGTERM; stop the same way as on ^C
        signal.signal(signal.SIGTERM, lambda *_: self.sh.stop.set())
        for t in threads:
            t.start()
        try:
            while all(t.is_alive() for t in threads):
                time.sleep(0.5)
        except KeyboardInterrupt:
            pass
        finally:
            self.sh.stop.set()
            for t in threads:
                t.join(timeout=3.0)
            self.restore_caps("shutdown")
            self.log("stop")


def cli_paths(g, state_dir, log_file, text_log):
    """Apply output paths from the command line. A path not given follows the ones
    that are, so pointing one output at a directory you can write never leaves
    another at a system default you cannot: --state-dir alone puts the logs there
    too, and --log-file alone puts the text log beside it (and vice versa)."""
    if state_dir:
        g["state_dir"] = state_dir
    base = (os.path.dirname(log_file or "") or os.path.dirname(text_log or "")
            or state_dir)
    if log_file:
        g["log_file"] = log_file
    elif base:
        g["log_file"] = os.path.join(base, "decisions.jsonl")
    if text_log is not None:
        g["text_log"] = text_log
    elif base:
        g["text_log"] = os.path.join(base, "sjp.log")


def main(argv=None):
    ap = argparse.ArgumentParser(prog="sjpd")
    ap.add_argument("-c", "--config")
    ap.add_argument("--mode", choices=("observe", "advise", "enforce"))
    ap.add_argument("--dry-run", action="store_true",
                    help="force mode=observe: log every action it would take, "
                         "with the command and the reason, and change nothing")
    ap.add_argument("--state-dir", help="override [general] state_dir; also where the "
                    "logs go unless --log-file/--text-log say otherwise")
    ap.add_argument("--log-file", help="override [general] log_file; the text log goes "
                    "next to it unless --text-log says otherwise")
    ap.add_argument("--text-log", help="override [general] text_log ('' = none)")
    ap.add_argument("--once", action="store_true",
                    help="one scoring pass to stdout, then exit (for testing)")
    ap.add_argument("--print-config", action="store_true")
    a = ap.parse_args(argv)

    cfg = config.load(a.config)
    if a.dry_run and a.mode not in (None, "observe"):
        ap.error("--dry-run means --mode observe")
    if a.mode or a.dry_run:
        cfg["general"]["mode"] = a.mode or "observe"
    cli_paths(cfg["general"], a.state_dir, a.log_file, a.text_log)
    if a.print_config:
        print(config.dump_defaults()); return 0

    d = Daemon(cfg)
    if a.once:
        # the configured logs here too, so --once and a real run produce the
        # same records rather than differing by invocation
        d.open_logs()
        d.preflight()
        d.resolve_limits()
        d.sh.nodes, d.sh.parts = slurm.nodes(), slurm.partitions()
        d.sh.nodes_at = time.time()
        try:
            d.sh.jobs = slurm.queue()
        except slurm.SlurmError as e:
            d.log("error", where="queue", detail=repr(e))
        d.score_once(d.sh.nodes, d.sh.parts, time.time())
        d.act_once()
        try:                    # a single pass has no "new" jobs: shadow them all
            d.seen = set()
            d.shadow(d.sh.jobs)
        except slurm.SlurmError as e:
            d.log("error", where="queue", detail=repr(e))
        print(d.last_rendered or "(no table produced)")
        return 0
    d.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
