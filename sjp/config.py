"""Configuration: every tunable in one TOML file, with documented defaults.

Nothing in the daemon reads a magic number directly; it all comes through here,
so a site can retune without touching code and a paper can report the settings.
"""
from __future__ import annotations
import copy, os, tomllib

DEFAULTS: dict = {
    "general": {
        # observe  - compute and log decisions, change nothing
        # advise   - also write the policy table (partitions only, no node pins)
        # enforce  - also pin nodes, release stale pins, and change QOS limits
        "mode": "observe",
        "state_dir": "/run/sjp",
        "log_file": "/var/log/sjp/decisions.jsonl",   # every record, JSON lines
        "text_log": "/var/log/sjp/sjp.log",          # the same, for people ("" = off)
        "disable_file": "/etc/sjp/disable",   # touch to fall back instantly
    },
    "cadence": {
        "score_interval": 1.0,        # seconds; the policy table refresh
        "node_poll_interval": 1.0,    # scontrol show nodes - small, cheap
        "queue_poll_interval": 8.0,   # squeue - takes a job-table lock, keep slow
        "act_interval": 15.0,         # how often the actor may issue updates
        "max_actions_per_interval": 5,
        "policy_max_age": 300.0,      # plugin ignores a table older than this
    },
    "topology": {
        # Empty = discover from `scontrol show partitions`. Listing some restricts
        # the packer to those. The exclusions below apply either way.
        "batch_partitions": [],
        "exclude_partitions": [],       # never assign these, by name
        "exclude_interactive": True,    # drop any partition named "interactive"
        # Ignore nodes with a GPU (Gres or CfgTRES gres/gpu); a partition left
        # with no nodes is dropped. GPU jobs are routed by the plugin, not here.
        "exclude_gpu_nodes": True,
    },
    "policy": {
        # Demand mix: (MB per CPU, weight) -- how much memory per CPU arriving
        # jobs ask for. The defaults are one cluster's; fit your own with
        # `python3 -m sjp.fit --since <date>`.
        "demand": [[800, 0.10], [1280, 0.15], [4267, 0.25],
                   [7680, 0.25], [14178, 0.15], [30720, 0.10]],
        "demand_median": 4267,        # for the stranding metric only
        "tolerance": 0.25,            # extra partitions admitted, CPUs of phi per job CPU
        "buckets": {
            "mem_per_cpu": [1024, 2048, 4096, 6144, 8192, 12288, 24576],
            "cpus": [1, 4, 8, 16, 32, 64, 128],
            "walltime_h": [1, 6, 24, 72],
        },
    },
    "pin": {
        # When a job can start the moment it is submitted, also choose its node
        # (--nodelist). Slurm still decides the partition by PriorityTier: the
        # node is chosen inside the highest-ranked partition that has room,
        # as the one that leaves the most room usable by other jobs. Enforce
        # mode only, because a pin that does not start must be released.
        "enabled": True,
        "min_gain": 1.0,          # nodes within this many placeable CPUs of the best
                                  # count as equally good by capacity...
        "min_ratio_gain": 0.1,    # ...and among those the one whose free memory per
                                  # CPU is closest to the job's wins. Pin only if the
                                  # winner beats the worst candidate by min_gain, or
                                  # by this much in |log ratio| (0.1 ~ 10%)
        "release_after": 60.0,    # seconds a pinned job may stay pending before sjpd
                                  # drops the pin and restores its partitions
        "max_age": 10.0,          # plugin places and pins only from node state this fresh
    },
    "starvation": {
        # A job placed in few partitions can wait while another partition would
        # take it. In enforce mode, a job sjp placed, pending for Resources or Priority
        # longer than its class's budget (hours since it became eligible) is
        # widened to every batch partition that can hold it. Not arrays, GPU jobs,
        # reservations or pinned jobs.
        "enabled": True,
        "budget_hours": {"fat": 1.0, "slim": 4.0},
        "fat_ratio_threshold": 6000,  # MB/CPU above which a job counts as "fat"
    },
    "limits": {
        # What to do, in enforce mode, when jobs are held only by the per-user or
        # per-account CPU cap of their QOS and would fit in idle hardware:
        #   off    - nothing.
        #   global - raise both caps of that QOS, for EVERYONE, for pulse_seconds,
        #            then put them back. The caps are read from each QOS itself;
        #            they are at base the rest of the time, and after a restart.
        #   flex   - move those jobs, one by one, to the QOS flex_qos_name, as
        #            many as fit in the free space now. Create that QOS first,
        #            with the caps you want on top of the normal ones, and allow
        #            it for every user (sacctmgr ... set qos+=flex); jobs of users
        #            who may not use it are not moved, and the log names them. A
        #            moved job that has not started within flex_revert_after goes back.
        "mode": "off",
        # While a pulse is on, the real caps are kept here, so that if sjpd stops
        # mid-pulse the next start restores them rather than taking the raised
        # ones as base.
        "state_file": "/var/lib/sjp/limits.json",
        "ceiling": 2.0,               # the raised caps, as a multiple of base
        "raise_above": 0.25,          # idle placeable fraction needed to pulse
        "lower_below": 0.10,          # end a pulse early if idle falls below this
        "hysteresis": 5,              # consecutive checks meeting both conditions
        "pulse_seconds": 60.0,        # how long the caps stay raised
        "cooldown_seconds": 300.0,    # minimum time at base between pulses
        "flex_qos_name": "flex",      # flex mode moves held jobs into this QOS
        "flex_revert_after": 60.0,    # seconds a moved job may stay pending there
    },
    "cohorts": {
        "key": ["user", "name", "cpus", "mem"],
        "min_members": 50,
        "max_queue_share": 0.5,       # one cohort may hold at most this much queue
    },
    "runtime_model": {
        "quantile": 0.95,
        "min_members": 20,
        "refresh_hours": 6,
        "margin": 1.25,
    },
}


def _merge(base: dict, over: dict) -> dict:
    out = copy.deepcopy(base)
    for k, v in over.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _merge(out[k], v)
        else:
            out[k] = v
    return out


def defaults() -> dict:
    """The built-in configuration, without looking at any file."""
    return copy.deepcopy(DEFAULTS)


def load(path: str | None = None) -> dict:
    path = path or os.environ.get("SJP_CONFIG", "/etc/sjp/sjp.toml")
    if not os.path.exists(path):
        return copy.deepcopy(DEFAULTS)
    with open(path, "rb") as f:
        return _merge(DEFAULTS, tomllib.load(f))


def dump_defaults() -> str:
    """Emit the default config as TOML, for `sjp-config --defaults`."""
    def fmt(v):
        if isinstance(v, bool):  return "true" if v else "false"
        if isinstance(v, str):   return f'"{v}"'
        if isinstance(v, list):  return "[" + ", ".join(fmt(x) for x in v) + "]"
        return str(v)
    out = []
    for sect, body in DEFAULTS.items():
        out.append(f"[{sect}]")
        for k, v in body.items():
            if isinstance(v, dict):
                out.append(f"\n[{sect}.{k}]")
                for k2, v2 in v.items():
                    out.append(f"{k2} = {fmt(v2)}")
            else:
                out.append(f"{k} = {fmt(v)}")
        out.append("")
    return "\n".join(out)
