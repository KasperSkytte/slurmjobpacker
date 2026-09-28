"""sqp-fit - fit [policy] demand to your cluster's own history.

    python3 -m sqp.fit --since 2026-01-01          # prints TOML for sqp.toml

The demand mix is what sqp measures free space against: how much memory per
CPU the jobs that arrive tend to ask for. It is fitted here from `sacct`
(read-only): single-node jobs, each weighted by the CPU-hours it held, and
summarised as six points, one per band of that distribution (0-10, 10-25,
25-50, 50-75, 75-90, 90-100%), each weighted by its band's width. A band is
represented by its upper edge -- the 99th percentile for the last, so a few
outliers do not set it -- which errs toward the memory-hungry end.
"""
from __future__ import annotations
import argparse, sys

from . import slurm

# (band start, band end, quantile representing it)
BANDS = [(0.00, 0.10, 0.10), (0.10, 0.25, 0.25), (0.25, 0.50, 0.50),
         (0.50, 0.75, 0.75), (0.75, 0.90, 0.90), (0.90, 1.00, 0.99)]


def weighted_quantile(pairs, q):
    """pairs: sorted [(value, weight)]; the value at cumulative weight share q."""
    total = sum(w for _, w in pairs)
    acc = 0.0
    for v, w in pairs:
        acc += w
        if acc >= q * total:
            return v
    return pairs[-1][0]


def fit(jobs):
    """jobs: [(cpus, mem_mb, seconds)] -> (demand [[MB/CPU, weight]], median MB/CPU)."""
    pairs = sorted((m / c, c * s) for c, m, s in jobs if c > 0 and m > 0 and s > 0)
    if not pairs:
        raise ValueError("no usable jobs")
    demand = [[round(weighted_quantile(pairs, q)), round(b - a, 2)] for a, b, q in BANDS]
    return demand, round(weighted_quantile(pairs, 0.5))


def from_sacct(since, until=None):
    """(cpus, mem_mb, seconds) for finished single-node jobs, via sacct."""
    args = ["sacct", "-a", "-X", "-n", "-P", "-S", since, "-o", "AllocTRES,ElapsedRaw"]
    if until:
        args += ["-E", until]
    out = []
    for line in slurm._run(args, timeout=600.0).splitlines():
        tres, _, secs = line.partition("|")
        t = dict(kv.split("=", 1) for kv in tres.split(",") if "=" in kv)
        try:
            if int(t.get("node", 1)) != 1:
                continue
            out.append((int(t["cpu"]), slurm._mem_mb(t["mem"]), int(secs or 0)))
        except (KeyError, ValueError):
            continue
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(prog="sqp-fit")
    ap.add_argument("--since", required=True, help="sacct start time, e.g. 2026-01-01")
    ap.add_argument("--until", help="sacct end time")
    a = ap.parse_args(argv)
    jobs = from_sacct(a.since, a.until)
    try:
        demand, median = fit(jobs)
    except ValueError as e:
        print(f"sqp-fit: {e} (is accounting enabled, and --since right?)", file=sys.stderr)
        return 1
    print(f"# fitted from {len(jobs):,} single-node jobs since {a.since}")
    print("[policy]")
    print("demand = [" + ", ".join(f"[{v}, {w}]" for v, w in demand) + "]")
    print(f"demand_median = {median}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
