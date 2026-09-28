#!/usr/bin/env python3
"""Reconstruct per-node occupancy from accounting history and measure:

  1. stranded CPU-hours  - idle CPUs that no plausible job could have used,
     because the memory left on their node was too small per idle CPU;
  2. cross-class opportunities - moments where a fat job sat pending while a
     slim-partition node had a free shape that would have fitted it.

Both are computed event-driven, no resampling.
"""
import sqlite3, sys, re, bisect, collections

from sitefile import SITE, expand          # ./site.toml, $SQP_SITE or --site PATH

DB = sys.argv[1] if len(sys.argv) > 1 else sys.exit('usage: stranding.py accounting.sqlite')
T0 = SITE.t0
db = sqlite3.connect(DB); db.row_factory = sqlite3.Row

PART = SITE.node_part
SLIM, FAT = set(SITE.slim), set(SITE.fat)

CAP = {}
for r in db.execute("select node_name,tres from event_table"):
    if r['node_name'] not in PART: continue
    t = dict(kv.split('=') for kv in r['tres'].split(',') if '=' in kv)
    CAP[r['node_name']] = (int(t.get('1', 0)), int(t.get('2', 0)))

# ---- events -------------------------------------------------------------
ev = collections.defaultdict(list)          # time -> [(node, dcpu, dmem)]
pend = []                                   # fat pending jobs to probe
nj = 0
for r in db.execute("""select nodelist,nodes_alloc,alloc_cpus,alloc_mem_mb,start,end,
                       eligible,wait,cpus_req,mem_mb from jobs
                       where start>=? and end>start and alloc_cpus>0""", (T0,)):
    nodes = [n for n in expand(r['nodelist']) if n in CAP]
    if not nodes: continue
    k = len(nodes)
    dc, dm = r['alloc_cpus'] / k, r['alloc_mem_mb'] / k
    for n in nodes:
        ev[r['start']].append((n, dc, dm)); ev[r['end']].append((n, -dc, -dm))
    nj += 1
    if r['wait'] > 300 and r['cpus_req'] > 0 and r['mem_mb'] / r['cpus_req'] > SITE.ratio_threshold:
        pend.append((r['eligible'] + 60, r['cpus_req'], r['mem_mb'],
                     r['mem_mb'] / r['cpus_req'], r['wait']))
print(f'jobs placed on timeline: {nj:,}   fat jobs pending >5min: {len(pend):,}')

times = sorted(ev)
pend.sort()
used = {n: [0.0, 0.0] for n in CAP}
DEMAND = {'p50': SITE.demand_median, 'p75': SITE.demand[3][0]}   # MB/CPU

acc = {k: collections.Counter() for k in DEMAND}
busy_ch = idle_ch = 0.0
pi = 0
hits = collections.Counter(); probes = 0
prev = times[0]

for t in times:
    dtl = (t - prev) / 3600.0
    if dtl > 0:
        for n, (uc, um) in used.items():
            cc, cm = CAP[n]
            fc, fm = cc - uc, cm - um
            busy_ch += uc * dtl; idle_ch += fc * dtl
            for k, qd in DEMAND.items():
                acc[k]['stranded'] += max(0.0, fc - fm / qd) * dtl
        # probe fat jobs that became pending in this interval
        while pi < len(pend) and pend[pi][0] <= t:
            _, jc, jm, jr, w = pend[pi]; pi += 1; probes += 1
            best = None
            for n, (uc, um) in used.items():
                if PART[n] not in SLIM: continue
                cc, cm = CAP[n]
                if cc - uc >= jc and cm - um >= jm:
                    best = n; break
            if best: hits[PART[best]] += 1; hits['any'] += 1
    for n, dc, dm in ev[t]:
        u = used[n]; u[0] += dc; u[1] += dm
        if u[0] < 1e-6: u[0] = 0.0
        if u[1] < 1e-6: u[1] = 0.0
    prev = t

span = (times[-1] - times[0]) / 3600.0
cap_ch = sum(c for c, _ in CAP.values()) * span
print(f'\nwindow: {span/24:.0f} days, capacity {cap_ch:,.0f} CPU-hours')
print(f'  allocated : {busy_ch:>14,.0f} CPU-h  ({busy_ch/cap_ch*100:5.1f}%)')
print(f'  idle      : {idle_ch:>14,.0f} CPU-h  ({idle_ch/cap_ch*100:5.1f}%)')
for k, qd in DEMAND.items():
    s = acc[k]['stranded']
    print(f'  of which STRANDED at {k} demand ({qd} MB/CPU): {s:,.0f} CPU-h '
          f'({s/cap_ch*100:.1f}% of capacity, {s/idle_ch*100:.1f}% of idle)')
print(f'\ncross-class opportunities:')
print(f'  fat jobs probed while pending >5 min : {probes:,}')
print(f'  a SLIM node could have taken it now  : {hits["any"]:,} '
      f'({hits["any"]/probes*100:.1f}%)' if probes else '')
for p in SITE.slim:
    print(f'    via {p}: {hits[p]:,}')
