# slurmqueuepacker

Places Slurm jobs by how well they fit the free space on each node right now, not only
by the memory per CPU they ask for.

## The problem

On shared nodes, what a job needs is memory *per CPU*. Clusters therefore split their
nodes into slim and fat partitions and route each job by that ratio. That keeps free
space well shaped in the long run, but it cannot see what is running. Slim nodes fill up
on CPUs first and are left with memory a high-memory job could use, while the fat nodes
run out of memory with CPUs idle, and high-memory jobs queue for them.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/img/mismatch-dark.svg">
  <img alt="One cluster at one moment: slim nodes with most of their memory free, fat nodes with memory full and CPUs idle, and 98 high-memory jobs waiting for the fat nodes." src="docs/img/mismatch-light.svg">
</picture>

An example from one production cluster. Over eleven months, at least 12% of the high-memory jobs
that waited more than an hour did so while a slim node had room for them
(`tools/figure_mismatch.py`).

## What it does

A daemon, `sqpd`, reads the cluster every second; a small Lua module, called from your
`job_submit.lua`, applies its decisions when a job is submitted. For each job it:

- chooses the partitions it may use, by how well it fits the free space in each;
- when it can start at once, pins its node: inside the highest-ranked partition
  (PriorityTier) with room, the node whose free memory per CPU best matches the job's;
- optionally, lifts the per-user and per-account CPU caps for a minute when jobs held
  only by those caps would fit in idle hardware.

Slurm still schedules, orders the queue and applies fair-share. If `sqpd` stops, jobs
keep their partition, or get a fallback rule you set.

## Status

**v<!-- x-release-please-start-version -->1.0.0<!-- x-release-please-end -->.** Tested end
to end against a real `slurmctld`, including every failure path; not yet run in
production. It starts in `observe` mode, which changes nothing.

## Installing

Needs Python 3.11+ (standard library only) and the Slurm commands `scontrol`, `squeue`,
`sacct` and `sacctmgr`, on the `slurmctld` host. Tested on Slurm 26.05.

| mode | reads the cluster | chooses partitions | pins nodes | pulses QOS caps |
|---|---|---|---|---|
| `observe` (default) | yes | no | no | no |
| `advise` | yes | yes | no | no |
| `enforce` | yes | yes | yes | if `[limits] mode = "global"` |

In `observe`, sqp runs only read-only Slurm commands (anything else is refused before it
starts, even under an admin account) and logs what it would do.

**1. Install**

<!-- x-release-please-start-version -->
```sh
sudo git clone --branch v1.2.0 https://github.com/kasperskytte/slurmqueuepacker /opt/slurmqueuepacker
cd /opt/slurmqueuepacker && python3 tests/test_policy.py        # ends in ALL PASS
```
<!-- x-release-please-end -->

**2. Dry run**, as your own user, from that directory:

```sh
python3 -m sqp.daemon --dry-run --state-dir ~/sqp-dry
tail -f ~/sqp-dry/sqp.log                                      # in another terminal
```

The log has one entry per job: where Slurm put it, where sqp would have, and why.
Its first entry lists the partitions sqp found (interactive partitions and GPU nodes are
left out; see `[topology]`). `python3 -m sqp.report ~/sqp-dry/decisions.jsonl --summary`
totals it up.

**3. Configure and run the service** (still `observe`):

```sh
sudo mkdir -p /etc/sqp
python3 -m sqp.daemon --print-config | sudo tee /etc/sqp/sqp.toml
python3 -m sqp.fit --since 2026-01-01    # your demand mix, for [policy] in sqp.toml
sudo cp systemd/sqpd.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now sqpd
```

Every setting is documented in the generated file. To use the QOS cap pulse, set
`[limits] mode = "global"` and the base caps to your QOS's values.

**4. Go live.** Enable Lua in `slurm.conf` (`JobSubmitPlugins=lua`), then:

- **If the cluster has a `job_submit.lua`,** load sqp in it and call `sqp.place()` where
  it chooses a batch job's partition. Everything else in your script stays as it is:

  ```lua
  local sqp = dofile("/opt/slurmqueuepacker/lua/sqp.lua")   -- once, at the top

  function slurm_job_submit(job_desc, part_list, submit_uid)
      -- ... your own rules: reservations, GPU jobs, interactive jobs ...
      return sqp.place(job_desc, submit_uid)                  -- partition (and node)
  end
  ```

- **If it has none,** copy `lua/job_submit.lua` next to `slurm.conf`. It sends `salloc` and
  `srun` jobs to an `interactive` partition if there is one, and the rest to sqp.

Run `scontrol reconfigure`. `sqp.place()` only sets the partition (and, for a pinned job,
the node), never rejects a job, and leaves interactive, GPU and reservation jobs alone.
Without a table from `sqpd` the job keeps its partition, unless you set a fallback:
`sqp.config.slim` and `sqp.config.fat`, used below and above `sqp.config.ratio_threshold`
MB per CPU (see the top of `lua/sqp.lua`). Then set `mode = "advise"` in `sqp.toml` and
restart `sqpd`, and later `mode = "enforce"`, which also pins nodes and pulses caps.

**Backing out.** `sudo touch /etc/sqp/disable` makes `sqp.place()` fall back on the next
submission; removing the call from `job_submit.lua` takes sqp out entirely. When `sqpd`
stops it puts the caps back to base; pins on jobs still pending stay, and can be
released with:

```sh
for j in $(squeue -h -t PD -o %i); do
  c=$(scontrol show job $j | grep -o 'AdminComment=sqp:pin=[^ ]*') || continue
  scontrol update jobid=$j reqnodelist= && scontrol update jobid=$j partition=${c##*from=}
done
```

**Upgrading.** `sudo git fetch --tags && sudo git checkout vX.Y.Z`, restart `sqpd`, and
run `scontrol reconfigure` so `slurmctld` loads the new `sqp.lua`.

## How it decides

**Partitions.** For every job shape (memory per CPU, CPUs, walltime), sqpd scores each
partition by how much *placeable capacity* the job would destroy: free space measured
against the mix of jobs the cluster actually runs (`[policy] demand`). The cheapest
partitions, within a tolerance, are allowed; Slurm tries them in PriorityTier order.
`sqp.place()` looks the job up in this table and drops any partition too small for it.

**Node pins.** Only for a job that can start at once, and only plain ones (one node, an
explicit memory request; no `--nodelist`, `--exclude`, `--constraint`, `--exclusive`,
hold or array). Among the
nodes that destroy the least capacity, the one whose free memory per CPU is closest to
the job's wins; if they are all about equal, Slurm chooses. The job is marked in its
`AdminComment`, and if it has not started within a minute the pin is removed and its
partitions restored. A user's own `--nodelist` is never touched.

**QOS caps.** Raised to twice the base, for everyone, for 60 s at a time, only when jobs
held by the per-user or per-account CPU cap would fit in idle hardware; then back to base,
with 5 minutes before the next pulse. Jobs started during a pulse keep running, so this
is kept short: a user submitting a large pool cannot take over the cluster.

All timings and thresholds are in `sqp.toml`.

## Analysing your own cluster

`tools/` replays and measures your accounting history, without touching Slurm:

```sh
bzcat slurm_acct_db_backup.bz2 > dump.sql                    # a mysqldump of slurm_acct_db
python3 tools/dump2sqlite.py dump.sql accounting.sqlite --cluster <ClusterName>
python3 tools/normalize.py accounting.sqlite
cp tools/site.example.toml site.toml                          # describe your partitions
python3 tools/stranding.py accounting.sqlite                  # stranded CPU-hours
python3 tools/simulate.py --db accounting.sqlite --start 2026-03-09 --days 14
python3 tools/figure_mismatch.py accounting.sqlite "2026-07-20 13:12" docs/img/
```

`simulate.py` replays the real arrival trace against the current rule and against sqp.
Compare policies within one run; its absolute numbers are not calibrated. The design and
its measurements are in `docs/design.html`.

## Development

```sh
python3 tests/test_policy.py          # behavioural tests; start no processes
tests/testcluster.sh start            # throwaway slurmctld as your user, no slurmd
export SLURM_CONF=/tmp/sqp-cluster/slurm.conf
python3 -m sqp.daemon -c /tmp/sqp-cluster/sqp.toml
tests/testcluster.sh stop
```

The test cluster's nodes are cloud nodes that never boot, so jobs are really allocated
(and stay `CONFIGURING`) and you can see the partitions and node sqp chose.

Releases are made by [release-please](https://github.com/googleapis/release-please) from
[Conventional Commits](https://www.conventionalcommits.org/) on `main`: `fix:` for a
patch, `feat:` for a minor release.
