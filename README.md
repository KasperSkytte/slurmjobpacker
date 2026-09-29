# slurmjobpacker

Slurm plugin for automatic partition and compute node selection to enable optimal packing of jobs on shared compute nodes (i.e. nodes run multiple jobs concurrently). This is achieved by optimally matching the Memory:CPU ratio of jobs to that currently available on nodes in order to reduce waste due to starvation of either CPU or Memory.

## The problem with sharing nodes

A computing cluster configured so that nodes can be shared by multiple jobs at once is often the preferred choice for smaller, local clusters compared to exclusive-only node access due to a potentially more efficient utilization of computing resources. In order to properly take advantage of this, users must both ensure to request hardware resources that match the job requirements as precisely as possible, but also avoid being in the way of other future jobs. Choosing the most appropriate hardware partition is challenging for many users, and many will get it wrong, leading to wasted computing resources, defeating the purpose of sharing nodes altogether. **The goal of `slurmjobpacker` is to BOTH automate the partition and node selection, but also to pack the cluster as tightly as possible**, in order to optimize resource utilization on clusters where nodes are shared among many users.


![packing](docs/img/packing-dark.svg)

In order to pack a cluster most efficiently when compute nodes run many jobs simultaneously, the average CPU+Memory shape of the jobs becomes very important. If the average job running on a node uses significantly more memory per CPU than that of the node, it will lead to memory starvation and therefore idle CPUs (fx a node with 256 allocatable threads and 1TB memory has a ratio of max 4.0 GB/thread, and thus the combined requirements of all jobs running on that node should preferably match that ratio). Similarly, allocating jobs that need little memory per CPU to, often much more expensive, nodes with extra memory can prevent memory demanding jobs to run there. Lastly, sometimes jobs with both low and high memory per CPU requirements actually fit perfectly together on the same node, so a simple [job submit LUA script](https://slurm.schedmd.com/job_submit_plugins.html) that assigns the partition to jobs simply based on a fixed memory per cpu threshold is not ideal either (e.g. more than x GB per CPU use a fat node, otherwise a slim node). 

`slurmjobpacker` uses the memory per CPU the jobs ask for and chooses the placement that leaves the most room usable for that mix of jobs considering the available space on all nodes at once, which is much better than leaving it up to the users and SLURMs default FIFO scheduling alone.

## How it works

A daemon, `sjpd`, reads the cluster every second; a small Lua module, called from your
`job_submit.lua`, applies its decisions when a job is submitted. For each job it:

- chooses the partitions it may use, by how well it fits the free space in each;
- when it can start at once, pins its node: inside the highest-ranked partition
  (PriorityTier) with room, the node whose free memory per CPU best matches the job's;
- optionally, lifts the per-user and per-account CPU caps for a minute when jobs held
  only by those caps would fit in idle hardware.

Slurm still schedules, orders the queue and applies fair-share. If `sjpd` stops, jobs
keep their partition, or get a fallback rule you set.

## Installation

Needs Python 3.11+ (standard library only). Tested on Slurm 26.05.

| mode | reads the cluster | chooses partitions | pins nodes | pulses QOS caps |
|---|---|---|---|---|
| `observe` (default) | yes | no | no | no |
| `advise` | yes | yes | no | no |
| `enforce` | yes | yes | yes | if `[limits] mode = "global"` |

In `observe`, sjp runs only read-only Slurm commands (anything else is refused before it
starts, even under an admin account) and logs what it would do.

**1. Install**

```sh
sudo git clone https://github.com/kasperskytte/slurmjobpacker /opt/slurmjobpacker
cd /opt/slurmjobpacker && python3 tests/test_policy.py        # ends in ALL PASS
```

**2. Dry run**, as your own user, from that directory:

```sh
python3 -m sjp.daemon --dry-run --state-dir ~/sjp-dry
tail -f ~/sjp-dry/sjp.log                                      # in another terminal
```

The log has one entry per job: where Slurm put it, where sjp would have, and why.
Its first entry lists the partitions sjp found (interactive partitions and GPU nodes are
left out; see `[topology]`). `python3 -m sjp.report ~/sjp-dry/decisions.jsonl --summary`
totals it up.

**3. Configure and run the service** (still `observe`):

```sh
sudo mkdir -p /etc/sjp
python3 -m sjp.daemon --print-config | sudo tee /etc/sjp/sjp.toml
python3 -m sjp.fit --since 2026-01-01    # your demand mix, for [policy] in sjp.toml
sudo cp systemd/sjpd.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now sjpd
```

Every setting is documented in the generated file. To use the QOS cap pulse, set
`[limits] mode = "global"` and the base caps to your QOS's values.

**4. Go live.** Enable Lua in `slurm.conf` (`JobSubmitPlugins=lua`), then:

- **If the cluster has a `job_submit.lua`,** load sjp in it and call `sjp.place()` where
  it chooses a batch job's partition. Everything else in your script stays as it is:

  ```lua
  local sjp = dofile("/opt/slurmjobpacker/lua/sjp.lua")   -- once, at the top

  function slurm_job_submit(job_desc, part_list, submit_uid)
      -- ... your own rules: reservations, GPU jobs, interactive jobs ...
      return sjp.place(job_desc, submit_uid)                  -- partition (and node)
  end
  ```

- **If it has none,** copy the example `lua/job_submit.lua` next to `slurm.conf` (usually `/etc/slurm/job_submit.lua`). It sends `salloc` and
  `srun` jobs to an `interactive` partition if there is one, and the rest to sjp.

Run `scontrol reconfigure`. `sjp.place()` only sets the partition (and, for a pinned job,
the node), never rejects a job, and leaves interactive, GPU and reservation jobs alone.
Without a table from `sjpd` the job keeps its partition, unless you set a fallback:
`sjp.config.slim` and `sjp.config.fat`, used below and above `sjp.config.ratio_threshold`
MB per CPU (see the top of `lua/sjp.lua`). Then set `mode = "advise"` in `sjp.toml` and
restart `sjpd`, and later `mode = "enforce"`, which also pins nodes and pulses caps.

**Backing out.** `sudo touch /etc/sjp/disable` makes `sjp.place()` fall back on the next
submission; removing the call from `job_submit.lua` takes sjp out entirely. When `sjpd`
stops it puts the caps back to base; pins on jobs still pending stay, and can be
released with:

```sh
for j in $(squeue -h -t PD -o %i); do
  c=$(scontrol show job $j | grep -o 'AdminComment=sjp:pin=[^ ]*') || continue
  scontrol update jobid=$j reqnodelist= && scontrol update jobid=$j partition=${c##*from=}
done
```

**Upgrading.** `sudo git fetch --tags && sudo git checkout vX.Y.Z`, restart `sjpd`, and
run `scontrol reconfigure` so `slurmctld` loads the new `sjp.lua`.

## How it decides...

**Partitions.** For every job shape (memory per CPU, CPUs, walltime), sjpd scores each
partition by how much *placeable capacity* the job would destroy: free space measured
against the mix of jobs currently running on the cluster (`[policy] demand`). The cheapest
partitions, within a tolerance, are allowed; Slurm tries them in `PriorityTier` order.
`sjp.place()` looks the job up in this table and drops any partition too small for it.

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

All timings and thresholds are in `sjp.toml`.

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

`simulate.py` replays the real arrival trace against the current rule and against sjp.
Compare policies within one run; its absolute numbers are not calibrated.

## Development

```sh
python3 tests/test_policy.py          # behavioural tests; start no processes
tests/testcluster.sh start            # throwaway slurmctld as your user, no slurmd
export SLURM_CONF=/tmp/sjp-cluster/slurm.conf
python3 -m sjp.daemon -c /tmp/sjp-cluster/sjp.toml
tests/testcluster.sh stop
```

The test cluster's nodes are cloud nodes that never boot, so jobs are really allocated
(and stay `CONFIGURING`) and you can see the partitions and node sjp chose.

Releases are made by [release-please](https://github.com/googleapis/release-please) from
[Conventional Commits](https://www.conventionalcommits.org/) on `main`: `fix:` for a
patch, `feat:` for a minor release.
