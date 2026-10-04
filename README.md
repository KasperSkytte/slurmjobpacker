# slurmjobpacker

SLURM plugin for automatic partition and compute node selection that also packs jobs optimally on clusters where compute nodes are shared by multiple users (i.e. no forced exclusive node access). It does so by matching the job shape in CPU+Memory+time space to what is currently available on the nodes, reducing waste from starvation of either CPUs or memory, and also preventing fragmentation by "packing jobs" tightly, leaving the most room possible for future jobs. Furthermore, it can dynamically adjust per-user or -account CPU limits temporarily depending on the overall partition or cluster load, or assign a different QOS, to better balance usage over time (fx clusters may have max CPU limits in place merely to prevent a few users from blocking the whole cluster, but during periods of low(er) activity there's no reason CPUs are idle when somebody needs them).

`slurmjobpacker` is a single sourced Lua function to use among your existing job submission logics defined in the [job submit Lua script](https://slurm.schedmd.com/job_submit_plugins.html), if any, so it easily integrates in any slurm configuration. It does not interfere with the normal scheduling by the slurm controller, priorities, fair-share, etc, it simply sets the partition and nodelist automatically at job submission, SLURM takes care of the rest.

## The problem with sharing nodes

A computing cluster configured so that nodes can be shared by multiple jobs at once is often preferred over exclusive node access for smaller, local clusters, because it can utilize computing resources more efficiently. To take proper advantage of this, users must both request resources that match their jobs' requirements as precisely as possible, but also avoid getting in the way of future jobs. Choosing the most appropriate hardware partition is challenging for many users, especially on very heterogeneous hardware, so naturally many users get it wrong, which results in wasted computing resources. **The goal of `slurmjobpacker` is BOTH to automate partition and node selection AND to pack the cluster as tightly as possible**, to optimize resource utilization mainly on clusters where many individual jobs share the same compute nodes.

![packing](docs/img/packing-dark.svg)

To pack a shared node cluster efficiently, the combined CPU and memory shape of the jobs on each node is very important. If the jobs on a node use significantly more memory per CPU on average than the node provides, the memory runs out first and the remaining CPUs sit idle. For example, a node with 256 allocatable threads and 1 TB of memory has at most 4 GB per thread, so the combined requirements of all jobs running on it should preferably match that ratio. Similarly, placing jobs that need little memory per CPU on nodes with extra memory, which are often much more expensive (especially in 2026!), can prevent memory-demanding jobs from running there. Lastly, sometimes a mix of jobs with both low and high memory-per-CPU requirements fit perfectly together on the same node, so a [job submit Lua script](https://slurm.schedmd.com/job_submit_plugins.html) that assigns partitions based only on a fixed memory-per-CPU threshold (e.g. a fat node above x GB per CPU, otherwise a slim node) is not ideal either as it doesn't consider running jobs at all. 

`slurmjobpacker` matches the memory and CPU "shape" of each job against the free space on all nodes at once, and chooses the placement that leaves the most usable room for the jobs that typically follow. This packs the cluster far better than leaving placement to users and Slurm alone.

## How it works

A daemon, `sjpd`, reads the cluster every second. A small Lua module, `sjp.lua`, is
called from your `job_submit.lua` and places each batch job when it is submitted:

- it chooses the partitions where the job fits the free space best;
- if the job can start now, it also chooses the node (in the highest `PriorityTier`
  partition with room, the node whose free memory per CPU best matches the job's);
- it returns `true` when it placed the job, or `false` and a reason (e.g. `"no room"`)
  when it could not, leaving the job untouched. Your script decides what to do then,
  e.g. apply its own fallback rule (see [Using sjp from job_submit.lua](#using-sjp-from-job_submitlua)).

```mermaid
flowchart TD
    sjpd["sjpd<br/>reads each node's free CPUs and memory,<br/>i.e. its free memory per CPU"] -. "every second" .-> table[("placement table")]
    submit(["sbatch"]) --> own
    subgraph lua ["job_submit.lua (on slurmctld)"]
        own["your own rules"] --> place{"sjp.place()<br/>does a node have room<br/>for the job now?"}
        place -- "no: returns false, reason" --> fallback["your fallback rule"]
    end
    table -.-> place
    place -- "yes: returns true" --> parts["sets partition and node<br/>matching the job's memory per CPU"]
    parts --> slurm(["Slurm schedules the job"])
    fallback --> slurm
```

Slurm still schedules, orders the queue and applies fair-share. Optionally, `sjpd` also
widens the partitions of sjp's jobs that have waited long, and helps jobs held only by
per-user CPU caps when they would fit on idle nodes.

The [wiki](https://github.com/KasperSkytte/slurmjobpacker/wiki) explains all of this in detail.

## Installation
Follow the below steps manually, or use the provided ansible role under [ansible-role-slurmjobpacker](./ansible-role-slurmjobpacker/).

Needs Python 3.11+ (standard library only). Tested on Slurm 24.11 and 26.05.

<!-- x-release-please-start-version -->
```sh
sudo git clone --branch v1.11.0 https://github.com/kasperskytte/slurmjobpacker /opt/slurmjobpacker
cd /opt/slurmjobpacker && python3 tests/test_policy.py        # ends in ALL PASS
```
<!-- x-release-please-end -->

**Try it out in observe mode first.** As your own user:

```sh
python3 -m sjp.daemon --dry-run --state-dir ~/sjp-dry
tail -f ~/sjp-dry/sjp.log                                      # in another terminal
```

The log shows, for each new job, where Slurm put it and where sjp would have, and why.

**Run the service.**

```sh
sudo mkdir -p /etc/sjp
python3 -m sjp.daemon --print-config | sudo tee /etc/sjp/sjp.toml
sudo cp systemd/sjpd.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now sjpd
```

It starts in `observe` mode: it only reads the cluster and logs what it would do.

## Configuration

`sjpd` reads `/etc/sjp/sjp.toml`; restart it after a change. Partitions, nodes and QOS
caps are read from Slurm, so most sites only set how far sjp may go:

| `[general] mode` | chooses partitions | pins nodes, widens long waits | helps jobs held by CPU caps |
|---|---|---|---|
| `observe` (default) | no | no | no |
| `advise` | yes | no | no |
| `enforce` | yes | yes | if `[limits] mode` is set |

Every setting is described in the [configuration reference](https://github.com/KasperSkytte/slurmjobpacker/wiki/Configuration-reference).
Helping jobs held by QOS CPU caps (`[limits] mode`) has its own page:
[QOS limits and flex](https://github.com/KasperSkytte/slurmjobpacker/wiki/QOS-limits-and-flex).

## Using sjp from job_submit.lua

Set `JobSubmitPlugins=lua` in `slurm.conf`. Load `sjp.lua` once at the top of your
`job_submit.lua`, and call `sjp.place()` where you choose a batch job's partition:

```lua
local ok, sjp = pcall(dofile, "/opt/slurmjobpacker/lua/sjp.lua")
if not ok then sjp = nil end        -- without sjp, your own rules still apply

function slurm_job_submit(job_desc, part_list, submit_uid)
    -- your own rules first: reservations, GPU jobs, interactive jobs, ...

    if sjp and sjp.place(job_desc, submit_uid) then
        return slurm.SUCCESS         -- placed by sjp
    end

    -- not placed: your fallback, for example by memory per CPU
    job_desc.partition = "slim1,slim2"
    return slurm.SUCCESS
end
```

Then run `scontrol reconfigure` (again after every sjp upgrade). If you have no
`job_submit.lua` yet, copy [`lua/job_submit.lua`](lua/job_submit.lua) next to
`slurm.conf` and set the few variables at its top.

`sjp.place()` never rejects a job. It returns `true` when it placed the job, or `false`
and a reason (such as `"no room"`) when it left the job for your own rules. What sjp did
is noted in the job's `AdminComment` and in the `slurmctld` log. See
[Using sjp in job_submit.lua](https://github.com/KasperSkytte/slurmjobpacker/wiki/Using-sjp-in-job-submit.lua) and
[Logs and AdminComment](https://github.com/KasperSkytte/slurmjobpacker/wiki/Logs-and-AdminComment).

To switch it off, `sudo touch /etc/sjp/disable`: from the next submission
`sjp.place()` returns `false`, so only your own rules apply.

## Seeing it live

`sjp-viz` shows the cluster live in a web browser: every node a box of CPUs × memory ×
time, every running job a box inside it, and the waiting jobs beside them. Click a job
for its details. A simpler 2D view, CPUs and memory as a pair of bars per node, is at
`/2d`. It only reads from Slurm, and it is optional: sjp works the same without it.

To try it, run `python3 -m sjp.viz` (or `python3 -m sjp.viz --demo` for a simulated
cluster), and open `http://127.0.0.1:8650`. To keep it running, install it as a service
next to `sjpd`:

```sh
sudo cp systemd/sjp-viz.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now sjp-viz
```

It answers only on the machine itself, because the page shows users and job names. To
see it from your own computer, open an SSH tunnel and browse to `http://localhost:8650`:

```sh
ssh -L 8650:localhost:8650 <controller>
```

With the Ansible role, set `sjp_viz_enabled: true`. See
[Visualizer](https://github.com/KasperSkytte/slurmjobpacker/wiki/Visualizer) for more.

## Limitations

Currently, sjp places single-node batch jobs only; jobs asking for several nodes, GPUs, node
features or a reservation are left to your own rules. See
[Limitations](https://github.com/KasperSkytte/slurmjobpacker/wiki/Limitations) for the rest.

## Analysing your own cluster

`tools/` replays and measures your accounting history, without touching Slurm:

```sh
bzcat slurm_acct_db_backup.bz2 > dump.sql                    # a mysqldump of slurm_acct_db
python3 tools/dump2sqlite.py dump.sql accounting.sqlite --cluster <ClusterName>
python3 tools/normalize.py accounting.sqlite
cp tools/site.example.toml site.toml                          # describe your partitions
python3 tools/stranding.py accounting.sqlite                  # stranded CPU-hours
python3 tools/simulate.py --db accounting.sqlite --start 2026-03-09 --days 14
```

## Development

```sh
python3 tests/test_policy.py          # unit tests
tests/testcluster.sh start            # throwaway slurmctld as your user
export SLURM_CONF=/tmp/sjp-cluster/slurm.conf
python3 -m sjp.daemon -c /tmp/sjp-cluster/sjp.toml
tests/testcluster.sh stop
```

Releases are made by [release-please](https://github.com/googleapis/release-please) from
[Conventional Commits](https://www.conventionalcommits.org/): `fix:` for a patch, `feat:`
for a minor release.
