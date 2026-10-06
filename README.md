# slurmjobpacker

SLURM plugin for automatic partition and compute node selection that also packs jobs optimally on clusters where compute nodes are shared by multiple users (i.e. no forced exclusive node access). It does so by matching the job shape in CPU+Memory+time space to what is currently available on the nodes, reducing waste from starvation of either CPUs or memory, and also preventing fragmentation by "packing jobs" tightly, leaving the most room possible for future jobs. Furthermore, it can dynamically adjust per-user or -account CPU limits temporarily depending on the overall partition or cluster load, or assign a different QOS, to better balance usage over time (fx clusters may have max CPU limits in place merely to prevent a few users from blocking the whole cluster, but during periods of low(er) activity there's no reason CPUs are idle when somebody needs them).

`slurmjobpacker` is a single sourced Lua function to use among your existing job submission logics defined in the [job submit Lua script](https://slurm.schedmd.com/job_submit_plugins.html), if any, so it easily integrates in any slurm configuration. It does not interfere with the normal scheduling by the slurm controller, priorities, fair-share, etc, it simply sets the partition and nodelist automatically at job submission, SLURM takes care of the rest.

An optional web-based visualizer shows the cluster live, every node and job in CPU, memory and time space, in 3D or 2D (see [Live cluster view in 2D and 3D](#live-cluster-view-in-2d-and-3d)):

![3D view of a simulated cluster: three slim and three fat nodes, each a box of CPUs × memory × time, with running jobs inside and waiting jobs beside them](docs/img/sjp-demo-3d.webp)

![2D view of the same: a CPU bar and a memory bar per node, with free and used space](docs/img/sjp-demo-2d.webp)

## The problem with sharing nodes

A computing cluster configured so that nodes can be shared by multiple jobs at once is often preferred over exclusive node access for smaller, local clusters, because it can utilize computing resources more efficiently. To take proper advantage of this, users must both request resources that match their jobs' requirements as precisely as possible, but also avoid getting in the way of future jobs. Choosing the most appropriate hardware partition is challenging for many users, especially on very heterogeneous hardware, so naturally many users get it wrong, which results in wasted computing resources. **The goal of `slurmjobpacker` is BOTH to automate partition and node selection AND to pack the cluster as tightly as possible**, to optimize resource utilization mainly on clusters where many individual jobs share the same compute nodes.

![packing](docs/img/packing-dark.svg)

To pack a shared node cluster efficiently, the combined CPU and memory shape of the jobs on each node is very important. If the jobs on a node use significantly more memory per CPU on average than the node provides, the memory runs out first and the remaining CPUs sit idle. For example, a node with 256 allocatable threads and 1 TB of memory has at most 4 GB per thread, so the combined requirements of all jobs running on it should preferably match that ratio. Similarly, placing jobs that need little memory per CPU on nodes with extra memory, which are often much more expensive (especially in 2026!), can prevent memory-demanding jobs from running there. Lastly, sometimes a mix of jobs with both low and high memory-per-CPU requirements fit perfectly together on the same node, so a [job submit Lua script](https://slurm.schedmd.com/job_submit_plugins.html) that assigns partitions based only on a fixed memory-per-CPU threshold (e.g. a fat node above x GB per CPU, otherwise a slim node) is not ideal either as it doesn't consider running jobs at all. 

`slurmjobpacker` matches the memory and CPU "shape" of each job against the free space on all nodes at once, and chooses the placement that leaves the most usable room for the jobs that typically follow. This packs the cluster far better than leaving placement to users and Slurm alone.

## How it works

See [How it works](https://github.com/KasperSkytte/slurmjobpacker/wiki/How-it-works) in the
wiki.

## Installation
Follow the below steps manually, or use the provided ansible role under [ansible-role-slurmjobpacker](./ansible-role-slurmjobpacker/).

Needs Python 3.11+ (standard library only). Tested on Slurm 24.11 and 26.05.

<!-- x-release-please-start-version -->
```sh
sudo git clone --branch v1.13.0 https://github.com/kasperskytte/slurmjobpacker /opt/slurmjobpacker
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

| `[general] mode` | chooses partitions | pins nodes, re-places pending jobs | helps jobs held by CPU caps |
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
and a reason (such as `"no room"`) when it left the job for your own rules. On a busy
cluster, set `[policy] always_place` (and `[pin] wait_for_room`) to have sjp place every
job anyway and move it as room opens. What sjp did
is noted in the job's `AdminComment` and in the `slurmctld` log. See
[Using sjp in job_submit.lua](https://github.com/KasperSkytte/slurmjobpacker/wiki/Using-sjp-in-job-submit.lua) and
[Logs and AdminComment](https://github.com/KasperSkytte/slurmjobpacker/wiki/Logs-and-AdminComment).

To switch it off, `sudo touch /etc/sjp/disable`: from the next submission
`sjp.place()` returns `false`, so only your own rules apply.

## Live cluster view in 2D and 3D

`sjp-viz` shows the cluster live in a web browser to easily see how jobs are packed together.

To try it, run `python3 -m sjp.viz` (or `python3 -m sjp.viz --demo` for a simulated
cluster), and open `http://127.0.0.1:8650`. To keep it running, install it as a service
next to `sjpd`:

```sh
sudo cp systemd/sjp-viz.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now sjp-viz
```

With the Ansible role, set `sjp_viz_enabled: true`. See
[Visualizer](https://github.com/KasperSkytte/slurmjobpacker/wiki/Visualizer) for more.

## Limitations

Currently, sjp places single-node batch jobs only; jobs asking for several nodes, GPUs, node
features or a reservation are left to your own rules. See
[Limitations](https://github.com/KasperSkytte/slurmjobpacker/wiki/Limitations) for the rest,
and [Issues](https://github.com/KasperSkytte/slurmjobpacker/issues) for known bugs and
feature requests, or to report a new one.

## Development

```sh
python3 tests/test_policy.py          # unit tests
python3 tests/test_lua.py             # the Lua plugin, against sjpd's decisions (needs lua5.4)
tests/testcluster.sh start            # throwaway slurmctld as your user
export SLURM_CONF=/tmp/sjp-cluster/slurm.conf
python3 -m sjp.daemon -c /tmp/sjp-cluster/sjp.toml
tests/testcluster.sh stop
```

Releases are made by [release-please](https://github.com/googleapis/release-please) from
[Conventional Commits](https://www.conventionalcommits.org/): `fix:` for a patch, `feat:`
for a minor release.
