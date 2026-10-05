# Changelog

## [1.12.1](https://github.com/KasperSkytte/slurmjobpacker/compare/v1.12.0...v1.12.1) (2026-10-05)


### Bug Fixes

* mark the space jobs use on each node red in the 3D view, and show job priority as an integer ([8bb987a](https://github.com/KasperSkytte/slurmjobpacker/commit/8bb987a9b430655297cb674ae3f438fb44378572))

## [1.12.0](https://github.com/KasperSkytte/slurmjobpacker/compare/v1.11.1...v1.12.0) (2026-10-05)


### Features

* recheck pending jobs' partitions every minute ([5bb0c91](https://github.com/KasperSkytte/slurmjobpacker/commit/5bb0c91fe96063a859e427c2aaefd83cf2c77533))
* show job run time in the 3D view, and free and used space per node in the 2D view ([36a6d2e](https://github.com/KasperSkytte/slurmjobpacker/commit/36a6d2eb7c57e9324a6f20885055e49be19e5763))


### Bug Fixes

* count pending jobs pinned to a node as taken, so bursts of similar jobs do not overbook it ([0f26149](https://github.com/KasperSkytte/slurmjobpacker/commit/0f26149441768b5ecb94bb1d105f0fd30067b4ff))

## [1.11.1](https://github.com/KasperSkytte/slurmjobpacker/compare/v1.11.0...v1.11.1) (2026-10-04)


### Bug Fixes

* colour jobs by how their memory per CPU fits their node by default ([1e03d28](https://github.com/KasperSkytte/slurmjobpacker/commit/1e03d282447f96d79a3ff05ea2f456fae46eab12))
* node names and resources above their cards in the 2D view ([9024324](https://github.com/KasperSkytte/slurmjobpacker/commit/90243246bd2fe143c1ea56ab3594ac209e2f40b3))

## [1.11.0](https://github.com/KasperSkytte/slurmjobpacker/compare/v1.10.0...v1.11.0) (2026-10-04)


### Features

* switch the visualizer's colours between memory per CPU and fit to the node ([209ec92](https://github.com/KasperSkytte/slurmjobpacker/commit/209ec92b5054d60779377cd8555263f8c749fb8e))

## [1.10.0](https://github.com/KasperSkytte/slurmjobpacker/compare/v1.9.0...v1.10.0) (2026-10-03)


### Features

* a live view of the cluster in 3D and 2D (python3 -m sjp.viz) ([edda413](https://github.com/KasperSkytte/slurmjobpacker/commit/edda41357995bf4339fe80d12136e662ff489cd6))

## [1.9.0](https://github.com/KasperSkytte/slurmjobpacker/compare/v1.8.0...v1.9.0) (2026-10-02)


### Features

* rename [limits] raise_above to min_idle_share ([4e97c57](https://github.com/KasperSkytte/slurmjobpacker/commit/4e97c57b50da9d51fa095343430d0b06bc0282f2))

## [1.8.0](https://github.com/KasperSkytte/slurmjobpacker/compare/v1.7.0...v1.8.0) (2026-10-02)


### Features

* count reservations against nodes' free space ([6ec5cb6](https://github.com/KasperSkytte/slurmjobpacker/commit/6ec5cb6abfaacb8f8851c2a0b1b2c377fdc368f4))

## [1.7.0](https://github.com/KasperSkytte/slurmjobpacker/compare/v1.6.0...v1.7.0) (2026-10-01)


### Features

* optional time-aware node choice ([pin] time_aware) ([c4bbee3](https://github.com/KasperSkytte/slurmjobpacker/commit/c4bbee32fe890251a4851510066c190407d72205))


### Bug Fixes

* leave multi-node jobs to the site's rules ([ac49170](https://github.com/KasperSkytte/slurmjobpacker/commit/ac49170e16212e788a5000d755f52e83df440693))
* mark flex moves once, as qos=&lt;own&gt;&gt;&lt;flex&gt; ([36fa690](https://github.com/KasperSkytte/slurmjobpacker/commit/36fa6909b116b4ba80735173c63a252d3ee32771))

## [1.6.0](https://github.com/KasperSkytte/slurmjobpacker/compare/v1.5.0...v1.6.0) (2026-10-01)


### Features

* AdminComment tells what sjp did with the job ([37c9a4d](https://github.com/KasperSkytte/slurmjobpacker/commit/37c9a4d5d889c71d95462c43f60d5ac370ef1c86))

## [1.5.0](https://github.com/KasperSkytte/slurmjobpacker/compare/v1.4.0...v1.5.0) (2026-10-01)


### Features

* flex mode skips jobs of users who may not use the flex QOS, and logs them ([1b79689](https://github.com/KasperSkytte/slurmjobpacker/commit/1b79689271255a3ead63ebcd416a7297ee3d25ab))
* flex QOS mode, verbose placement log, and QOS caps changed by hand are followed ([13e0d91](https://github.com/KasperSkytte/slurmjobpacker/commit/13e0d91d7995efdb9f0c2cf594d45e798079083f))

## [1.4.0](https://github.com/KasperSkytte/slurmjobpacker/compare/v1.3.0...v1.4.0) (2026-09-30)


### Features

* add an example Ansible role that installs sjp ([ba64a03](https://github.com/KasperSkytte/slurmjobpacker/commit/ba64a03b55f4b4da29bb990c24ddd2627e99e2b6))
* the Ansible role can install an example job_submit.lua and checks slurm.conf ([469c413](https://github.com/KasperSkytte/slurmjobpacker/commit/469c413f0f9decdf3ca2876074d764b64d1f5d2d))


### Bug Fixes

* leave jobs with --constraint to the site's rules ([5057f40](https://github.com/KasperSkytte/slurmjobpacker/commit/5057f405823061796571bf1cbde3394c8e84c8c3))

## [1.3.0](https://github.com/KasperSkytte/slurmjobpacker/compare/v1.2.0...v1.3.0) (2026-09-30)


### Features

* sjp.place() reports whether it placed the job, leaving fallbacks to the site ([eedf345](https://github.com/KasperSkytte/slurmjobpacker/commit/eedf3454247dbdde5c5d76f081270d765cbe023c))

## [1.2.0](https://github.com/KasperSkytte/slurmjobpacker/compare/v1.1.0...v1.2.0) (2026-09-29)


### Features

* load sjp from the cluster's own job_submit.lua ([db29f9b](https://github.com/KasperSkytte/slurmjobpacker/commit/db29f9bcb0277a32fb7934e6629d0c421584bf45))

## [1.1.0](https://github.com/KasperSkytte/slurmjobpacker/compare/v1.0.0...v1.1.0) (2026-09-28)


### Features

* fit the demand mix from sacct with python3 -m sjp.fit ([ccaf94d](https://github.com/KasperSkytte/slurmjobpacker/commit/ccaf94dd834d2de0d868349e83f4249a340cbd5d))
* make the plugin and defaults site-neutral ([6b6bd67](https://github.com/KasperSkytte/slurmjobpacker/commit/6b6bd67d7d5aa1cd05d0c74c84ab05e0fbd65a06))
* pin jobs that can start now to the node that best fits them ([5a7cb11](https://github.com/KasperSkytte/slurmjobpacker/commit/5a7cb11b6f421eded8a303757f8f251ddc431519))
* raise the QOS CPU caps only in short pulses ([5a7cb11](https://github.com/KasperSkytte/slurmjobpacker/commit/5a7cb11b6f421eded8a303757f8f251ddc431519))
* readable per-job text log ([5a7cb11](https://github.com/KasperSkytte/slurmjobpacker/commit/5a7cb11b6f421eded8a303757f8f251ddc431519))
* the QOS cap pulse is off by default ([6b6bd67](https://github.com/KasperSkytte/slurmjobpacker/commit/6b6bd67d7d5aa1cd05d0c74c84ab05e0fbd65a06))


### Bug Fixes

* count powered-down nodes as available ([5a7cb11](https://github.com/KasperSkytte/slurmjobpacker/commit/5a7cb11b6f421eded8a303757f8f251ddc431519))
* keep all output under --state-dir unless told otherwise ([b5bf659](https://github.com/KasperSkytte/slurmjobpacker/commit/b5bf659105fa2b1eee94541b9130024488ae2ebc))
* never rewrite a job's memory request ([6b6bd67](https://github.com/KasperSkytte/slurmjobpacker/commit/6b6bd67d7d5aa1cd05d0c74c84ab05e0fbd65a06))
* pin jobs waiting on a dependency like any other job ([5a7cb11](https://github.com/KasperSkytte/slurmjobpacker/commit/5a7cb11b6f421eded8a303757f8f251ddc431519))
* recognise jobs held by the per-account CPU cap ([5a7cb11](https://github.com/KasperSkytte/slurmjobpacker/commit/5a7cb11b6f421eded8a303757f8f251ddc431519))
* refuse non-read-only Slurm commands outside enforce mode ([89f314c](https://github.com/KasperSkytte/slurmjobpacker/commit/89f314c9c8d48889f8f6a68970d10ed8f2e76225))
* **systemd:** run sjpd from the install directory ([a8f01af](https://github.com/KasperSkytte/slurmjobpacker/commit/a8f01af4945a98bd48528efa640d167cfbdd3985))
* **tests:** use built-in defaults instead of a path assumed not to exist ([97edfe9](https://github.com/KasperSkytte/slurmjobpacker/commit/97edfe958d5b25249d2df33c87cce9eacb5b6e05))
* warn that limits.mode perjob is not implemented ([f04943f](https://github.com/KasperSkytte/slurmjobpacker/commit/f04943fb73e4354316e97ccdc30d4a27c797284d))

## 1.0.0 (2026-09-24)

First release.

### Features

* sjpd control loop with `observe`, `advise` and `enforce` modes, and a `job_submit.lua`
  plugin that falls back to the site's static rule on any failure
* dry run (`--dry-run`): logs every intended action with its command and reason, and for
  each job where Slurm placed it versus where sjp would have; every state-changing command
  goes through one guard that is off outside `enforce`
* `sjp.report` reads the decision log as a timeline and summary
* partitions discovered automatically; `interactive` partitions and GPU nodes excluded by
  default, each overridable
* global elastic QOS limits, a trace-driven simulator and workload analysis tools
