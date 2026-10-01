# Changelog

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
