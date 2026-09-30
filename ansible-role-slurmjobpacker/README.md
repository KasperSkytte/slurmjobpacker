# Ansible role: slurmjobpacker

An example role that installs slurmjobpacker (sjp) on your Slurm controllers:

- checks out sjp on every host in the play, so any controller's `job_submit.lua`
  can load `lua/sjp.lua`;
- installs `sjp.toml` and the `sjpd` service on one of them (`sjp_daemon_host`);
- sets or removes the disable file (`sjp_disabled`);
- checks that `slurm.conf` has `JobSubmitPlugins=lua`: a warning in `observe` mode, an
  error in `advise` and `enforce`;
- optionally (`sjp_job_submit_install: true`) installs an example `job_submit.lua` next
  to `slurm.conf`, from [`templates/job_submit.lua.j2`](templates/job_submit.lua.j2):
  interactive jobs to an interactive partition, GPU jobs to a GPU partition, every other
  batch job to sjp, and a fixed memory-per-CPU fallback rule when sjp does not place it;
- after an upgrade, restarts `sjpd` and, if `sjp_reconfigure_slurm` is true, runs
  `scontrol reconfigure` so `slurmctld` loads the new `sjp.lua`.

It never changes `slurm.conf`. If you keep your own `job_submit.lua`, add the
`sjp.place()` call to it yourself (see the main README) and set
`sjp_reconfigure_slurm: true`.

## Example playbook

Copy this folder to your roles path as `slurmjobpacker`, then:

```yaml
- hosts: slurm_controllers
  become: true
  roles:
    - role: slurmjobpacker
      vars:
        sjp_job_submit_install: true
        sjp_gpu_partition: gpu
        sjp_fallback_slim: slim1,slim2
        sjp_fallback_fat: fat1,fat2
        sjp_config:
          general:
            mode: enforce
          limits:
            mode: global
```

All variables and their defaults are in [`defaults/main.yml`](defaults/main.yml).
`sjp_config` becomes `sjp.toml`; list every setting with
`python3 -m sjp.daemon --print-config`.
