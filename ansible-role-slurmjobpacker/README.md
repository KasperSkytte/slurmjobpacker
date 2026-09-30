# Ansible role: slurmjobpacker

An example role that installs slurmjobpacker (sjp) on your Slurm controllers:

- checks out sjp on every host in the play, so any controller's `job_submit.lua`
  can load `lua/sjp.lua`;
- installs `sjp.toml` and the `sjpd` service on one of them (`sjp_daemon_host`);
- sets or removes the disable file (`sjp_disabled`);
- after an upgrade, restarts `sjpd` and, if `sjp_reconfigure_slurm` is true, runs
  `scontrol reconfigure` so `slurmctld` loads the new `sjp.lua`.

It does not touch `slurm.conf` or `job_submit.lua`: add the `sjp.place()` call to your
`job_submit.lua` yourself (see the main README), then set `sjp_reconfigure_slurm: true`.

## Example playbook

Copy this folder to your roles path as `slurmjobpacker`, then:

```yaml
- hosts: slurm_controllers
  become: true
  roles:
    - role: slurmjobpacker
      vars:
        sjp_reconfigure_slurm: true
        sjp_config:
          general:
            mode: enforce
          limits:
            mode: global
```

All variables and their defaults are in [`defaults/main.yml`](defaults/main.yml).
`sjp_config` becomes `sjp.toml`; list every setting with
`python3 -m sjp.daemon --print-config`.
