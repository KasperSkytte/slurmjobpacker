"""Install sjpd and sjp-viz as services, from this checkout:

    sudo python3 -m sjp.install               # both; sjpd in observe mode
    sudo python3 -m sjp.install --no-viz      # sjpd only
    sudo python3 -m sjp.install --no-packer   # the visualizer only
    sudo python3 -m sjp.install --uninstall

It writes /etc/sjp/sjp.toml with the defaults if there is none (an existing
one is kept), the systemd units and the logrotate rule for sjpd's logs, then
starts the services, or restarts them if they run. Run it again after an
upgrade: the services restart on this checkout's code, and the choices made
before (--no-viz, --no-packer, --viz-bind, --viz-port) are kept unless given
again (--viz, --packer add one back). The configuration, logs and state are
never removed.
"""
from __future__ import annotations
import argparse, grp, os, pwd, re, shlex, subprocess, sys

from . import config

TOP = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
UNITS = "etc/systemd/system"
CONF = "etc/sjp/sjp.toml"
ROTATE = "etc/logrotate.d/sjp"


def run(argv) -> bool:
    print("  $ " + shlex.join(argv))
    return subprocess.run(argv).returncode == 0


class Installer:
    """root: where / is (a staging directory for tests or packages); run: how
    commands are run; None runs none."""

    def __init__(self, root="/", run_=run, python=sys.executable):
        self.root, self.run, self.python = root, run_, python

    def path(self, p):
        return os.path.join(self.root, p.lstrip("/"))

    def write(self, p, text, mode=0o644):
        full = self.path(p)
        old = open(full).read() if os.path.exists(full) else None
        if old == text:
            return False
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "w") as f:
            f.write(text)
        os.chmod(full, mode)
        print(f"  {'updated' if old is not None else 'wrote'} /{p.lstrip('/')}")
        return True

    def remove(self, p):
        if os.path.exists(self.path(p)):
            os.remove(self.path(p))
            print(f"  removed /{p}")

    def previous(self) -> dict:
        """The choices of an earlier install, from the units it wrote; {}
        if there was none."""
        viz = self.path(f"{UNITS}/sjp-viz.service")
        packer = os.path.exists(self.path(f"{UNITS}/sjpd.service"))
        if not packer and not os.path.exists(viz):
            return {}
        out = dict(packer=packer, viz=os.path.exists(viz))
        if out["viz"]:
            line = next((l for l in open(viz) if l.startswith("ExecStart=")), "")
            args = shlex.split(line.partition("=")[2])
            for opt in ("bind", "port"):
                if f"--{opt}" in args:
                    out[f"viz_{opt}"] = args[args.index(f"--{opt}") + 1]
        return out

    def unit(self, name, user, group, extra_paths=(), viz_args=()):
        """systemd/<name> from this checkout, for where it is and who runs it."""
        text = open(os.path.join(TOP, "systemd", name)).read()
        text = text.replace("/opt/slurmjobpacker", TOP).replace("/usr/bin/python3", self.python)
        text = re.sub(r"^User=.*$", f"User={user}", text, flags=re.M)
        text = re.sub(r"^Group=.*$", f"Group={group}", text, flags=re.M)
        if extra_paths:
            text = re.sub(r"^(ReadWritePaths=.*)$", lambda m: m.group(1) + " "
                          + " ".join(extra_paths), text, flags=re.M)
        def exec_start(m):
            args = shlex.split(m.group(1))
            for opt, val in viz_args:    # (option, value), or (flag, None)
                if val is None:
                    args += [opt] if opt not in args else []
                elif opt in args:
                    args[args.index(opt) + 1] = val
                else:
                    args += [opt, val]
            return "ExecStart=" + shlex.join(args)
        return re.sub(r"^ExecStart=(.*)$", exec_start, text, flags=re.M)

    def rotate(self, cfg, user, group) -> str:
        g = cfg["general"]
        logs = " ".join(p for p in (g["log_file"], g.get("text_log")) if p)
        text = open(os.path.join(TOP, ROTATE)).read()
        text = text.replace("/var/log/sjp/decisions.jsonl /var/log/sjp/sjp.log", logs)
        return text.replace("su slurm slurm", f"su {user} {group}")

    def service(self, name):
        """Enable a service and (re)start it, so it runs this checkout's code."""
        if self.run is not None:
            self.run(["systemctl", "enable", name])
            self.run(["systemctl", "restart", name])

    def stop(self, name):
        if self.run is not None and os.path.exists(self.path(f"{UNITS}/{name}")):
            self.run(["systemctl", "disable", "--now", name])

    def install(self, packer=None, viz=None, user="slurm", viz_bind=None, viz_port=None):
        """packer, viz: None keeps what an earlier install chose; both, at first."""
        before = self.previous()
        packer = before.get("packer", True) if packer is None else packer
        viz = before.get("viz", True) if viz is None else viz
        if not packer and not viz:
            sys.exit("sjp.install: --no-packer and --no-viz leave nothing to install")
        viz_bind = viz_bind or before.get("viz_bind")
        viz_port = viz_port or before.get("viz_port")
        try:
            pw = pwd.getpwnam(user)
            group = grp.getgrgid(pw.pw_gid).gr_name
        except KeyError:
            sys.exit(f"sjp.install: no user {user!r}; give the one slurmctld runs as with --user")
        print(f"Installing slurmjobpacker from {TOP}, run as {user}")

        conf = self.path(CONF)
        if os.path.exists(conf):
            print(f"  kept /{CONF}")
        else:
            self.write(CONF, config.dump_defaults() + "\n")
        cfg = config.load(conf)

        services = []
        if not packer:
            self.stop("sjpd.service")
            self.remove(f"{UNITS}/sjpd.service")
            self.remove(ROTATE)
        else:
            g = cfg["general"]
            dirs = sorted({os.path.dirname(p) for p in (g["log_file"], g.get("text_log")) if p}
                          - {"/var/log/sjp"})
            self.write(f"{UNITS}/sjpd.service", self.unit("sjpd.service", user, group, dirs))
            services.append("sjpd.service")
            if os.path.isdir(self.path("etc/logrotate.d")):
                self.write(ROTATE, self.rotate(cfg, user, group))
            else:
                print("  no /etc/logrotate.d: sjpd's logs are not rotated")
        if viz:
            args = [(o, v) for o, v in (("--bind", viz_bind), ("--port", viz_port)) if v]
            if not packer:
                args.append(("--no-packer", None))
            self.write(f"{UNITS}/sjp-viz.service",
                       self.unit("sjp-viz.service", user, group, viz_args=args))
            services.append("sjp-viz.service")
        else:
            self.stop("sjp-viz.service")
            self.remove(f"{UNITS}/sjp-viz.service")
        if self.run is not None:
            self.run(["systemctl", "daemon-reload"])
        for name in services:
            self.service(name)
        if packer:
            self.next_steps(cfg)

    def uninstall(self):
        print("Removing the slurmjobpacker services (configuration, logs and state stay)")
        for name in ("sjpd.service", "sjp-viz.service"):
            self.stop(name)
            self.remove(f"{UNITS}/{name}")
        self.remove(ROTATE)
        if self.run is not None:
            self.run(["systemctl", "daemon-reload"])

    def next_steps(self, cfg):
        slurm_conf = os.environ.get("SLURM_CONF", "/etc/slurm/slurm.conf")
        try:
            lua = re.search(r"(?im)^\s*JobSubmitPlugins\s*=.*lua", open(self.path(slurm_conf)).read())
        except OSError:
            lua = None
        print(f"\nsjpd runs in {cfg['general']['mode']} mode (/{CONF}).")
        if not lua:
            print(f"Set JobSubmitPlugins=lua in {slurm_conf} for sjp to place jobs.")
        print(f'Load sjp in job_submit.lua with dofile("{TOP}/lua/sjp.lua") and call '
              "sjp.place() (see the README),\nthen run scontrol reconfigure, again after "
              "every upgrade.")


def main(argv=None):
    ap = argparse.ArgumentParser(prog="python3 -m sjp.install",
                                 description=__doc__.split("\n")[0].rstrip(":") + ".")
    ap.add_argument("--packer", action=argparse.BooleanOptionalAction,
                    help="sjpd, the packer (default: yes, or as installed before); "
                         "--no-packer removes one installed before")
    ap.add_argument("--viz", action=argparse.BooleanOptionalAction,
                    help="sjp-viz, the visualizer (default: yes, or as installed before); "
                         "--no-viz removes one installed before")
    ap.add_argument("--viz-bind", help="address sjp-viz answers on (default 127.0.0.1)")
    ap.add_argument("--viz-port", help="its port (default 8650)")
    ap.add_argument("--user", default="slurm", help="the user the services run as (slurm)")
    ap.add_argument("--uninstall", action="store_true",
                    help="stop and remove the services; configuration, logs and state stay")
    ap.add_argument("--root", default="/", help=argparse.SUPPRESS)
    a = ap.parse_args(argv)
    if a.root == "/" and os.geteuid() != 0:
        sys.exit("sjp.install: run it as root (sudo python3 -m sjp.install)")
    if a.root == "/" and not a.uninstall and TOP.startswith(("/home/", "/root/", "/run/user/")):
        sys.exit(f"sjp.install: {TOP} is in a home directory, which the services (and "
                 "slurmctld) may not read. Clone it to /opt/slurmjobpacker and install from there.")
    inst = Installer(a.root, run if a.root == "/" else None)
    if a.uninstall:
        inst.uninstall()
    else:
        inst.install(a.packer, a.viz, a.user, a.viz_bind, a.viz_port)
    return 0


if __name__ == "__main__":
    sys.exit(main())
