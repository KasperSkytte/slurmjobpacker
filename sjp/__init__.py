"""slurmjobpacker - state-aware partition placement for Slurm."""
__version__ = "1.17.1"  # x-release-please-version


def describe() -> str:
    """The version running: the release, or for a git checkout between releases
    what git says, e.g. "1.16.0-1-gbadc397" (one commit after 1.16.0)."""
    import os, subprocess
    top = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if os.path.exists(os.path.join(top, ".git")):
        try:
            out = subprocess.run(["git", "-C", top, "describe", "--tags", "--dirty"],
                                 capture_output=True, text=True, timeout=5).stdout.strip()
            if out:
                return out.removeprefix("v")
        except (OSError, subprocess.SubprocessError):
            pass
    return __version__
