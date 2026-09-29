#!/usr/bin/env python3
"""The README's illustration: the same jobs on the same two nodes, packed two ways.

Each node is drawn in resource space -- CPUs across, memory up -- as a box of its
size. Its jobs are rectangles of their CPUs x memory, stacked corner to corner
from the origin, so what is left of the node is exactly the rectangle between
the last corner and the node's top-right corner. Synthetic numbers, chosen to
show the effect; no data involved.

  usage: figure_packing.py docs/img/
"""
import sys

GB = 1
SLIM = dict(name="slim node", cpus=64, mem=256 * GB)
FAT = dict(name="fat node", cpus=64, mem=1024 * GB)
LIGHT = dict(kind="light", cpus=12, mem=24)       # 2 GB per CPU
HEAVY = dict(kind="heavy", cpus=4, mem=200)       # 50 GB per CPU
WAITING = dict(kind="waiting", cpus=16, mem=128)  # 8 GB per CPU

# Left: jobs landed wherever there was room when they arrived.
BLIND = {"slim node": [HEAVY], "fat node": [LIGHT] * 4 + [HEAVY] * 2}
# Right: each job on the node whose free memory per CPU matches its own.
MATCHED = {"slim node": [LIGHT] * 4, "fat node": [HEAVY] * 3}

THEMES = {
    "light": dict(surface="#fcfcfb", ink="#0b0b0b", ink2="#52514e", muted="#898781",
                  axis="#c3c2b7", free="#f0efec", light="#2a78d6", heavy="#eb6834"),
    "dark": dict(surface="#1a1a19", ink="#ffffff", ink2="#c3c2b7", muted="#898781",
                 axis="#383835", free="#2c2c2a", light="#3987e5", heavy="#d95926"),
}
FONT = "-apple-system, 'Segoe UI', Helvetica, Arial, sans-serif"

W = 920
PX_CPU = 2.6          # px per CPU
PX_GB = 0.24          # px per GB: a 1024 GB node is ~246 px tall
PLOT_H = 1024 * PX_GB


def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;")


def text(x, y, s, fill, size=12, weight=400, anchor="start"):
    return (f'<text x="{x:.1f}" y="{y:.1f}" fill="{fill}" font-size="{size}" '
            f'font-weight="{weight}" text-anchor="{anchor}">{esc(s)}</text>')


def node(c, x0, base, spec, jobs, verdict):
    """One node at (x0, base) = its origin, y growing upwards on screen."""
    out = []
    w, h = spec["cpus"] * PX_CPU, spec["mem"] * PX_GB
    used_c = sum(j["cpus"] for j in jobs)
    used_m = sum(j["mem"] for j in jobs)
    free_c, free_m = spec["cpus"] - used_c, spec["mem"] - used_m
    # free space: the rectangle from the last corner to the node's far corner
    fx, fy = x0 + used_c * PX_CPU, base - spec["mem"] * PX_GB
    out.append(f'<rect x="{fx:.1f}" y="{fy:.1f}" width="{free_c * PX_CPU:.1f}" '
               f'height="{free_m * PX_GB:.1f}" fill="{c["free"]}"/>')
    # jobs, corner to corner, with a 1px surface gap between them
    cx, cm = 0, 0
    for j in jobs:
        jx, jy = x0 + cx * PX_CPU, base - (cm + j["mem"]) * PX_GB
        out.append(f'<rect x="{jx + 0.5:.1f}" y="{jy + 0.5:.1f}" '
                   f'width="{max(j["cpus"] * PX_CPU - 1, 1):.1f}" '
                   f'height="{max(j["mem"] * PX_GB - 1, 1):.1f}" rx="1.5" fill="{c[j["kind"]]}"/>')
        cx, cm = cx + j["cpus"], cm + j["mem"]
    # the node's outline and axes
    out.append(f'<rect x="{x0:.1f}" y="{base - h:.1f}" width="{w:.1f}" height="{h:.1f}" '
               f'fill="none" stroke="{c["axis"]}" stroke-width="1"/>')
    out.append(text(x0 + w / 2, base + 16, f"{spec['cpus']} CPUs", c["muted"], 11,
                    anchor="middle"))
    out.append(f'<text x="{x0 - 8:.1f}" y="{base - h / 2:.1f}" fill="{c["muted"]}" '
               f'font-size="11" text-anchor="middle" '
               f'transform="rotate(-90 {x0 - 8:.1f} {base - h / 2:.1f})">{spec["mem"]} GB</text>')
    # the waiting job, outlined at the free corner: inside if it fits, over the edge if not
    wx, wy = fx, base - (used_m + WAITING["mem"]) * PX_GB
    fits = free_c >= WAITING["cpus"] and free_m >= WAITING["mem"]
    out.append(f'<rect x="{wx:.1f}" y="{wy:.1f}" width="{WAITING["cpus"] * PX_CPU:.1f}" '
               f'height="{WAITING["mem"] * PX_GB:.1f}" fill="none" stroke="{c["ink"]}" '
               f'stroke-width="1.5" stroke-dasharray="4 3"/>')
    out.append(text(x0 + w / 2, base + 36, f"{spec['name']}: {verdict(fits)}",
                    c["ink"] if fits else c["ink2"], 12, 600 if fits else 400, anchor="middle"))
    return out


def panel(c, x0, top, title, layout):
    out = [text(x0, top, title, c["ink"], 14, 600)]
    base = top + 26 + PLOT_H
    for i, spec in enumerate((SLIM, FAT)):
        out += node(c, x0 + 22 + i * (64 * PX_CPU + 70), base, spec, layout[spec["name"]],
                    lambda fits: "fits" if fits else "does not fit")
    return out, base + 44


def draw(theme):
    c = THEMES[theme]
    pad = 24
    out = [text(pad, pad + 16, "The same jobs on two nodes (CPUs across, memory up)",
                c["ink"], 16, 600)]
    ly = pad + 42                                              # legend
    items = (("light", "light job, 2 GB/CPU"), ("heavy", "heavy job, 50 GB/CPU"),
             ("waiting", "waiting job, 8 GB/CPU"))
    for i, (kind, label) in enumerate(items):
        x = pad + i * 200
        if kind == "waiting":
            out.append(f'<rect x="{x}" y="{ly - 9}" width="12" height="10" fill="none" '
                       f'stroke="{c["ink"]}" stroke-width="1.5" stroke-dasharray="3 2"/>')
        else:
            out.append(f'<rect x="{x}" y="{ly - 9}" width="12" height="10" rx="2" fill="{c[kind]}"/>')
        out.append(text(x + 18, ly, label, c["ink2"], 12))

    top = ly + 38
    left, b1 = panel(c, pad, top, "Placed wherever there is room", BLIND)
    right, b2 = panel(c, W / 2 + 12, top, "Placed where each job matches what is idle", MATCHED)
    out += left + right
    h = max(b1, b2) + pad
    body = "\n".join(out)
    desc = ("Two panels of two nodes each, drawn with CPUs on the x axis and memory on the y "
            "axis. Left: jobs placed wherever there is room; the fat node's CPUs fill with "
            "light jobs and the slim node's memory with a heavy one, so a waiting job fits "
            "on neither although 68 CPUs and 584 GB are free. Right: jobs placed by the free "
            "CPU and memory on each node, each job where its shape matches what is idle; "
            "the waiting job fits on either node.")
    return (f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{h:.0f}" '
            f'viewBox="0 0 {W} {h:.0f}" font-family="{FONT}" role="img">\n'
            f'<title>Packing jobs on shared nodes</title><desc>{desc}</desc>\n'
            f'<rect width="100%" height="100%" rx="8" fill="{c["surface"]}"/>\n{body}\n</svg>\n')


def main():
    out_dir = sys.argv[1] if len(sys.argv) > 1 else "docs/img"
    for theme in THEMES:
        path = f"{out_dir.rstrip('/')}/packing-{theme}.svg"
        open(path, "w").write(draw(theme))
        print("wrote", path)


if __name__ == "__main__":
    main()
