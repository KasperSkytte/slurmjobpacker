#!/usr/bin/env python3
"""The README's illustration: a slim and a fat node, badly and well packed.

Each node is drawn in resource space -- CPUs across, memory up -- as a box of its
size. Its jobs are rectangles of their CPUs x memory, stacked corner to corner
from the origin, in alphabetical order. Hatched areas are wasted: CPUs
no job can use for want of memory, or memory no job can use for want of CPUs.
Plain grey is free in both, and so still usable by new jobs.

Synthetic but realistic numbers: the same eleven jobs of varying sizes and
memory per CPU in both halves. Badly packed, a big high-memory job sits on the
slim node and low-memory jobs fill the fat node, and one job has nowhere to go;
well packed, every job runs and the fat node keeps room for new ones.

  usage: figure_packing.py docs/img/
"""
import sys

SLIM = dict(name="slim node", cpus=64, mem=256)
FAT = dict(name="fat node", cpus=64, mem=1024)
# (CPUs, GB)
JOBS = dict(A=(8, 160), B=(16, 32), C=(24, 48), D=(12, 36), E=(8, 40), F=(4, 80),
            G=(6, 30), H=(10, 20), I=(4, 120), J=(16, 128), K=(2, 60))
HIGH = 6    # GB per CPU from which a job counts as high-memory (its colour)
BAD = {"slim node": "AEGH", "fat node": "BCDFIK"}      # J cannot start anywhere
GOOD = {"slim node": "BCDHK", "fat node": "AEFGIJ"}

THEMES = {
    "light": dict(surface="#fcfcfb", ink="#0b0b0b", ink2="#52514e", muted="#898781",
                  axis="#c3c2b7", rule="#e1e0d9", waste="#ecebe6", hatch="#898781",
                  free="#f0efec", low="#2a78d6", high="#eb6834"),
    "dark": dict(surface="#1a1a19", ink="#ffffff", ink2="#c3c2b7", muted="#898781",
                 axis="#383835", rule="#2c2c2a", waste="#2c2c2a", hatch="#6f6e69",
                 free="#252524", low="#3987e5", high="#d95926"),
}
FONT = "-apple-system, 'Segoe UI', Helvetica, Arial, sans-serif"

PX_CPU = 3.2          # px per CPU: a 64-CPU node is 205 px wide
PX_GB = 0.28          # px per GB: a 1024 GB node is 287 px tall
NODE_GAP = 72        # room for the next node's memory axis
AXIS_L = 44          # room left of a node for its memory axis


def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;")


def text(x, y, s, fill, size=12, weight=400, anchor="start"):
    return (f'<text x="{x:.1f}" y="{y:.1f}" fill="{fill}" font-size="{size}" '
            f'font-weight="{weight}" text-anchor="{anchor}">{esc(s)}</text>')


def label_in(c, x, y, w, h, s, size=11):
    """Words wrapped to fit a w x h area, centred on a small plate so they stay
    readable over hatching; nothing if they do not fit."""
    per_line = max(1, int((w - 12) / (size * 0.62)))
    lines, cur = [], ""
    for word in s.split():
        if cur and len(cur) + 1 + len(word) > per_line:
            lines.append(cur)
            cur = word
        else:
            cur = f"{cur} {word}".strip()
    lines.append(cur)
    lh = size * 1.3
    if len(lines) * lh > h - 2 or max(len(l) for l in lines) > per_line:
        return []
    tw = max(len(l) for l in lines) * size * 0.58 + 10
    th = len(lines) * lh + 4
    cx, cy = x + w / 2, y + h / 2
    out = [f'<rect x="{cx - tw / 2:.1f}" y="{cy - th / 2:.1f}" width="{tw:.1f}" '
           f'height="{th:.1f}" rx="3" fill="{c["surface"]}" fill-opacity="0.92"/>']
    y0 = cy - (len(lines) - 1) * lh / 2 + size * 0.35
    out += [text(cx, y0 + i * lh, l, c["ink2"], size, anchor="middle")
            for i, l in enumerate(lines)]
    return out


def node(c, x0, base, spec, keys, waste_note):
    out = []
    w, h = spec["cpus"] * PX_CPU, spec["mem"] * PX_GB
    jobs = [(k,) + JOBS[k] for k in sorted(keys)]              # A, B, C ... left to right
    used_c, used_m = sum(j[1] for j in jobs), sum(j[2] for j in jobs)
    idle_c, idle_m = spec["cpus"] - used_c, spec["mem"] - used_m
    cx, cy = x0 + used_c * PX_CPU, base - used_m * PX_GB     # where the jobs end
    top = base - h
    if waste_note == "cpu":            # memory is gone: the idle CPUs are stranded
        out.append(f'<rect x="{cx:.1f}" y="{top:.1f}" width="{idle_c * PX_CPU:.1f}" '
                   f'height="{h:.1f}" fill="url(#waste)"/>')
        out += label_in(c, cx, top, idle_c * PX_CPU, h,
                        "Unavailable CPUs due to memory starvation")
    elif waste_note == "mem":          # CPUs are gone: the idle memory is stranded
        out.append(f'<rect x="{x0:.1f}" y="{top:.1f}" width="{w:.1f}" '
                   f'height="{idle_m * PX_GB:.1f}" fill="url(#waste)"/>')
        out += label_in(c, x0, top, w, idle_m * PX_GB,
                        "Wasted memory which memory-heavy jobs could have needed")
    elif waste_note == "some":         # a little memory left over, as on a real node
        out.append(f'<rect x="{x0:.1f}" y="{top:.1f}" width="{w:.1f}" '
                   f'height="{idle_m * PX_GB:.1f}" fill="url(#waste)"/>')
        out += label_in(c, x0, top, w, idle_m * PX_GB, "Minimal memory waste")
    elif waste_note == "free":         # both left, together: room for new jobs
        out.append(f'<rect x="{cx:.1f}" y="{top:.1f}" width="{idle_c * PX_CPU:.1f}" '
                   f'height="{idle_m * PX_GB:.1f}" fill="{c["free"]}"/>')
        out += label_in(c, cx, top, idle_c * PX_CPU, idle_m * PX_GB, "Room for more jobs")
    # jobs, corner to corner, with a 1px surface gap between them
    # labelled with its ID just above its top edge, which is always clear: the
    # next job starts at this one's top-right corner
    x, m = x0, 0
    for key, jc, jm in jobs:
        y = base - (m + jm) * PX_GB
        kind = "high" if jm / jc >= HIGH else "low"
        out.append(f'<rect x="{x + 0.5:.1f}" y="{y + 0.5:.1f}" width="{jc * PX_CPU - 1:.1f}" '
                   f'height="{max(jm * PX_GB - 1, 1.5):.1f}" rx="1.5" fill="{c[kind]}"/>')
        out.append(text(x + jc * PX_CPU / 2, y - 3, key, c["ink"], 10, 600, anchor="middle"))
        x, m = x + jc * PX_CPU, m + jm
    out.append(f'<rect x="{x0:.1f}" y="{top:.1f}" width="{w:.1f}" height="{h:.1f}" '
               f'fill="none" stroke="{c["axis"]}" stroke-width="1"/>')
    # axes: CPUs across, memory up, each with its two end ticks
    out.append(text(x0, base + 14, "0", c["muted"], 10, anchor="middle"))
    out.append(text(x0 + w, base + 14, str(spec["cpus"]), c["muted"], 10, anchor="middle"))
    out.append(text(x0 + w / 2, base + 14, "CPUs", c["muted"], 11, anchor="middle"))
    out.append(text(x0 - 4, base, "0", c["muted"], 10, anchor="end"))
    out.append(text(x0 - 4, top + 8, str(spec["mem"]), c["muted"], 10, anchor="end"))
    ty, tx = base - h / 2, x0 - 32          # clear of the tick labels
    out.append(f'<text x="{tx:.1f}" y="{ty:.1f}" fill="{c["muted"]}" font-size="11" '
               f'text-anchor="middle" transform="rotate(-90 {tx:.1f} {ty:.1f})">Memory (GB)</text>')
    out.append(text(x0 + w / 2, base + 32, spec["name"], c["ink2"], 12, 600, anchor="middle"))
    return out


def pair(c, x0, top, title, note, layout, notes, waiting=()):
    out = [text(x0, top, title, c["ink"], 14, 600), text(x0, top + 18, note, c["ink2"], 12)]
    base = top + 40 + FAT["mem"] * PX_GB
    if waiting:         # jobs that cannot start: shown in the space above the slim node
        wx, wy = x0 + AXIS_L, base - FAT["mem"] * PX_GB
        out.append(text(wx, wy + 10, "Waiting in the queue:", c["ink2"], 12))
        y = wy + 22
        for key in waiting:
            jc, jm = JOBS[key]
            kind = "high" if jm / jc >= HIGH else "low"
            out.append(f'<rect x="{wx:.1f}" y="{y:.1f}" width="{jc * PX_CPU:.1f}" '
                       f'height="{jm * PX_GB:.1f}" rx="1.5" fill="{c[kind]}"/>')
            out.append(text(wx + jc * PX_CPU + 8, y + jm * PX_GB / 2 + 4,
                            f"{key}: {jc} CPUs, {jm} GB", c["ink"], 11, 600))
            y += jm * PX_GB + 10
    for i, spec in enumerate((SLIM, FAT)):
        out += node(c, x0 + AXIS_L + i * (SLIM["cpus"] * PX_CPU + NODE_GAP), base, spec,
                    layout[spec["name"]], notes[i])
    return out, base + 44


def draw(theme):
    c = THEMES[theme]
    pad = 24
    defs = (f'<defs><pattern id="waste" width="6" height="6" patternUnits="userSpaceOnUse" '
            f'patternTransform="rotate(45)"><rect width="6" height="6" fill="{c["waste"]}"/>'
            f'<line x1="0" y1="0" x2="0" y2="6" stroke="{c["hatch"]}" stroke-width="1.2"/>'
            f'</pattern></defs>')
    out = [text(pad, pad + 16, "Badly and well packed nodes, with the same jobs",
                c["ink"], 16, 600)]
    ly = pad + 42                                              # legend, two rows
    rows = ((("low", c["low"], "Job with a low memory/CPU requirement"),
             ("high", c["high"], "Job with a high memory/CPU requirement")),
            (("waste", "url(#waste)", "Wasted"), ("free", c["free"], "Free")))
    legend_w = 0
    for r, items in enumerate(rows):
        x, y = pad, ly + r * 20
        for kind, fill, label in items:
            out.append(f'<rect x="{x}" y="{y - 9}" width="12" height="10" rx="2" fill="{fill}"/>')
            out.append(text(x + 18, y, label, c["ink2"], 12))
            x += 18 + len(label) * 6.6 + 28
        legend_w = max(legend_w, x - 28 - pad)

    top = ly + 60
    # the two halves stacked, divided by a line
    pair_w = AXIS_L + 2 * SLIM["cpus"] * PX_CPU + NODE_GAP
    width = max(pad + pair_w + 8 + pad, legend_w + 2 * pad)
    upper, b1 = pair(c, pad, top, "Badly packed",
                     "Job J cannot run: neither node has room for it.", BAD,
                     ("cpu", "mem"),
                     waiting=sorted(set(JOBS) - set("".join(BAD.values()))))
    rule = b1 + 6
    lower, b2 = pair(c, pad, rule + 34, "Well packed",
                     "Every job runs, J included, with room for more jobs.", GOOD, ("some", "free"))
    out += upper + lower
    out.append(f'<line x1="{pad}" y1="{rule:.1f}" x2="{width - pad:.1f}" y2="{rule:.1f}" '
               f'stroke="{c["rule"]}" stroke-width="1"/>')
    h = b2 + pad
    body = "\n".join(out)
    desc = ("A slim node (64 CPUs, 256 GB) and a fat node (64 CPUs, 1024 GB), drawn with "
            "CPUs across and memory up, packed badly and well with the same eleven jobs. "
            "Badly packed: a big high-memory job on the slim node uses up its memory, "
            "leaving 32 CPUs unavailable; low-memory jobs use up the fat node's CPUs, "
            "wasting 648 GB; job J (16 CPUs, 128 GB) cannot run on either node. "
            "Jobs are labelled A to K. Well packed: every "
            "job runs, J included, the slim node wastes 60 GB, and the fat node keeps "
            "18 CPUs and 466 GB free for more jobs.")
    return (f'<svg xmlns="http://www.w3.org/2000/svg" width="{width:.0f}" height="{h:.0f}" '
            f'viewBox="0 0 {width:.0f} {h:.0f}" font-family="{FONT}" role="img">\n'
            f'<title>Badly and well packed nodes</title><desc>{desc}</desc>\n{defs}\n'
            f'<rect width="100%" height="100%" rx="8" fill="{c["surface"]}"/>\n{body}\n</svg>\n')


def main():
    out_dir = sys.argv[1] if len(sys.argv) > 1 else "docs/img"
    for theme in THEMES:
        path = f"{out_dir.rstrip('/')}/packing-{theme}.svg"
        open(path, "w").write(draw(theme))
        print("wrote", path)


if __name__ == "__main__":
    main()
