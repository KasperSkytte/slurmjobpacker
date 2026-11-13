// What a running job uses of what it was given, for the job details: a graph
// of CPU and memory over time as shares of its allocation, its averages and
// peak below, and a rough verdict. sjp-viz samples jobs with sstat; the state
// gives each running job's latest figures as `use`, /usage.json?job= its
// samples. A job whose GPUs Slurm measures has them too (`use.gpu`): how busy
// they are, as a share of the GPUs it was given, and its GPU memory, as a
// share of theirs where sjp.toml says how much a GPU has (`gpu_mem`).
const sjpUse = (() => {
  const pct = x => x == null ? "–" : Math.round(100 * x) + "%";
  const gb = mb => (mb >= 10240 ? (mb / 1024).toFixed(0) : (mb / 1024).toFixed(1)) + " GB";
  // How little is wasted: red when little of the allocation is utilized,
  // yellow at half, green when nearly all of it is (the lighter shade, for
  // text on the dark panel).
  const colour = (f, bar) =>
    `hsl(${120 * Math.min(1, Math.max(0, f))}, ${bar ? "70%, 48%" : "75%, 60%"})`;

  // The share of their allocation that running jobs utilize together, CPUs
  // now and memory in use, over the jobs measured so far; null before any is.
  function totals(running) {
    const jobs = new Map();
    for (const r of running) if (r.use && !jobs.has(r.id)) jobs.set(r.id, r);
    let cpus = 0, busy = 0, mem = 0, used = 0;
    for (const j of jobs.values()) {
      const n = j.nodes || 1, b = j.use.now != null ? j.use.now : j.use.avg;
      if (b == null) continue;
      cpus += j.cpus * n; busy += b; mem += j.mem * n; used += j.use.mem;
    }
    return cpus ? { cpu: busy / cpus, mem: used / mem } : null;
  }

  // The panel's CPU or memory bar: allocated as before, and within it the part
  // utilized in green, the rest of the allocation, unused, in red.
  function bar(alloc, util, share) {
    alloc.style.background = share == null ? "" : colour(0, true);
    util.style.width = share == null ? "0" : 100 * Math.min(1, share) + "%";
    util.style.background = colour(1, true);
  }

  // A running job's shares of its allocation: CPUs kept busy on average and
  // now, memory in use and at its peak. null until it has been sampled.
  function shares(j) {
    const u = j.use;
    if (!u) return null;
    const cpus = j.cpus * (j.nodes || 1), mem = j.mem * (j.nodes || 1);
    // GPUs: percent summed over them, so of 100 % for each GPU it was given
    const gpus = (j.gpus || 0) * (j.nodes || 1), gu = gpus && u.gpu;
    return { cpus, memAlloc: mem, cpu: u.avg != null ? u.avg / cpus : null,
             now: u.now != null ? u.now / cpus : null, mem: u.mem / mem, peak: u.peak / mem,
             memMb: u.mem, peakMb: u.peak,
             gpus: gu ? gpus : 0, gpu: gu ? gu.avg / (100 * gpus) : null,
             gpuNow: gu ? gu.now / (100 * gpus) : null, gpuMb: gu ? gu.mem : 0,
             gpuPeakMb: gu ? gu.peak : 0, gpuMemAlloc: gu && j.gpu_mem ? j.gpu_mem * gpus : 0 };
  }

  // Rough verdicts, for users to see whether a job runs as intended; not for
  // accounting. [text, colour] for CPU, its average coloured by the share
  // utilized; and memory, its peak from red at a quarter of what it asked for
  // or less to green at 90 % or more.
  function verdict(s) {
    const cpu = s.cpu == null ? ["measuring…", null]
      : [s.cpu < 0.25 ? "mostly idle" : s.cpu < 0.6 ? "partly utilized" : "well utilized", colour(s.cpu)];
    const mem = [s.peak > 0.95 ? "near its limit" : s.peak >= 0.7 ? "fits well"
      : s.peak >= 0.25 ? "less than allocated" : "far less than allocated",
      colour((s.peak - 0.25) / 0.65)];
    const gpu = s.gpu == null ? null
      : [s.gpu < 0.25 ? "mostly idle" : s.gpu < 0.6 ? "partly utilized" : "well utilized", colour(s.gpu)];
    return { cpu, mem, gpu };
  }
  const tag = ([text, colour]) => colour
    ? `<span style="color:${colour}">● ${text}</span>` : `<span style="opacity:.7">${text}</span>`;

  function rows(j) {
    const s = shares(j);
    if (!s) return `<tr><td>Use</td><td>not measured yet</td></tr>`;
    const v = verdict(s);
    return `<tr><td>CPU use</td><td>${pct(s.cpu)} of ${+s.cpus.toFixed(1)} CPUs on average` +
      (s.now != null ? `, ${pct(s.now)} now` : "") + `<br>${tag(v.cpu)}</td></tr>` +
      `<tr><td>Memory use</td><td>${gb(s.memMb)} of ${gb(s.memAlloc)} (${pct(s.mem)}), ` +
      `peak ${pct(s.peak)}<br>${tag(v.mem)}</td></tr>`;
  }
  function gpuRows(j) {
    const s = shares(j), v = s && verdict(s);
    if (!v || !v.gpu) return "";
    const a = s.gpuMemAlloc;             // 0: how much a GPU has is not set
    return `<tr><td>GPU use</td><td>${pct(s.gpu)} of ${+s.gpus.toFixed(1)} GPU${s.gpus === 1 ? "" : "s"} ` +
      `on average, ${pct(s.gpuNow)} now<br>${tag(v.gpu)}</td></tr>` +
      `<tr><td>GPU memory</td><td>` + (a
        ? `${gb(s.gpuMb)} of ${gb(a)} (${pct(s.gpuMb / a)}), peak ${pct(s.gpuPeakMb / a)}`
        : `${gb(s.gpuMb)} in use, peak ${gb(s.gpuPeakMb)}`) + `</td></tr>`;
  }

  // A graph of samples over time: lines [column, divisor, colour, name], each
  // sample's column divided by its divisor, 1 at the dashed line (100%).
  // right: [label at the dashed line] for a second scale on the right.
  function chart(pts, lines, w, h, right) {
    if (pts.length < 2)
      return `<div style="font-size:12px;opacity:.7">The graph starts after two samples.</div>`;
    const t0 = pts[0][0], t1 = pts[pts.length - 1][0], pad = 32, rpad = right ? 40 : 4;
    const top = Math.max(1.1, ...pts.map(p => Math.max(...lines.map(([k, d]) => p[k] / d)))) * 1.05;
    const X = t => pad + (w - pad - rpad) * (t - t0) / Math.max(t1 - t0, 1);
    const Y = f => 4 + (h - 20) * (1 - f / top);
    const path = (k, div) => pts.map((p, i) =>
      `${i ? "L" : "M"}${X(p[0]).toFixed(1)},${Y(p[k] / div).toFixed(1)}`).join("");
    const span = t1 - t0, ago = span >= 86400 ? (span / 86400).toFixed(1) + " d"
      : span >= 3600 ? (span / 3600).toFixed(1) + " h" : Math.round(span / 60) + " min";
    const rx = w - rpad + 4;
    return `<svg width="${w}" height="${h}" style="display:block;overflow:visible">
      <line x1="${pad}" x2="${w - rpad}" y1="${Y(1)}" y2="${Y(1)}" stroke="#8b949e" stroke-dasharray="3 3"/>
      <text x="${pad - 4}" y="${Y(1) + 3}" fill="#8b949e" font-size="10" text-anchor="end">100%</text>
      <text x="${pad - 4}" y="${Y(0) + 3}" fill="#8b949e" font-size="10" text-anchor="end">0</text>
      ${right ? `<text x="${rx}" y="${Y(1) + 3}" fill="#8b949e" font-size="10">${right}</text>
      <text x="${rx}" y="${Y(0) + 3}" fill="#8b949e" font-size="10">0</text>` : ""}
      <line x1="${pad}" x2="${w - rpad}" y1="${Y(0)}" y2="${Y(0)}" stroke="#30363d"/>
      ${[...lines].reverse().map(([k, d, c]) =>
        `<path d="${path(k, d)}" fill="none" stroke="${c}" stroke-width="1.5"/>`).join("")}
      <text x="${pad}" y="${h - 2}" fill="#8b949e" font-size="10">${ago} ago</text>
      <text x="${w - rpad}" y="${h - 2}" fill="#8b949e" font-size="10" text-anchor="end">now</text>
    </svg>
    <div style="font-size:11px;color:#8b949e;margin-top:2px">` +
      lines.map(([, , c, name]) => `<span style="color:${c}">━</span> ${name}`).join(" &nbsp; ") + `</div>`;
  }
  // A round amount of GPU memory (MB) above the most a job used: the right
  // scale's top. Slurm does not say how much a GPU has, so the job's own use sets it.
  function gpuMemTop(pts) {
    const most = Math.max(1024, ...pts.map(p => p[4]));
    const step = 2 ** Math.ceil(Math.log2(most / 1024)) * 1024 / 4;
    return Math.ceil(most * 1.1 / step) * step;
  }

  // The job's part of the details: title, graph, then its use in figures; for
  // a job with measured GPUs, a second graph and rows for them.
  // latest() gives the job as the newest state has it; the part is refreshed
  // from it every 15 s while it stays on screen.
  async function panel(el, j, latest = () => j, w = 232, h = 96) {
    const id = String(j.id);
    el.dataset.job = id;
    let pts;
    try { pts = await (await fetch(`usage.json?job=${encodeURIComponent(id)}`)).json(); }
    catch (e) { pts = []; }
    if (el.dataset.job !== id) return;              // another job was picked meanwhile
    j = latest() || j;
    const title = t => `<div style="font-weight:600;margin-bottom:4px">${t}</div>`;
    const n = j.nodes || 1, gpus = j.use && j.use.gpu ? (j.gpus || 0) * n : 0;
    // el is a tbody of the details table, so the rows line up with the others
    let html = `<tr><td colspan="2" style="color:inherit;padding-top:8px">` +
      title("Job utilization") +
      chart(pts, [[1, j.cpus * n, "#58a6ff", "CPU"], [2, j.mem * n, "#f0883e", "memory"]], w, h) +
      `</td></tr>` + rows(j);
    if (gpus) {
      // GPU memory as a share of the GPUs' own, on the same scale; where how
      // much a GPU has is not set, in GB on a scale of its own (right)
      const g = pts.filter(p => p.length > 4), alloc = j.gpu_mem ? j.gpu_mem * gpus : 0;
      const top = alloc || (g.length ? gpuMemTop(g) : 0);
      html += `<tr><td colspan="2" style="color:inherit;padding-top:8px">` + title("GPU utilization") +
        chart(g, [[3, 100 * gpus, "#3fb950", "GPU"],
                  [4, top, "#d2a8ff", alloc ? "GPU memory" : "GPU memory (right)"]],
              w, h, alloc ? "" : gb(top)) + `</td></tr>` + gpuRows(j);
    }
    el.innerHTML = html;
    setTimeout(() => {
      if (el.dataset.job === id && el.isConnected && el.offsetParent) panel(el, j, latest, w, h);
    }, 15000);
  }

  // The share of its CPUs a running job keeps busy, on average (now, until
  // there is an average); null if not measured.
  function efficiency(j) {
    const s = shares(j);
    return !s ? null : s.cpu != null ? s.cpu : s.now;
  }

  return { panel, totals, bar, pct, colour, efficiency };
})();
