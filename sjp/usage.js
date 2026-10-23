// What a running job uses of what it was given, for the job details: a graph
// of CPU and memory over time as shares of its allocation, its averages and
// peak below, and a rough verdict. sjp-viz samples jobs with sstat; the state
// gives each running job's latest figures as `use`, /usage.json?job= its
// samples.
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
  // utilized, coloured on the scale above.
  function bar(alloc, util, share) {
    alloc.style.background = share == null ? "" : "rgba(88, 166, 255, 0.3)";
    util.style.width = share == null ? "0" : 100 * Math.min(1, share) + "%";
    if (share != null) util.style.background = colour(share, true);
  }

  // A running job's shares of its allocation: CPUs kept busy on average and
  // now, memory in use and at its peak. null until it has been sampled.
  function shares(j) {
    const u = j.use;
    if (!u) return null;
    const cpus = j.cpus * (j.nodes || 1), mem = j.mem * (j.nodes || 1);
    return { cpus, memAlloc: mem, cpu: u.avg != null ? u.avg / cpus : null,
             now: u.now != null ? u.now / cpus : null, mem: u.mem / mem, peak: u.peak / mem,
             memMb: u.mem, peakMb: u.peak };
  }

  // Rough verdicts, for users to see whether a job runs as intended; not for
  // accounting. [text, colour] for CPU (its average) and memory (its peak),
  // coloured by the share utilized.
  function verdict(s) {
    const cpu = s.cpu == null ? ["measuring…", null]
      : [s.cpu < 0.25 ? "mostly idle" : s.cpu < 0.6 ? "partly utilized" : "well utilized", colour(s.cpu)];
    const mem = [s.peak > 0.95 ? "near its limit" : s.peak >= 0.7 ? "fits"
      : s.peak >= 0.25 ? "less than allocated" : "far less than allocated", colour(s.peak)];
    return { cpu, mem };
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

  function svg(pts, cpus, mem, w, h) {
    if (pts.length < 2)
      return `<div style="font-size:12px;opacity:.7">The graph starts after two samples.</div>`;
    const t0 = pts[0][0], t1 = pts[pts.length - 1][0], pad = 32;
    const top = Math.max(1.1, ...pts.map(p => Math.max(p[1] / cpus, p[2] / mem))) * 1.05;
    const X = t => pad + (w - pad - 4) * (t - t0) / Math.max(t1 - t0, 1);
    const Y = f => 4 + (h - 20) * (1 - f / top);
    const line = (k, div) => pts.map((p, i) =>
      `${i ? "L" : "M"}${X(p[0]).toFixed(1)},${Y(p[k] / div).toFixed(1)}`).join("");
    const span = t1 - t0, ago = span >= 86400 ? (span / 86400).toFixed(1) + " d"
      : span >= 3600 ? (span / 3600).toFixed(1) + " h" : Math.round(span / 60) + " min";
    return `<svg width="${w}" height="${h}" style="display:block;overflow:visible">
      <line x1="${pad}" x2="${w - 4}" y1="${Y(1)}" y2="${Y(1)}" stroke="#8b949e" stroke-dasharray="3 3"/>
      <text x="${pad - 4}" y="${Y(1) + 3}" fill="#8b949e" font-size="10" text-anchor="end">100%</text>
      <text x="${pad - 4}" y="${Y(0) + 3}" fill="#8b949e" font-size="10" text-anchor="end">0</text>
      <line x1="${pad}" x2="${w - 4}" y1="${Y(0)}" y2="${Y(0)}" stroke="#30363d"/>
      <path d="${line(2, mem)}" fill="none" stroke="#f0883e" stroke-width="1.5"/>
      <path d="${line(1, cpus)}" fill="none" stroke="#58a6ff" stroke-width="1.5"/>
      <text x="${pad}" y="${h - 2}" fill="#8b949e" font-size="10">${ago} ago</text>
      <text x="${w - 4}" y="${h - 2}" fill="#8b949e" font-size="10" text-anchor="end">now</text>
    </svg>
    <div style="font-size:11px;color:#8b949e;margin-top:2px">
      <span style="color:#58a6ff">━</span> CPU &nbsp; <span style="color:#f0883e">━</span> memory</div>`;
  }

  // The job's part of the details: title, graph, then its use in figures.
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
    // el is a tbody of the details table, so the rows line up with the others
    el.innerHTML = `<tr><td colspan="2" style="color:inherit;padding-top:8px">` +
      `<div style="font-weight:600;margin-bottom:4px">Job utilization vs allocated</div>` +
      svg(pts, j.cpus * (j.nodes || 1), j.mem * (j.nodes || 1), w, h) + `</td></tr>` + rows(j);
    setTimeout(() => {
      if (el.dataset.job === id && el.isConnected && el.offsetParent) panel(el, j, latest, w, h);
    }, 15000);
  }

  return { panel, totals, bar, pct, colour };
})();
