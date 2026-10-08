/* Minimal dependency-free SVG charts: line, grouped bar, horizontal bar.
   All charts share the same chrome: hairline grid, muted axis text, thin
   marks, hover tooltips. Colors come from CSS custom properties so light
   and dark mode both work. */
(function () {
  "use strict";

  const NS = "http://www.w3.org/2000/svg";

  function el(tag, attrs) {
    const node = document.createElementNS(NS, tag);
    for (const k in attrs) node.setAttribute(k, attrs[k]);
    return node;
  }

  function cssVar(name) {
    return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  }

  function niceTicks(maxValue, count) {
    if (maxValue <= 0) return [0, 1];
    const rough = maxValue / count;
    const mag = Math.pow(10, Math.floor(Math.log10(rough)));
    const step = [1, 2, 2.5, 5, 10].map((m) => m * mag).find((s) => s >= rough) || mag * 10;
    const ticks = [];
    for (let v = 0; v <= maxValue + 1e-9; v += step) ticks.push(+v.toFixed(6));
    if (ticks[ticks.length - 1] < maxValue) ticks.push(ticks[ticks.length - 1] + step);
    return ticks;
  }

  function makeTooltip(container) {
    const tip = document.createElement("div");
    tip.className = "tooltip";
    container.appendChild(tip);
    return tip;
  }

  function showTip(tip, container, xPix, yPix, html) {
    tip.innerHTML = html;
    tip.style.display = "block";
    const cw = container.clientWidth;
    const tw = tip.offsetWidth;
    let left = xPix + 12;
    if (left + tw > cw - 4) left = xPix - tw - 12;
    tip.style.left = Math.max(4, left) + "px";
    tip.style.top = Math.max(0, yPix - 34) + "px";
  }

  /* ------------------------------------------------------------------ */
  /* Line chart with shared categorical x (e.g. weeks), crosshair hover  */
  /* opts: { series: [{name, values, color}], labels: [..],              */
  /*         markers: [{index, value, color, label}], yMax, yFmt,        */
  /*         height, threshold: {value, label} }                         */
  /* ------------------------------------------------------------------ */
  function lineChart(container, opts) {
    container.classList.add("chart");
    container.innerHTML = "";
    const W = 720, H = opts.height || 240;
    const pad = { l: 44, r: 12, t: 10, b: 26 };
    const iw = W - pad.l - pad.r, ih = H - pad.t - pad.b;
    const labels = opts.labels;
    const n = labels.length;

    let maxV = opts.yMax;
    if (maxV == null) {
      maxV = 0;
      opts.series.forEach((s) => s.values.forEach((v) => { if (v != null && v > maxV) maxV = v; }));
      maxV = maxV || 1;
    }
    const ticks = opts.yMax != null ? null : niceTicks(maxV, 4);
    const yTop = ticks ? ticks[ticks.length - 1] : maxV;

    const x = (i) => pad.l + (n === 1 ? iw / 2 : (i / (n - 1)) * iw);
    const y = (v) => pad.t + ih - (v / yTop) * ih;

    const svg = el("svg", { viewBox: `0 0 ${W} ${H}`, role: "img" });

    (ticks || [0, yTop / 2, yTop]).forEach((t) => {
      svg.appendChild(el("line", {
        x1: pad.l, x2: W - pad.r, y1: y(t), y2: y(t),
        stroke: cssVar("--grid"), "stroke-width": 1,
      }));
      const txt = el("text", {
        x: pad.l - 8, y: y(t) + 4, "text-anchor": "end",
        fill: cssVar("--muted"), "font-size": 11,
      });
      txt.textContent = opts.yFmt ? opts.yFmt(t) : t;
      svg.appendChild(txt);
    });
    svg.appendChild(el("line", {
      x1: pad.l, x2: W - pad.r, y1: y(0), y2: y(0),
      stroke: cssVar("--baseline"), "stroke-width": 1,
    }));

    // x labels: first, last, and a few between
    const lblCount = Math.min(6, n);
    for (let k = 0; k < lblCount; k++) {
      const i = Math.round((k / (lblCount - 1)) * (n - 1));
      const txt = el("text", {
        x: x(i), y: H - 8, "text-anchor": k === 0 ? "start" : k === lblCount - 1 ? "end" : "middle",
        fill: cssVar("--muted"), "font-size": 11,
      });
      txt.textContent = labels[i];
      svg.appendChild(txt);
    }

    if (opts.threshold) {
      svg.appendChild(el("line", {
        x1: pad.l, x2: W - pad.r,
        y1: y(opts.threshold.value), y2: y(opts.threshold.value),
        stroke: cssVar("--muted"), "stroke-width": 1, "stroke-dasharray": "4 4",
      }));
      const txt = el("text", {
        x: W - pad.r, y: y(opts.threshold.value) - 5, "text-anchor": "end",
        fill: cssVar("--muted"), "font-size": 10.5,
      });
      txt.textContent = opts.threshold.label;
      svg.appendChild(txt);
    }

    opts.series.forEach((s) => {
      let d = "", pen = false;
      s.values.forEach((v, i) => {
        if (v == null) { pen = false; return; }
        d += (pen ? "L" : "M") + x(i).toFixed(1) + " " + y(v).toFixed(1);
        pen = true;
      });
      svg.appendChild(el("path", {
        d, fill: "none", stroke: s.color, "stroke-width": 2,
        "stroke-linejoin": "round", "stroke-linecap": "round",
      }));
    });

    (opts.markers || []).forEach((m) => {
      svg.appendChild(el("circle", {
        cx: x(m.index), cy: y(m.value), r: 4,
        fill: m.color, stroke: cssVar("--surface"), "stroke-width": 2,
      }));
    });

    const cross = el("line", {
      y1: pad.t, y2: pad.t + ih, stroke: cssVar("--baseline"),
      "stroke-width": 1, visibility: "hidden",
    });
    svg.appendChild(cross);
    const dots = opts.series.map((s) =>
      svg.appendChild(el("circle", { r: 4, fill: s.color, stroke: cssVar("--surface"), "stroke-width": 2, visibility: "hidden" }))
    );

    const tip = makeTooltip(container);
    svg.addEventListener("mousemove", (ev) => {
      const rect = svg.getBoundingClientRect();
      const sx = ((ev.clientX - rect.left) / rect.width) * W;
      let i = Math.round(((sx - pad.l) / iw) * (n - 1));
      i = Math.max(0, Math.min(n - 1, i));
      cross.setAttribute("x1", x(i));
      cross.setAttribute("x2", x(i));
      cross.setAttribute("visibility", "visible");
      let html = `<div class="t-title">${labels[i]}</div>`;
      opts.series.forEach((s, si) => {
        const v = s.values[i];
        if (v == null) { dots[si].setAttribute("visibility", "hidden"); return; }
        dots[si].setAttribute("cx", x(i));
        dots[si].setAttribute("cy", y(v));
        dots[si].setAttribute("visibility", "visible");
        const shown = opts.yFmt ? opts.yFmt(v) : v;
        html += `<div class="t-row"><span class="t-swatch" style="background:${s.color}"></span>${s.name}: <b>${shown}</b></div>`;
      });
      (opts.markers || []).forEach((m) => {
        if (m.index === i && m.label) html += `<div class="t-row"><span class="t-swatch" style="background:${m.color}"></span>${m.label}</div>`;
      });
      const py = ((ev.clientY - rect.top) / rect.height) * H;
      showTip(tip, container, (x(i) / W) * container.clientWidth, (py / H) * container.clientHeight, html);
    });
    svg.addEventListener("mouseleave", () => {
      tip.style.display = "none";
      cross.setAttribute("visibility", "hidden");
      dots.forEach((d) => d.setAttribute("visibility", "hidden"));
    });

    container.appendChild(svg);
  }

  /* ------------------------------------------------------------------ */
  /* Grouped bar chart. opts: { categories, series:[{name,values,color}],*/
  /*   yMax, yFmt, height }                                              */
  /* ------------------------------------------------------------------ */
  function groupedBarChart(container, opts) {
    container.classList.add("chart");
    container.innerHTML = "";
    const W = 720, H = opts.height || 240;
    const pad = { l: 44, r: 12, t: 10, b: 26 };
    const iw = W - pad.l - pad.r, ih = H - pad.t - pad.b;
    const cats = opts.categories, ns = opts.series.length;

    let maxV = opts.yMax;
    if (maxV == null) {
      maxV = 0;
      opts.series.forEach((s) => s.values.forEach((v) => { if (v > maxV) maxV = v; }));
      maxV = maxV || 1;
    }
    const y = (v) => pad.t + ih - (v / maxV) * ih;
    const svg = el("svg", { viewBox: `0 0 ${W} ${H}`, role: "img" });

    [0, maxV / 2, maxV].forEach((t) => {
      svg.appendChild(el("line", {
        x1: pad.l, x2: W - pad.r, y1: y(t), y2: y(t),
        stroke: cssVar("--grid"), "stroke-width": 1,
      }));
      const txt = el("text", {
        x: pad.l - 8, y: y(t) + 4, "text-anchor": "end",
        fill: cssVar("--muted"), "font-size": 11,
      });
      txt.textContent = opts.yFmt ? opts.yFmt(t) : t;
      svg.appendChild(txt);
    });

    const groupW = iw / cats.length;
    const barW = Math.min(26, (groupW * 0.7) / ns);
    const tip = makeTooltip(container);

    cats.forEach((cat, ci) => {
      const cx = pad.l + groupW * ci + groupW / 2;
      const txt = el("text", {
        x: cx, y: H - 8, "text-anchor": "middle",
        fill: cssVar("--muted"), "font-size": 11,
      });
      txt.textContent = cat;
      svg.appendChild(txt);

      opts.series.forEach((s, si) => {
        const v = s.values[ci];
        if (v == null) return;
        const bx = cx - (ns * barW + (ns - 1) * 2) / 2 + si * (barW + 2);
        const by = y(v), h = y(0) - by;
        const r = Math.min(4, h);
        const d = `M${bx} ${y(0)} L${bx} ${by + r} Q${bx} ${by} ${bx + r} ${by} ` +
          `L${bx + barW - r} ${by} Q${bx + barW} ${by} ${bx + barW} ${by + r} L${bx + barW} ${y(0)} Z`;
        const path = el("path", { d, fill: s.color });
        path.addEventListener("mousemove", (ev) => {
          const rect = svg.getBoundingClientRect();
          const shown = opts.yFmt ? opts.yFmt(v) : v;
          showTip(tip, container,
            ((bx + barW / 2) / W) * container.clientWidth,
            ((ev.clientY - rect.top) / rect.height) * container.clientHeight,
            `<div class="t-title">${cat}</div><div class="t-row"><span class="t-swatch" style="background:${s.color}"></span>${s.name}: <b>${shown}</b></div>`);
        });
        path.addEventListener("mouseleave", () => { tip.style.display = "none"; });
        svg.appendChild(path);
      });
    });

    svg.appendChild(el("line", {
      x1: pad.l, x2: W - pad.r, y1: y(0), y2: y(0),
      stroke: cssVar("--baseline"), "stroke-width": 1,
    }));
    container.appendChild(svg);
  }

  /* ------------------------------------------------------------------ */
  /* Horizontal bar chart for rankings (single series).                  */
  /* opts: { items: [{label, value}], color, vFmt, height }              */
  /* ------------------------------------------------------------------ */
  function hBarChart(container, opts) {
    container.classList.add("chart");
    container.innerHTML = "";
    const rowH = 26;
    const W = 720, H = opts.items.length * rowH + 8;
    const pad = { l: 170, r: 56, t: 4, b: 4 };
    const iw = W - pad.l - pad.r;
    const maxV = Math.max(...opts.items.map((d) => d.value)) || 1;
    const svg = el("svg", { viewBox: `0 0 ${W} ${H}`, role: "img" });
    const color = opts.color || cssVar("--series-1");

    opts.items.forEach((d, i) => {
      const cy = pad.t + i * rowH + rowH / 2;
      const w = (d.value / maxV) * iw;
      const lbl = el("text", {
        x: pad.l - 8, y: cy + 4, "text-anchor": "end",
        fill: cssVar("--ink-2"), "font-size": 12,
      });
      lbl.textContent = d.label;
      svg.appendChild(lbl);
      const r = Math.min(4, w);
      const yTopBar = cy - 7;
      const dPath = `M${pad.l} ${yTopBar} L${pad.l + w - r} ${yTopBar} Q${pad.l + w} ${yTopBar} ${pad.l + w} ${yTopBar + r} ` +
        `L${pad.l + w} ${yTopBar + 14 - r} Q${pad.l + w} ${yTopBar + 14} ${pad.l + w - r} ${yTopBar + 14} L${pad.l} ${yTopBar + 14} Z`;
      svg.appendChild(el("path", { d: dPath, fill: color }));
      const val = el("text", {
        x: pad.l + w + 6, y: cy + 4, "text-anchor": "start",
        fill: cssVar("--muted"), "font-size": 11.5,
      });
      val.textContent = opts.vFmt ? opts.vFmt(d.value) : d.value;
      svg.appendChild(val);
    });

    svg.appendChild(el("line", {
      x1: pad.l, x2: pad.l, y1: pad.t, y2: H - pad.b,
      stroke: cssVar("--baseline"), "stroke-width": 1,
    }));
    container.appendChild(svg);
  }

  window.Charts = { lineChart, groupedBarChart, hBarChart };
})();
