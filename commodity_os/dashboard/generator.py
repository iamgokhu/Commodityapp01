"""Dashboard generator producing self-contained HTML + JSON for D3.js visualizations."""

import asyncio
import json
import logging
import os
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

from commodity_os.core.events import Event, EventType, event_bus

logger = logging.getLogger(__name__)

D3_CDN = "https://d3js.org/d3.v7.min.js"


class DashboardGenerator:
    """Generates JSON data and a self-contained HTML dashboard for commodity data."""

    def __init__(self, output_dir: str = "output"):
        self.output_dir = Path(output_dir)
        self.assets_dir = self.output_dir / "dashboard_assets"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.assets_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def generate(self, entities: List[Dict], consolidated: Dict) -> None:
        json_data = self.generate_json_data(entities, consolidated)
        html_content = self.generate_html(json_data)
        self.save_files(json_data, html_content)

        await event_bus.emit(
            EventType.DASHBOARD_JSON,
            payload={"path": str(self.output_dir / "dashboard.json"), "keys": list(json_data.keys())},
            source="DashboardGenerator",
        )
        await event_bus.emit(
            EventType.DASHBOARD_HTML,
            payload={"path": str(self.output_dir / "dashboard.html")},
            source="DashboardGenerator",
        )
        logger.info("Dashboard files written to %s", self.output_dir)

    # ------------------------------------------------------------------
    # 1. JSON data generation
    # ------------------------------------------------------------------

    def generate_json_data(self, entities: List[Dict], consolidated: Dict) -> Dict[str, Any]:
        data = self._safe_entities(entities)
        ts = time.time()

        bar_by_state = self._count_field(data, "state")
        bar_by_product = self._count_field(data, "product")
        bar_by_type = self._count_field(data, "entity_type")

        pie_state = self._count_field(data, "state", limit=10)
        pie_product = self._count_field(data, "product")
        pie_type = self._count_field(data, "entity_type")

        price_trends = self._price_trends(data)
        treemap_data = self._treemap(data)
        force_data = self._force_graph(data)
        table_rows = self._top_entities(data, limit=50)

        summary = {
            "total_entities": len(data),
            "states_covered": len(set(e.get("state", "") for e in data if e.get("state"))),
            "products": len(set(e.get("product", "") for e in data if e.get("product"))),
            "entity_types": len(set(e.get("entity_type", "") for e in data if e.get("entity_type"))),
        }

        all_states = sorted(set(e.get("state", "") for e in data if e.get("state")))
        all_products = sorted(set(e.get("product", "") for e in data if e.get("product")))
        all_types = sorted(set(e.get("entity_type", "") for e in data if e.get("entity_type")))

        return {
            "generated_at": ts,
            "summary": summary,
            "filters": {"states": all_states, "products": all_products, "entity_types": all_types},
            "bar": {"by_state": bar_by_state, "by_product": bar_by_product, "by_type": bar_by_type},
            "pie": {"state": pie_state, "product": pie_product, "entity_type": pie_type},
            "price_trends": price_trends,
            "treemap": treemap_data,
            "force": force_data,
            "table": table_rows,
            "consolidated": consolidated,
        }

    # ------------------------------------------------------------------
    # 2. HTML generation
    # ------------------------------------------------------------------

    def generate_html(self, json_data: Dict[str, Any]) -> str:
        data_js = json.dumps(json_data, ensure_ascii=False)
        return HTML_TEMPLATE.replace("__DASHBOARD_DATA__", data_js)

    # ------------------------------------------------------------------
    # 3. File I/O
    # ------------------------------------------------------------------

    def save_files(self, json_data: Dict[str, Any], html_content: str) -> None:
        json_path = self.output_dir / "dashboard.json"
        html_path = self.output_dir / "dashboard.html"

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html_content)

        logger.info("Wrote %s (%d bytes) and %s (%d bytes)", json_path, json_path.stat().st_size, html_path, html_path.stat().st_size)

    # ------------------------------------------------------------------
    # Helpers – data aggregation
    # ------------------------------------------------------------------

    @staticmethod
    def _safe_entities(entities: Any) -> List[Dict]:
        if not isinstance(entities, list):
            return []
        return [e for e in entities if isinstance(e, dict)]

    @staticmethod
    def _count_field(data: List[Dict], field: str, limit: int = 0) -> List[Dict[str, Any]]:
        counts = Counter(e.get(field, "Unknown") or "Unknown" for e in data)
        items = [{"name": k, "value": v} for k, v in counts.most_common(limit if limit else len(counts))]
        return items

    @staticmethod
    def _price_trends(data: List[Dict]) -> List[Dict[str, Any]]:
        by_date: Dict[str, List[float]] = defaultdict(list)
        for e in data:
            price = e.get("market_price")
            date = e.get("date") or e.get("price_date")
            if price is not None and date:
                try:
                    by_date[str(date)].append(float(price))
                except (ValueError, TypeError):
                    continue
        trends = []
        for d in sorted(by_date):
            vals = by_date[d]
            trends.append({"date": d, "avg_price": round(sum(vals) / len(vals), 2), "min_price": min(vals), "max_price": max(vals), "count": len(vals)})
        return trends

    @staticmethod
    def _treemap(data: List[Dict]) -> Dict[str, Any]:
        tree: Dict[str, Any] = {"name": "India", "children": []}
        by_state: Dict[str, Dict[str, List[str]]] = defaultdict(lambda: defaultdict(list))
        for e in data:
            state = e.get("state", "Unknown") or "Unknown"
            district = e.get("district", "Unknown") or "Unknown"
            taluk = e.get("taluk", "Unknown") or "Unknown"
            by_state[state][district].append(taluk)
        for state_name, districts in sorted(by_state.items()):
            state_node: Dict[str, Any] = {"name": state_name, "children": []}
            for dist_name, tuks in sorted(districts.items()):
                dist_node: Dict[str, Any] = {"name": dist_name, "children": []}
                for t in sorted(set(tuks)):
                    dist_node["children"].append({"name": t, "value": len([1 for _ in tuks if _ == t])})
                state_node["children"].append(dist_node)
            tree["children"].append(state_node)
        return tree

    @staticmethod
    def _force_graph(data: List[Dict]) -> Dict[str, Any]:
        nodes: List[Dict] = []
        links: List[Dict] = []
        node_set = set()
        for e in data:
            name = e.get("name", "Unknown") or "Unknown"
            if name not in node_set:
                node_set.add(name)
                nodes.append({"id": name, "group": e.get("entity_type", "Unknown"), "state": e.get("state", "")})
        source_field_map = {"manufacturer": "suppliers", "wholesaler": "clients", "exporter": "partners"}
        for e in data:
            src = e.get("name", "") or ""
            for rel_key in ("suppliers", "clients", "partners", "related"):
                for target in e.get(rel_key, []):
                    if target in node_set:
                        links.append({"source": src, "target": target, "type": rel_key})
        return {"nodes": nodes, "links": links}

    @staticmethod
    def _top_entities(data: List[Dict], limit: int = 50) -> List[Dict[str, Any]]:
        rows = []
        for e in data:
            rows.append({
                "name": e.get("name", ""),
                "state": e.get("state", ""),
                "district": e.get("district", ""),
                "taluk": e.get("taluk", ""),
                "product": e.get("product", ""),
                "entity_type": e.get("entity_type", ""),
                "contact": e.get("contact", e.get("phone", "")),
                "email": e.get("email", ""),
                "website": e.get("website", ""),
                "market_price": e.get("market_price", ""),
                "year_established": e.get("year_established", ""),
            })
        return rows[:limit]


# ======================================================================
# Self-contained HTML template with embedded CSS/JS + D3.js
# ======================================================================

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta http-equiv="refresh" content="300">
<title>Commodity Dashboard</title>
<style>
:root {
  --bg: #ffffff; --bg2: #f5f7fa; --fg: #1a1a2e; --fg2: #555;
  --card: #ffffff; --border: #e0e0e0; --accent: #2563eb;
  --accent2: #7c3aed; --accent3: #059669; --accent4: #dc2626;
  --table-hover: #f0f4ff; --shadow: rgba(0,0,0,.08);
}
[data-theme="dark"] {
  --bg: #0f172a; --bg2: #1e293b; --fg: #e2e8f0; --fg2: #94a3b8;
  --card: #1e293b; --border: #334155; --accent: #60a5fa;
  --accent2: #a78bfa; --accent3: #34d399; --accent4: #f87171;
  --table-hover: #1e3a5f; --shadow: rgba(0,0,0,.3);
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: var(--bg); color: var(--fg); line-height: 1.5; }
.header { background: var(--bg2); border-bottom: 1px solid var(--border); padding: 12px 24px; display: flex; justify-content: space-between; align-items: center; position: sticky; top: 0; z-index: 100; }
.header h1 { font-size: 1.25rem; font-weight: 600; }
.header-controls { display: flex; gap: 8px; align-items: center; }
.btn { padding: 6px 14px; border-radius: 6px; border: 1px solid var(--border); background: var(--card); color: var(--fg); cursor: pointer; font-size: .85rem; transition: background .15s; }
.btn:hover { background: var(--accent); color: #fff; }
.btn.active { background: var(--accent); color: #fff; }
.container { max-width: 1400px; margin: 0 auto; padding: 20px; }
.cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-bottom: 24px; }
.card { background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 20px; box-shadow: 0 1px 3px var(--shadow); }
.card .label { font-size: .8rem; color: var(--fg2); text-transform: uppercase; letter-spacing: .5px; }
.card .value { font-size: 1.8rem; font-weight: 700; margin-top: 4px; }
.card:nth-child(1) .value { color: var(--accent); }
.card:nth-child(2) .value { color: var(--accent2); }
.card:nth-child(3) .value { color: var(--accent3); }
.card:nth-child(4) .value { color: var(--accent4); }
.filters { background: var(--bg2); border: 1px solid var(--border); border-radius: 10px; padding: 16px 20px; margin-bottom: 24px; display: flex; gap: 12px; align-items: center; flex-wrap: wrap; }
.filters label { font-size: .8rem; color: var(--fg2); text-transform: uppercase; }
.filters select { padding: 6px 10px; border-radius: 6px; border: 1px solid var(--border); background: var(--card); color: var(--fg); font-size: .85rem; }
.chart-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(420px, 1fr)); gap: 20px; margin-bottom: 24px; }
.chart-box { background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 16px; box-shadow: 0 1px 3px var(--shadow); overflow: hidden; }
.chart-box h3 { font-size: .95rem; margin-bottom: 12px; color: var(--fg2); }
.chart-box svg { width: 100%; }
.treemap-box, .force-box { margin-bottom: 24px; }
.table-section { background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 16px; box-shadow: 0 1px 3px var(--shadow); margin-bottom: 24px; overflow-x: auto; }
.table-section h3 { font-size: .95rem; margin-bottom: 12px; color: var(--fg2); }
.search-box { padding: 8px 12px; border: 1px solid var(--border); border-radius: 6px; background: var(--bg2); color: var(--fg); width: 260px; margin-bottom: 12px; font-size: .85rem; }
table { width: 100%; border-collapse: collapse; font-size: .82rem; }
th, td { text-align: left; padding: 8px 10px; border-bottom: 1px solid var(--border); }
th { background: var(--bg2); color: var(--fg2); font-weight: 600; cursor: pointer; white-space: nowrap; }
th:hover { color: var(--accent); }
tr:hover { background: var(--table-hover); }
.tooltip { position: absolute; background: var(--card); border: 1px solid var(--border); border-radius: 6px; padding: 8px 12px; font-size: .8rem; pointer-events: none; box-shadow: 0 4px 12px var(--shadow); z-index: 200; max-width: 280px; }
.footer { text-align: center; padding: 16px; font-size: .75rem; color: var(--fg2); }
@media print {
  .header-controls, .filters { display: none; }
  .chart-box, .card, .table-section { break-inside: avoid; box-shadow: none; border: 1px solid #ccc; }
  body { background: #fff; color: #000; }
}
@media (max-width: 768px) {
  .chart-row { grid-template-columns: 1fr; }
  .cards { grid-template-columns: repeat(2, 1fr); }
  .filters { flex-direction: column; align-items: stretch; }
}
</style>
</head>
<body>
<div class="header">
  <h1>Commodity Intelligence Dashboard</h1>
  <div class="header-controls">
    <button class="btn" onclick="refreshData()">Refresh</button>
    <button class="btn" onclick="toggleTheme()" id="themeBtn">Dark Mode</button>
    <button class="btn" onclick="window.print()">Print</button>
  </div>
</div>

<div class="container">
  <!-- Summary Cards -->
  <div class="cards" id="summaryCards"></div>

  <!-- Filters -->
  <div class="filters">
    <div><label>State</label><br><select id="filterState"><option value="">All</option></select></div>
    <div><label>Product</label><br><select id="filterProduct"><option value="">All</option></select></div>
    <div><label>Entity Type</label><br><select id="filterType"><option value="">All</option></select></div>
    <div style="align-self:flex-end"><button class="btn" onclick="applyFilters()">Apply</button></div>
  </div>

  <!-- Charts Row 1: Bars -->
  <div class="chart-row">
    <div class="chart-box"><h3>Entities by State</h3><div id="barState"></div></div>
    <div class="chart-box"><h3>Entities by Product</h3><div id="barProduct"></div></div>
    <div class="chart-box"><h3>Entities by Type</h3><div id="barType"></div></div>
  </div>

  <!-- Charts Row 2: Pies -->
  <div class="chart-row">
    <div class="chart-box"><h3>State Distribution</h3><div id="pieState"></div></div>
    <div class="chart-box"><h3>Product Distribution</h3><div id="pieProduct"></div></div>
    <div class="chart-box"><h3>Entity Type Distribution</h3><div id="pieType"></div></div>
  </div>

  <!-- Price Trends -->
  <div class="chart-row">
    <div class="chart-box" style="grid-column:1/-1"><h3>Price Trends Over Time</h3><div id="lineChart"></div></div>
  </div>

  <!-- Treemap -->
  <div class="chart-box treemap-box"><h3>Geographic Hierarchy (State &gt; District &gt; Taluk)</h3><div id="treemap"></div></div>

  <!-- Force Graph -->
  <div class="chart-box force-box"><h3>Entity Relationships</h3><div id="forceGraph"></div></div>

  <!-- Data Table -->
  <div class="table-section">
    <h3>Top Entities</h3>
    <input type="text" class="search-box" id="tableSearch" placeholder="Search entities..." oninput="filterTable()">
    <div id="tableWrapper"><table id="dataTable"><thead><tr id="tableHead"></tr></thead><tbody id="tableBody"></tbody></table></div>
  </div>
</div>

<div class="footer" id="footer"></div>

<div class="tooltip" id="tooltip" style="display:none"></div>

<script src="https://d3js.org/d3.v7.min.js"></script>
<script>
(function() {
  const DATA = __DASHBOARD_DATA__;
  const tooltip = document.getElementById('tooltip');
  let currentData = DATA;

  const fmt = n => n.toLocaleString();
  const colors = ['#2563eb','#7c3aed','#059669','#dc2626','#d97706','#0891b2','#be185d','#4f46e5','#16a34a','#ea580c'];

  function showTip(evt, html) {
    tooltip.innerHTML = html;
    tooltip.style.display = 'block';
    const r = tooltip.getBoundingClientRect();
    let x = evt.pageX + 12, y = evt.pageY - 10;
    if (x + r.width > window.innerWidth - 10) x = evt.pageX - r.width - 12;
    if (y + r.height > document.body.scrollHeight) y = evt.pageY - r.height - 10;
    tooltip.style.left = x + 'px'; tooltip.style.top = y + 'px';
  }
  function hideTip() { tooltip.style.display = 'none'; }

  // --- Theme ---
  function getTheme() { return localStorage.getItem('dashTheme') || 'light'; }
  function applyTheme(t) {
    document.documentElement.setAttribute('data-theme', t);
    document.getElementById('themeBtn').textContent = t === 'dark' ? 'Light Mode' : 'Dark Mode';
  }
  window.toggleTheme = function() {
    const next = getTheme() === 'dark' ? 'light' : 'dark';
    localStorage.setItem('dashTheme', next);
    applyTheme(next);
    renderAll(currentData);
  };
  applyTheme(getTheme());

  // --- Summary Cards ---
  function renderSummary(s) {
    const el = document.getElementById('summaryCards');
    el.innerHTML = [
      { label: 'Total Entities', value: fmt(s.total_entities) },
      { label: 'States Covered', value: fmt(s.states_covered) },
      { label: 'Products', value: fmt(s.products) },
      { label: 'Entity Types', value: fmt(s.entity_types) },
    ].map(c => '<div class="card"><div class="label">' + c.label + '</div><div class="value">' + c.value + '</div></div>').join('');
  }

  // --- Filters ---
  function populateFilters(f) {
    const addOpts = (sel, list) => {
      const s = document.getElementById(sel);
      list.forEach(v => { const o = document.createElement('option'); o.value = v; o.textContent = v; s.appendChild(o); });
    };
    addOpts('filterState', f.states);
    addOpts('filterProduct', f.products);
    addOpts('filterType', f.entity_types);
  }
  window.applyFilters = function() {
    const st = document.getElementById('filterState').value;
    const pr = document.getElementById('filterProduct').value;
    const ty = document.getElementById('filterType').value;
    let d = DATA;
    if (st || pr || ty) {
      const d2 = Object.assign({}, DATA);
      if (d2.table) d2.table = d2.table.filter(r => (!st || r.state === st) && (!pr || r.product === pr) && (!ty || r.entity_type === ty));
      d2.summary = { total_entities: d2.table.length, states_covered: new Set(d2.table.map(r => r.state)).size, products: new Set(d2.table.map(r => r.product)).size, entity_types: new Set(d2.table.map(r => r.entity_type)).size };
      d2.bar = { by_state: countField(d2.table, 'state'), by_product: countField(d2.table, 'product'), by_type: countField(d2.table, 'entity_type') };
      d2.pie = { state: countField(d2.table, 'state', 10), product: countField(d2.table, 'product'), entity_type: countField(d2.table, 'entity_type') };
      currentData = d2;
    } else {
      currentData = DATA;
    }
    renderAll(currentData);
  };
  function countField(arr, f, lim) {
    const c = {}; arr.forEach(e => { const v = e[f] || 'Unknown'; c[v] = (c[v]||0)+1; });
    return Object.entries(c).sort((a,b) => b[1]-a[1]).slice(0, lim||9999).map(([n,v]) => ({name:n,value:v}));
  }

  // --- Bar Charts ---
  function renderBar(containerId, data, color) {
    const el = document.getElementById(containerId);
    el.innerHTML = '';
    if (!data || !data.length) return;
    const m = { top: 10, right: 20, bottom: 60, left: 50 };
    const w = el.clientWidth - 2;
    const barH = 28;
    const h = data.length * barH + m.top + m.bottom;
    const svg = d3.select('#' + containerId).append('svg').attr('width', w).attr('height', h);
    const g = svg.append('g').attr('transform', 'translate(' + m.left + ',' + m.top + ')');
    const x = d3.scaleLinear().domain([0, d3.max(data, d => d.value) || 1]).range([0, w - m.left - m.right - 80]);
    const y = d3.scaleBand().domain(data.map(d => d.name)).range([0, data.length * barH]).padding(0.25);
    g.selectAll('rect').data(data).join('rect')
      .attr('x', 0).attr('y', d => y(d.name)).attr('height', y.bandwidth()).attr('width', d => x(d.value))
      .attr('fill', color || colors[0]).attr('rx', 4).style('cursor', 'pointer')
      .on('mouseover', (evt, d) => showTip(evt, '<b>' + d.name + '</b><br>Count: ' + fmt(d.value)))
      .on('mouseout', hideTip);
    g.selectAll('.lbl').data(data).join('text').attr('class', 'lbl')
      .attr('x', -4).attr('y', d => y(d.name) + y.bandwidth() / 2).attr('dy', '.35em')
      .attr('text-anchor', 'end').attr('fill', 'var(--fg2)').attr('font-size', '11px').text(d => d.name.length > 18 ? d.name.slice(0, 16) + '…' : d.name);
    g.selectAll('.val').data(data).join('text').attr('class', 'val')
      .attr('x', d => x(d.value) + 4).attr('y', d => y(d.name) + y.bandwidth() / 2).attr('dy', '.35em')
      .attr('fill', 'var(--fg)').attr('font-size', '11px').text(d => fmt(d.value));
  }

  // --- Pie Charts ---
  function renderPie(containerId, data, colorSet) {
    const el = document.getElementById(containerId);
    el.innerHTML = '';
    if (!data || !data.length) return;
    const sz = Math.min(el.clientWidth, 320);
    const r = sz / 2 - 20;
    const svg = d3.select('#' + containerId).append('svg').attr('width', sz).attr('height', sz);
    const g = svg.append('g').attr('transform', 'translate(' + sz/2 + ',' + sz/2 + ')');
    const pie = d3.pie().value(d => d.value).sort(null);
    const arc = d3.arc().innerRadius(r * 0.4).outerRadius(r);
    const total = d3.sum(data, d => d.value);
    g.selectAll('path').data(pie(data)).join('path')
      .attr('d', arc).attr('fill', (d, i) => (colorSet || colors)[i % colors.length])
      .style('cursor', 'pointer')
      .on('mouseover', (evt, d) => showTip(evt, '<b>' + d.data.name + '</b><br>' + fmt(d.data.value) + ' (' + (d.data.value/total*100).toFixed(1) + '%)'))
      .on('mouseout', hideTip);
  }

  // --- Line Chart ---
  function renderLine(containerId, trends) {
    const el = document.getElementById(containerId);
    el.innerHTML = '';
    if (!trends || !trends.length) { el.innerHTML = '<p style="color:var(--fg2)">No price trend data available</p>'; return; }
    const m = { top: 20, right: 30, bottom: 40, left: 60 };
    const w = el.clientWidth - 2;
    const h = 260;
    const svg = d3.select('#' + containerId).append('svg').attr('width', w).attr('height', h);
    const g = svg.append('g').attr('transform', 'translate(' + m.left + ',' + m.top + ')');
    const iw = w - m.left - m.right;
    const ih = h - m.top - m.bottom;
    const x = d3.scalePoint().domain(trends.map(d => d.date)).range([0, iw]);
    const y = d3.scaleLinear().domain([0, d3.max(trends, d => d.max_price) || 1]).nice().range([ih, 0]);
    const line = d3.line().x(d => x(d.date)).y(d => y(d.avg_price)).curve(d3.curveMonotoneX);
    g.append('path').datum(trends).attr('fill', 'none').attr('stroke', colors[0]).attr('stroke-width', 2).attr('d', line);
    g.selectAll('circle').data(trends).join('circle')
      .attr('cx', d => x(d.date)).attr('cy', d => y(d.avg_price)).attr('r', 4)
      .attr('fill', colors[0]).style('cursor', 'pointer')
      .on('mouseover', (evt, d) => showTip(evt, '<b>' + d.date + '</b><br>Avg: ' + fmt(d.avg_price) + '<br>Range: ' + fmt(d.min_price) + ' – ' + fmt(d.max_price)))
      .on('mouseout', hideTip);
    g.append('g').attr('transform', 'translate(0,' + ih + ')').call(d3.axisBottom(x).tickSize(0)).selectAll('text').attr('transform', 'rotate(-40)').style('text-anchor', 'end').attr('font-size', '10px');
    g.append('g').call(d3.axisLeft(y).ticks(5)).selectAll('text').attr('font-size', '10px');
  }

  // --- Treemap ---
  function renderTreemap(containerId, tree) {
    const el = document.getElementById(containerId);
    el.innerHTML = '';
    if (!tree || !tree.children || !tree.children.length) { el.innerHTML = '<p style="color:var(--fg2)">No geographic data available</p>'; return; }
    const w = el.clientWidth - 2;
    const h = 400;
    const svg = d3.select('#' + containerId).append('svg').attr('width', w).attr('height', h);
    const root = d3.hierarchy(tree).sum(d => d.value || 1);
    d3.treemap().size([w, h]).padding(2)(root);
    const nodes = svg.selectAll('g').data(root.leaves()).join('g').attr('transform', d => 'translate(' + d.x0 + ',' + d.y0 + ')');
    nodes.append('rect')
      .attr('width', d => Math.max(0, d.x1 - d.x0)).attr('height', d => Math.max(0, d.y1 - d.y0))
      .attr('fill', (d, i) => colors[i % colors.length]).attr('rx', 3).attr('opacity', 0.85).style('cursor', 'pointer')
      .on('mouseover', function(evt, d) {
        d3.select(this).attr('opacity', 1);
        const path = [d.parent.parent ? d.parent.parent.data.name : '', d.parent ? d.parent.data.name : '', d.data.name].filter(Boolean).join(' > ');
        showTip(evt, '<b>' + path + '</b><br>Count: ' + fmt(d.value));
      })
      .on('mouseout', function() { d3.select(this).attr('opacity', 0.85); hideTip(); });
    nodes.append('text').attr('x', 4).attr('y', 14).attr('font-size', '10px').attr('fill', '#fff')
      .text(d => d.data.name.length > 14 ? d.data.name.slice(0, 12) + '…' : d.data.name);
  }

  // --- Force Graph ---
  function renderForce(containerId, graph) {
    const el = document.getElementById(containerId);
    el.innerHTML = '';
    if (!graph || !graph.nodes || !graph.nodes.length) { el.innerHTML = '<p style="color:var(--fg2)">No relationship data available</p>'; return; }
    const w = el.clientWidth - 2;
    const h = 400;
    const svg = d3.select('#' + containerId).append('svg').attr('width', w).attr('height', h);
    const groupColors = {};
    let gi = 0;
    graph.nodes.forEach(n => { if (!groupColors[n.group]) { groupColors[n.group] = colors[gi++ % colors.length]; }});
    const sim = d3.forceSimulation(graph.nodes)
      .force('link', d3.forceLink(graph.links).id(d => d.id).distance(80))
      .force('charge', d3.forceManyBody().strength(-120))
      .force('center', d3.forceCenter(w / 2, h / 2));
    const link = svg.append('g').selectAll('line').data(graph.links).join('line')
      .attr('stroke', 'var(--border)').attr('stroke-width', 1.5).attr('stroke-opacity', 0.6);
    const node = svg.append('g').selectAll('circle').data(graph.nodes).join('circle')
      .attr('r', 6).attr('fill', d => groupColors[d.group] || colors[0]).style('cursor', 'pointer')
      .on('mouseover', (evt, d) => showTip(evt, '<b>' + d.id + '</b><br>Type: ' + d.group + '<br>State: ' + d.state))
      .on('mouseout', hideTip)
      .call(d3.drag().on('start', (evt, d) => { if (!evt.active) sim.alphaTarget(0.3).restart(); d.fx = d.x; d.fy = d.y; })
        .on('drag', (evt, d) => { d.fx = evt.x; d.fy = evt.y; })
        .on('end', (evt, d) => { if (!evt.active) sim.alphaTarget(0); d.fx = null; d.fy = null; }));
    sim.on('tick', () => {
      link.attr('x1', d => d.source.x).attr('y1', d => d.source.y).attr('x2', d => d.target.x).attr('y2', d => d.target.y);
      node.attr('cx', d => d.x).attr('cy', d => d.y);
    });
  }

  // --- Data Table ---
  const tableCols = [
    { key: 'name', label: 'Name' }, { key: 'state', label: 'State' }, { key: 'district', label: 'District' },
    { key: 'product', label: 'Product' }, { key: 'entity_type', label: 'Type' }, { key: 'contact', label: 'Contact' },
    { key: 'email', label: 'Email' }, { key: 'market_price', label: 'Price' }
  ];
  let sortKey = '', sortAsc = true;
  function renderTable(rows) {
    document.getElementById('tableHead').innerHTML = tableCols.map(c => '<th data-col="' + c.key + '">' + c.label + '</th>').join('');
    document.querySelectorAll('#tableHead th').forEach(th => {
      th.addEventListener('click', () => { const k = th.dataset.col; if (sortKey === k) sortAsc = !sortAsc; else { sortKey = k; sortAsc = true; } drawRows(rows); });
    });
    drawRows(rows);
  }
  function drawRows(rows) {
    let d = rows.slice();
    if (sortKey) d.sort((a, b) => { const va = a[sortKey] || '', vb = b[sortKey] || ''; return sortAsc ? String(va).localeCompare(String(vb)) : String(vb).localeCompare(String(va)); });
    document.getElementById('tableBody').innerHTML = d.map(r =>
      '<tr>' + tableCols.map(c => '<td>' + (r[c.key] || '') + '</td>').join('') + '</tr>'
    ).join('');
  }
  window.filterTable = function() {
    const q = document.getElementById('tableSearch').value.toLowerCase();
    const rows = currentData.table.filter(r => !q || Object.values(r).some(v => String(v).toLowerCase().includes(q)));
    drawRows(rows);
  };

  // --- Render All ---
  function renderAll(d) {
    renderSummary(d.summary);
    renderBar('barState', d.bar.by_state, colors[0]);
    renderBar('barProduct', d.bar.by_product, colors[1]);
    renderBar('barType', d.bar.by_type, colors[2]);
    renderPie('pieState', d.pie.state, colors);
    renderPie('pieProduct', d.pie.product, colors);
    renderPie('pieType', d.pie.entity_type, colors);
    renderLine('lineChart', d.price_trends);
    renderTreemap('treemap', d.treemap);
    renderForce('forceGraph', d.force);
    renderTable(d.table);
    document.getElementById('footer').textContent = 'Generated: ' + new Date(d.generated_at * 1000).toLocaleString() + ' | Total Entities: ' + fmt(d.summary.total_entities);
  }

  window.refreshData = function() { location.reload(); };

  renderAll(DATA);
})();
</script>
</body>
</html>
"""
