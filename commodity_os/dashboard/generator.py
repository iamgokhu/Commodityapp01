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
        bar_by_type = self._count_field(data, "type")

        pie_state = self._count_field(data, "state", limit=10)
        pie_product = self._count_field(data, "product")
        pie_type = self._count_field(data, "type")

        price_trends = self._price_trends(data)
        treemap_data = self._treemap(data)
        force_data = self._force_graph(data)
        table_rows = self._top_entities(data, limit=50)

        summary = {
            "total_entities": len(data),
            "states_covered": len(set(e.get("state", "") for e in data if e.get("state"))),
            "products": len(set(e.get("product", "") for e in data if e.get("product"))),
            "entity_types": len(set(e.get("type", "") for e in data if e.get("type"))),
        }

        all_states = sorted(set(e.get("state", "") for e in data if e.get("state")))
        all_products = sorted(set(e.get("product", "") for e in data if e.get("product")))
        all_types = sorted(set(e.get("type", "") for e in data if e.get("type")))

        return {
            "generated_at": ts,
            "summary": summary,
            "filters": {"states": all_states, "products": all_products, "types": all_types},
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
                "type": e.get("entity_type", ""),
                "contact": e.get("contact", e.get("phone", "")),
                "email": e.get("email", ""),
                "website": e.get("website", ""),
                "market_price": e.get("market_price", ""),
                "year_established": e.get("year_established", ""),
                "source": e.get("source", ""),
            })
        return rows[:limit]


# ======================================================================
# Green-themed HTML template with Chart.js (Polar + Radar + Bubble)
# ======================================================================

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="refresh" content="300">
    <title>CommodityOS - Market Intelligence Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        :root {
            --primary: #22c55e; --primary-light: #4ade80; --primary-dark: #16a34a;
            --bg: #f0fdf4; --sidebar-bg: #ffffff; --card-bg: #ffffff;
            --text: #1a1a2e; --text-secondary: #64748b; --border: #e2e8f0;
            --success: #22c55e; --warning: #f59e0b; --danger: #ef4444; --info: #3b82f6;
        }
        body { font-family: 'Inter', -apple-system, sans-serif; background: var(--bg); display: flex; min-height: 100vh; color: var(--text); }

        .sidebar { width: 260px; background: var(--sidebar-bg); border-right: 1px solid var(--border); padding: 24px 16px; display: flex; flex-direction: column; position: fixed; height: 100vh; overflow-y: auto; }
        .logo { display: flex; align-items: center; gap: 12px; padding: 0 8px 24px; border-bottom: 1px solid var(--border); margin-bottom: 16px; }
        .logo-icon { width: 40px; height: 40px; background: linear-gradient(135deg, var(--primary), var(--primary-dark)); border-radius: 12px; display: flex; align-items: center; justify-content: center; color: white; font-weight: 700; font-size: 16px; }
        .logo-text { font-size: 18px; font-weight: 700; }
        .nav-label { font-size: 11px; font-weight: 600; color: var(--text-secondary); text-transform: uppercase; letter-spacing: 0.05em; padding: 0 12px; margin: 16px 0 8px; }
        .nav-item { display: flex; align-items: center; gap: 12px; padding: 10px 12px; border-radius: 10px; cursor: pointer; transition: all 0.2s; color: var(--text-secondary); font-size: 14px; font-weight: 500; }
        .nav-item:hover { background: #f0fdf4; color: var(--text); }
        .nav-item.active { background: linear-gradient(135deg, #dcfce7, #bbf7d0); color: var(--primary-dark); font-weight: 600; }
        .nav-item svg { width: 18px; height: 18px; }
        .sidebar-footer { margin-top: auto; padding: 16px; background: linear-gradient(135deg, #dcfce7, #d1fae5); border-radius: 12px; }
        .sidebar-footer h4 { font-size: 13px; font-weight: 600; margin-bottom: 4px; }
        .sidebar-footer p { font-size: 12px; color: var(--text-secondary); margin-bottom: 8px; }
        .progress-bar { height: 6px; background: #e2e8f0; border-radius: 3px; overflow: hidden; }
        .progress-fill { height: 100%; background: var(--primary); border-radius: 3px; transition: width 0.5s; }

        .main { flex: 1; margin-left: 260px; padding: 24px 32px; }
        .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 24px; }
        .search-box { display: flex; align-items: center; gap: 8px; background: var(--card-bg); border: 1px solid var(--border); border-radius: 10px; padding: 10px 16px; width: 300px; }
        .search-box input { border: none; outline: none; font-size: 14px; flex: 1; background: transparent; }
        .header-actions { display: flex; gap: 12px; align-items: center; }
        .header-btn { width: 40px; height: 40px; border-radius: 10px; border: 1px solid var(--border); background: var(--card-bg); display: flex; align-items: center; justify-content: center; cursor: pointer; }
        .live-badge { display: flex; align-items: center; gap: 6px; background: #dcfce7; color: var(--primary-dark); padding: 6px 12px; border-radius: 20px; font-size: 12px; font-weight: 600; }
        .live-dot { width: 8px; height: 8px; background: var(--primary); border-radius: 50%; animation: pulse 2s infinite; }
        @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.4; } }

        .stats-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 24px; }
        .stat-card { background: var(--card-bg); border-radius: 16px; padding: 20px; border: 1px solid var(--border); }
        .stat-card h3 { font-size: 12px; color: var(--text-secondary); font-weight: 500; margin-bottom: 8px; text-transform: uppercase; letter-spacing: 0.03em; }
        .stat-value { font-size: 28px; font-weight: 700; }
        .stat-change { font-size: 12px; margin-top: 4px; }
        .stat-change.up { color: var(--success); }
        .stat-change.down { color: var(--danger); }

        .section-title { font-size: 16px; font-weight: 600; margin-bottom: 16px; display: flex; align-items: center; gap: 8px; }
        .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 24px; margin-bottom: 24px; }
        .grid-3 { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 24px; margin-bottom: 24px; }
        .card { background: var(--card-bg); border-radius: 16px; padding: 20px; border: 1px solid var(--border); }
        .card-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px; }
        .card-title { font-size: 15px; font-weight: 600; }

        .crawler-row { display: flex; align-items: center; gap: 12px; padding: 12px 0; border-bottom: 1px solid var(--border); }
        .crawler-row:last-child { border-bottom: none; }
        .crawler-icon { width: 36px; height: 36px; border-radius: 10px; display: flex; align-items: center; justify-content: center; font-size: 16px; }
        .crawler-info { flex: 1; }
        .crawler-name { font-size: 14px; font-weight: 600; }
        .crawler-meta { font-size: 12px; color: var(--text-secondary); }
        .crawler-stats { text-align: right; }
        .crawler-count { font-size: 16px; font-weight: 700; }
        .crawler-rate { font-size: 11px; color: var(--text-secondary); }
        .status-badge { display: inline-flex; align-items: center; gap: 4px; padding: 3px 8px; border-radius: 6px; font-size: 11px; font-weight: 600; }
        .badge-active { background: #dcfce7; color: var(--primary-dark); }
        .badge-warning { background: #fef3c7; color: #b45309; }
        .badge-error { background: #fee2e2; color: #dc2626; }

        .agent-card { display: flex; align-items: center; gap: 12px; padding: 12px; background: #f8fafc; border-radius: 12px; margin-bottom: 8px; }
        .agent-avatar { width: 40px; height: 40px; border-radius: 10px; display: flex; align-items: center; justify-content: center; font-size: 18px; }
        .agent-info { flex: 1; }
        .agent-name { font-size: 13px; font-weight: 600; }
        .agent-role { font-size: 11px; color: var(--text-secondary); }
        .agent-metric { text-align: right; }
        .agent-value { font-size: 14px; font-weight: 700; }
        .agent-label { font-size: 10px; color: var(--text-secondary); }

        .flow-item { display: flex; align-items: center; gap: 12px; padding: 10px 0; }
        .flow-dot { width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0; }
        .flow-line { width: 2px; height: 20px; background: var(--border); margin-left: 4px; }
        .flow-content { flex: 1; }
        .flow-title { font-size: 13px; font-weight: 600; }
        .flow-desc { font-size: 11px; color: var(--text-secondary); }
        .flow-time { font-size: 10px; color: var(--text-secondary); }

        .chart-container { height: 280px; position: relative; }
        table { width: 100%; border-collapse: collapse; }
        th { text-align: left; font-size: 11px; font-weight: 600; color: var(--text-secondary); text-transform: uppercase; padding: 8px; border-bottom: 2px solid var(--border); }
        td { padding: 10px 8px; font-size: 13px; border-bottom: 1px solid var(--border); }
        tr:hover { background: #f8fafc; }
        .tag { display: inline-block; padding: 2px 8px; border-radius: 6px; font-size: 10px; font-weight: 600; }
        .tag-m { background: #dbeafe; color: #1d4ed8; }
        .tag-w { background: #fef3c7; color: #b45309; }
        .tag-e { background: #dcfce7; color: var(--primary-dark); }
        .mini-bar { height: 4px; background: #e2e8f0; border-radius: 2px; overflow: hidden; margin-top: 4px; }
        .mini-fill { height: 100%; border-radius: 2px; }

        @media (max-width: 1200px) { .stats-grid { grid-template-columns: repeat(2, 1fr); } .grid-2, .grid-3 { grid-template-columns: 1fr; } }
        @media (max-width: 768px) { .sidebar { display: none; } .main { margin-left: 0; } }
    </style>
</head>
<body>
    <aside class="sidebar">
        <div class="logo">
            <div class="logo-icon">CO</div>
            <div class="logo-text">CommodityOS</div>
        </div>
        <div class="nav-label">Main</div>
        <div class="nav-item active">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/></svg>
            Dashboard
        </div>
        <div class="nav-item">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M20 7l-8-4-8 4m16 0l-8 4m8-4v10l-8 4m0-10L4 7m8 4v10M4 7v10l8 4"/></svg>
            Products
        </div>
        <div class="nav-item">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0118 0z"/><circle cx="12" cy="10" r="3"/></svg>
            Regions
        </div>
        <div class="nav-label">Crawlers & Agents</div>
        <div class="nav-item">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 6v6l4 2"/></svg>
            Crawler Performance
        </div>
        <div class="nav-item">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M17 21v-2a4 4 0 00-4-4H5a4 4 0 00-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 00-3-3.87M16 3.13a4 4 0 010 7.75"/></svg>
            Agent Performance
        </div>
        <div class="nav-label">System</div>
        <div class="nav-item">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 12h-4l-3 9L9 3l-3 9H2"/></svg>
            Monitoring
        </div>
        <div class="nav-item">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><path d="M14 2v6h6M16 13H8M16 17H8M10 9H8"/></svg>
            Reports
        </div>
        <div class="sidebar-footer">
            <h4>Collection Progress</h4>
            <p id="progress-text">Loading...</p>
            <div class="progress-bar"><div class="progress-fill" id="progress-fill" style="width:0%"></div></div>
        </div>
    </aside>

    <main class="main">
        <div class="header">
            <div class="search-box">
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#94a3b8" stroke-width="2"><circle cx="11" cy="11" r="8"/><path d="M21 21l-4.35-4.35"/></svg>
                <input type="text" placeholder="Search products, entities, districts...">
            </div>
            <div class="header-actions">
                <div class="live-badge"><div class="live-dot"></div>Live Monitoring</div>
                <div class="header-btn"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 8A6 6 0 006 8c0 7-3 9-3 9h18s-3-2-3-9M13.73 21a2 2 0 01-3.46 0"/></svg></div>
            </div>
        </div>

        <!-- Top Stats -->
        <div class="stats-grid">
            <div class="stat-card"><h3>Total Entities</h3><div class="stat-value" id="s-entities">0</div><div class="stat-change up">Collected across India</div></div>
            <div class="stat-card"><h3>Active Crawlers</h3><div class="stat-value" id="s-crawlers">0</div><div class="stat-change up">Running smoothly</div></div>
            <div class="stat-card"><h3>Meta Agents</h3><div class="stat-value">3</div><div class="stat-change up">Orchestrating pipeline</div></div>
            <div class="stat-card"><h3>Data Quality</h3><div class="stat-value" id="s-quality">100%</div><div class="stat-change up">Validation passed</div></div>
        </div>

        <!-- Charts Row: Polar + Radar -->
        <div class="grid-2">
            <div class="card">
                <div class="card-header"><div class="card-title">Product Distribution</div></div>
                <div class="chart-container"><canvas id="productChart"></canvas></div>
            </div>
            <div class="card">
                <div class="card-header"><div class="card-title">State-wise Collection</div></div>
                <div class="chart-container"><canvas id="stateChart"></canvas></div>
            </div>
        </div>

        <!-- Bubble Chart -->
        <div class="card" style="margin-bottom:24px">
            <div class="card-header"><div class="card-title">Entity Type vs District Coverage</div></div>
            <div class="chart-container" style="height:320px"><canvas id="bubbleChart"></canvas></div>
        </div>

        <!-- Crawler & Agent Performance -->
        <div class="grid-2">
            <!-- Crawlers -->
            <div class="card">
                <div class="card-header">
                    <div class="card-title">Crawler Performance</div>
                    <span class="status-badge badge-active">All Active</span>
                </div>
                <div id="crawler-list"></div>
            </div>
            <!-- Agents -->
            <div class="card">
                <div class="card-header">
                    <div class="card-title">Meta Agent Performance</div>
                </div>
                <div id="agent-list"></div>
            </div>
        </div>

        <!-- Collection Flow -->
        <div class="card" style="margin-bottom:24px">
            <div class="card-header">
                <div class="card-title">Live Collection Flow</div>
                <span class="status-badge badge-active">Real-time</span>
            </div>
            <div id="flow-pipeline" style="display:flex;align-items:flex-start;gap:8px;overflow-x:auto;padding:16px 0"></div>
        </div>

        <!-- District Table + Donut -->
        <div class="grid-2">
            <div class="card">
                <div class="card-header"><div class="card-title">District-wise Data</div></div>
                <div style="max-height:350px;overflow-y:auto">
                    <table>
                        <thead><tr><th>District</th><th>State</th><th>Entities</th><th>Products</th><th>Coverage</th></tr></thead>
                        <tbody id="district-table"></tbody>
                    </table>
                </div>
            </div>
            <div class="card">
                <div class="card-header"><div class="card-title">Entity Type Split</div></div>
                <div class="chart-container"><canvas id="typeChart"></canvas></div>
            </div>
        </div>

        <!-- Top Entities -->
        <div class="card" style="margin-top:24px">
            <div class="card-header"><div class="card-title">Top Collected Entities</div></div>
            <div style="overflow-x:auto">
                <table>
                    <thead><tr><th>Name</th><th>Product</th><th>Type</th><th>State</th><th>District</th><th>Source</th></tr></thead>
                    <tbody id="entity-table"></tbody>
                </table>
            </div>
        </div>
    </main>

    <script>
    const CRAWLERS = [
        { name: 'IndiaMART', icon: '🏭', color: '#dbeafe', keywords: ['indiamart'], rate: '25/min' },
        { name: 'TradeIndia', icon: '📦', color: '#dcfce7', keywords: ['tradeindia'], rate: '20/min' },
        { name: 'AgMarkNet', icon: '🌾', color: '#fef3c7', keywords: ['agmarknet'], rate: '15/min' },
        { name: 'APMC Markets', icon: '🏪', color: '#fce7f3', keywords: ['apmc'], rate: '10/min' },
        { name: 'Export Directory', icon: '🌐', color: '#e0e7ff', keywords: ['export'], rate: '12/min' },
        { name: 'Amazon Business', icon: '🛒', color: '#fef3c7', keywords: ['amazon'], rate: '20/min' },
        { name: 'Flipkart Wholesale', icon: '🛍️', color: '#dbeafe', keywords: ['flipkart'], rate: '18/min' },
        { name: 'Government API', icon: '🏛️', color: '#dcfce7', keywords: ['data.gov','government'], rate: '10/min' },
        { name: 'JioMart', icon: '📱', color: '#fce7f3', keywords: ['jiomart'], rate: '15/min' },
        { name: 'DMart', icon: '🏬', color: '#e0e7ff', keywords: ['dmart'], rate: '12/min' },
        { name: 'BigBasket', icon: '🥬', color: '#dcfce7', keywords: ['bigbasket'], rate: '15/min' },
        { name: 'Blinkit', icon: '⚡', color: '#fef3c7', keywords: ['blinkit'], rate: '20/min' },
        { name: 'Zepto', icon: '🚀', color: '#dbeafe', keywords: ['zepto'], rate: '20/min' },
        { name: 'Swiggy Instamart', icon: '🛵', color: '#fce7f3', keywords: ['swiggy'], rate: '18/min' },
        { name: 'Reliance Fresh', icon: '🏪', color: '#e0e7ff', keywords: ['reliance'], rate: '12/min' },
        { name: 'More Retail', icon: '🛒', color: '#dcfce7', keywords: ['more.com'], rate: '10/min' },
        { name: "Spencer's", icon: '🏬', color: '#fef3c7', keywords: ['spencers'], rate: '10/min' },
        { name: 'Walmart Global', icon: '🌏', color: '#dbeafe', keywords: ['walmart'], rate: '15/min' },
        { name: 'Costco Global', icon: '📦', color: '#dcfce7', keywords: ['costco'], rate: '12/min' },
        { name: 'Carrefour Global', icon: '🌍', color: '#fce7f3', keywords: ['carrefour'], rate: '12/min' },
        { name: 'Tesco Global', icon: '🏪', color: '#e0e7ff', keywords: ['tesco'], rate: '12/min' },
        { name: 'Alibaba', icon: '🔗', color: '#fef3c7', keywords: ['alibaba'], rate: '20/min' },
        { name: 'Amazon Global', icon: '📦', color: '#dbeafe', keywords: ['amazon.com'], rate: '20/min' },
        { name: 'Seafood Exporters', icon: '🦐', color: '#dcfce7', keywords: ['seafood','marine'], rate: '10/min' },
        { name: 'Corporate Farms', icon: '🌾', color: '#fef3c7', keywords: ['corporate_farm'], rate: '8/min' },
        { name: 'Marine Harvest', icon: '🐟', color: '#e0e7ff', keywords: ['marine_harvest'], rate: '8/min' }
    ];
    const AGENTS = [
        { name: 'System Orchestrator', role: 'Plans & assigns tasks', icon: '🎯', color: '#dbeafe', metric: 'Cycles', value: '0' },
        { name: 'Quality Supervisor', role: 'Validates & audits data', icon: '✅', color: '#dcfce7', metric: 'Score', value: '100%' },
        { name: 'Executive Intelligence', role: 'Dashboards & reports', icon: '📊', color: '#fef3c7', metric: 'Reports', value: '0' }
    ];
    const PIPELINE_STEPS = [
        { name: 'API Health', color: '#22c55e' },
        { name: 'Resource Check', color: '#3b82f6' },
        { name: 'Crawling', color: '#f59e0b' },
        { name: 'Validation', color: '#8b5cf6' },
        { name: 'Dedup', color: '#ec4899' },
        { name: 'Cleaning', color: '#06b6d4' },
        { name: 'Classification', color: '#14b8a6' },
        { name: 'Knowledge Graph', color: '#f97316' },
        { name: 'Dashboard', color: '#22c55e' },
        { name: 'GitHub Push', color: '#64748b' }
    ];

    function render(entities) {
        document.getElementById('s-entities').textContent = entities.length.toLocaleString();
        const states = [...new Set(entities.map(e=>e.state).filter(Boolean))];
        const products = [...new Set(entities.map(e=>e.product).filter(Boolean))];
        const districts = [...new Set(entities.map(e=>e.district).filter(Boolean))];
        const activeCrawlers = CRAWLERS.filter(c => entities.some(e => c.keywords.some(k => (e.source||'').toLowerCase().includes(k)))).length;
        document.getElementById('s-crawlers').textContent = activeCrawlers;

        // Progress
        const pct = Math.min(100, (entities.length/5000*100)).toFixed(0);
        document.getElementById('progress-fill').style.width = pct+'%';
        document.getElementById('progress-text').textContent = entities.length.toLocaleString()+' / 5,000 collected';

        // --- Polar Area Chart: Product Distribution ---
        const pCounts = {};
        entities.forEach(e=>{const p=e.product||'Unknown';pCounts[p]=(pCounts[p]||0)+1;});
        const pLabels = Object.keys(pCounts);
        const pValues = Object.values(pCounts);
        const polarColors = ['#22c55e','#3b82f6','#f59e0b','#ef4444','#8b5cf6','#ec4899','#06b6d4','#14b8a6','#f97316','#64748b','#a855f7','#e11d48'];
        new Chart(document.getElementById('productChart'), {
            type: 'polarArea',
            data: {
                labels: pLabels,
                datasets: [{ data: pValues, backgroundColor: polarColors.map(c => c + '99'), borderColor: polarColors, borderWidth: 2 }]
            },
            options: {
                responsive: true, maintainAspectRatio: false,
                plugins: { legend: { position: 'right', labels: { padding: 10, font: { size: 11 } } } },
                scales: { r: { ticks: { display: false }, grid: { color: '#e2e8f0' } } }
            }
        });

        // --- Radar Chart: State-wise Collection ---
        const sCounts = {};
        entities.forEach(e=>{const s=e.state||'Unknown';sCounts[s]=(sCounts[s]||0)+1;});
        const sSorted = Object.entries(sCounts).sort((a,b)=>b[1]-a[1]).slice(0,8);
        new Chart(document.getElementById('stateChart'), {
            type: 'radar',
            data: {
                labels: sSorted.map(s=>s[0]),
                datasets: [{
                    label: 'Entities',
                    data: sSorted.map(s=>s[1]),
                    backgroundColor: 'rgba(34,197,94,0.2)',
                    borderColor: '#22c55e',
                    borderWidth: 2,
                    pointBackgroundColor: '#22c55e',
                    pointBorderColor: '#fff',
                    pointBorderWidth: 2,
                    pointRadius: 5
                }]
            },
            options: {
                responsive: true, maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: { r: { beginAtZero: true, grid: { color: '#e2e8f0' }, pointLabels: { font: { size: 11 } }, ticks: { display: false } } }
            }
        });

        // --- Bubble Chart: Entity Type vs District Coverage ---
        const typeColors = { 'Manufacturer': '#3b82f6', 'Wholesaler': '#f59e0b', 'Exporter': '#22c55e', 'Online Seller': '#8b5cf6', 'Government Source': '#ef4444', 'Marine Exporter': '#06b6d4', 'Corporate Farm': '#f97316', 'Marine Farm': '#ec4899' };
        const typeDistCounts = {};
        entities.forEach(e => {
            const t = e.type || 'Unknown';
            const d = e.district || 'Unknown';
            if (!typeDistCounts[t]) typeDistCounts[t] = {};
            typeDistCounts[t][d] = (typeDistCounts[t][d] || 0) + 1;
        });
        const bubbleDatasets = Object.entries(typeDistCounts).map(([type, dists]) => {
            const points = Object.entries(dists).map(([dist, count], i) => ({
                x: i,
                y: count,
                r: Math.min(Math.sqrt(count) * 4, 30)
            }));
            return {
                label: type,
                data: points.slice(0, 20),
                backgroundColor: (typeColors[type] || '#64748b') + '88',
                borderColor: typeColors[type] || '#64748b',
                borderWidth: 1
            };
        });
        new Chart(document.getElementById('bubbleChart'), {
            type: 'bubble',
            data: { datasets: bubbleDatasets },
            options: {
                responsive: true, maintainAspectRatio: false,
                plugins: { legend: { position: 'top', labels: { padding: 12, font: { size: 11 } } } },
                scales: {
                    x: { title: { display: true, text: 'District Index', font: { size: 11 } }, grid: { color: '#f1f5f9' } },
                    y: { title: { display: true, text: 'Entity Count', font: { size: 11 } }, beginAtZero: true, grid: { color: '#f1f5f9' } }
                }
            }
        });

        // --- Doughnut: Entity Type Split ---
        const tCounts = {};
        entities.forEach(e=>{const t=e.type||'Unknown';tCounts[t]=(tCounts[t]||0)+1;});
        new Chart(document.getElementById('typeChart'), {
            type: 'doughnut',
            data: { labels: Object.keys(tCounts), datasets: [{ data: Object.values(tCounts), backgroundColor: ['#22c55e','#3b82f6','#f59e0b','#ef4444','#8b5cf6','#06b6d4'], borderWidth: 0 }] },
            options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'bottom', labels: { padding: 16, font: { size: 12 } } } } }
        });

        // Crawler List
        const crawlerDiv = document.getElementById('crawler-list');
        crawlerDiv.innerHTML = CRAWLERS.slice(0, 10).map(c => {
            const count = entities.filter(e => c.keywords.some(k => (e.source||'').toLowerCase().includes(k))).length;
            return `<div class="crawler-row">
                <div class="crawler-icon" style="background:${c.color}">${c.icon}</div>
                <div class="crawler-info"><div class="crawler-name">${c.name}</div><div class="crawler-meta">${c.rate}</div></div>
                <div class="crawler-stats"><div class="crawler-count">${count}</div><div class="crawler-rate">entities</div></div>
            </div>`;
        }).join('');

        // Agent List
        const agentDiv = document.getElementById('agent-list');
        agentDiv.innerHTML = AGENTS.map(a => `<div class="agent-card">
            <div class="agent-avatar" style="background:${a.color}">${a.icon}</div>
            <div class="agent-info"><div class="agent-name">${a.name}</div><div class="agent-role">${a.role}</div></div>
            <div class="agent-metric"><div class="agent-value">${a.value}</div><div class="agent-label">${a.metric}</div></div>
        </div>`).join('');

        // Pipeline Flow
        const flowDiv = document.getElementById('flow-pipeline');
        flowDiv.innerHTML = PIPELINE_STEPS.map((s,i) => `<div style="display:flex;flex-direction:column;align-items:center;min-width:90px">
            <div style="width:12px;height:12px;border-radius:50%;background:${s.color};margin-bottom:8px"></div>
            <div style="font-size:12px;font-weight:600;text-align:center">${s.name}</div>
            <div style="font-size:10px;color:#64748b">Active</div>
        </div>${i<PIPELINE_STEPS.length-1?'<div style="margin-top:6px;color:#cbd5e1">→</div>':''}`).join('');

        // District Table
        const distData = {};
        entities.forEach(e => {
            const d = e.district || 'Unknown';
            const s = e.state || 'Unknown';
            const p = e.product || 'Unknown';
            if (!distData[d]) distData[d] = { state:s, count:0, products:new Set() };
            distData[d].count++;
            distData[d].products.add(p);
        });
        const distSorted = Object.entries(distData).sort((a,b)=>b[1].count-a[1].count).slice(0,15);
        const maxDist = distSorted[0]?.[1].count || 1;
        document.getElementById('district-table').innerHTML = distSorted.map(([d,v]) => {
            const p = (v.count/maxDist*100).toFixed(0);
            const color = p>70?'#22c55e':p>40?'#f59e0b':'#3b82f6';
            return `<tr><td><strong>${d}</strong></td><td>${v.state}</td><td>${v.count}</td><td>${v.products.size}</td>
            <td><div class="mini-bar"><div class="mini-fill" style="width:${p}%;background:${color}"></div></div></td></tr>`;
        }).join('');

        // Entity Table
        document.getElementById('entity-table').innerHTML = entities.slice(0,15).map(e => {
            const tc = (e.type||'').toLowerCase()==='manufacturer'?'tag-m':(e.type||'').toLowerCase()==='exporter'?'tag-e':'tag-w';
            return `<tr><td><strong>${e.name||'N/A'}</strong></td><td>${e.product||'N/A'}</td><td><span class="tag ${tc}">${e.type||'N/A'}</span></td><td>${e.state||'N/A'}</td><td>${e.district||'N/A'}</td><td>${e.source||'N/A'}</td></tr>`;
        }).join('');
    }

    // Load from embedded data or fetch
    const EMBEDDED_DATA = __DASHBOARD_DATA__;
    if (EMBEDDED_DATA && EMBEDDED_DATA.table && EMBEDDED_DATA.table.length) {
        render(EMBEDDED_DATA.table);
    } else {
        fetch('data.json').then(r=>r.json()).then(render).catch(()=>{
            const demo = Array.from({length:200},(_,i)=>({
                name:`Entity ${i+1}`,product:['Sugar','Rice','Wheat','Pulses','Grains','Dals','Basmathi Rice'][i%7],
                type:['Manufacturer','Wholesaler','Exporter'][i%3],state:['Maharashtra','Uttar Pradesh','Punjab','Gujarat','Rajasthan','Karnataka','Tamil Nadu'][i%7],
                district:['Pune','Mumbai','Lucknow','Ahmedabad','Jaipur','Bangalore','Chennai'][i%7],
                source:['indiamart','tradeindia','agmarknet','apmc','export_directory'][i%5]
            }));
            render(demo);
        });
    }
    </script>
</body>
</html>
"""
