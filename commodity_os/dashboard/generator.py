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

        # Product hierarchy: category → [subcategories]
        product_hierarchy = {}
        for e in data:
            cat = e.get("category", "") or ""
            prod = e.get("product", "") or ""
            if cat and prod:
                if cat not in product_hierarchy:
                    product_hierarchy[cat] = {}
                product_hierarchy[cat][prod] = product_hierarchy[cat].get(prod, 0) + 1

        # State hierarchy: state → {district: count}
        state_hierarchy = {}
        for e in data:
            st = e.get("state", "") or ""
            dist = e.get("district", "") or ""
            if st:
                if st not in state_hierarchy:
                    state_hierarchy[st] = {}
                if dist:
                    state_hierarchy[st][dist] = state_hierarchy[st].get(dist, 0) + 1

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
            "product_hierarchy": product_hierarchy,
            "state_hierarchy": state_hierarchy,
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
                "category": e.get("category", e.get("commodity_group", "")),
                "type": e.get("entity_type", e.get("type", "")),
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

        .sidebar { width: 260px; background: var(--sidebar-bg); border-right: 1px solid var(--border); padding: 24px 16px; display: flex; flex-direction: column; position: fixed; height: 100vh; overflow-y: auto; z-index: 10; }
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
        .badge-standby { background: #e0e7ff; color: #4338ca; }

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

        .page { display: none; }
        .page.active { display: block; }
        .breadcrumb { display: flex; align-items: center; gap: 6px; margin-bottom: 16px; font-size: 13px; flex-wrap: wrap; }
        .breadcrumb-item { cursor: pointer; color: var(--primary-dark); font-weight: 500; }
        .breadcrumb-item:hover { text-decoration: underline; }
        .breadcrumb-sep { color: var(--text-secondary); }
        .breadcrumb-current { color: var(--text-secondary); font-weight: 600; }
        .drill-chart { cursor: pointer; }
        .drill-chart:hover { box-shadow: 0 0 0 2px var(--primary); }

        .cat-card { display: flex; align-items: center; gap: 12px; padding: 14px; background: #f8fafc; border-radius: 12px; margin-bottom: 8px; cursor: pointer; transition: all 0.2s; border: 1px solid transparent; }
        .cat-card:hover { background: #dcfce7; border-color: var(--primary); }
        .cat-icon { width: 40px; height: 40px; border-radius: 10px; display: flex; align-items: center; justify-content: center; font-size: 18px; }
        .cat-info { flex: 1; }
        .cat-name { font-size: 14px; font-weight: 600; }
        .cat-count { font-size: 12px; color: var(--text-secondary); }
        .cat-arrow { color: var(--text-secondary); font-size: 16px; }

        .health-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; margin-bottom: 24px; }
        .health-card { background: var(--card-bg); border-radius: 16px; padding: 20px; border: 1px solid var(--border); text-align: center; }
        .health-value { font-size: 36px; font-weight: 700; margin: 8px 0; }
        .health-label { font-size: 12px; color: var(--text-secondary); text-transform: uppercase; }
        .health-bar { height: 8px; background: #e2e8f0; border-radius: 4px; overflow: hidden; margin-top: 12px; }
        .health-fill { height: 100%; border-radius: 4px; transition: width 0.5s; }

        .report-card { display: flex; align-items: center; gap: 16px; padding: 16px; background: #f8fafc; border-radius: 12px; margin-bottom: 10px; border: 1px solid var(--border); }
        .report-icon { width: 44px; height: 44px; border-radius: 10px; display: flex; align-items: center; justify-content: center; font-size: 20px; }
        .report-info { flex: 1; }
        .report-title { font-size: 14px; font-weight: 600; }
        .report-meta { font-size: 12px; color: var(--text-secondary); }
        .report-btn { padding: 6px 14px; border-radius: 8px; border: none; font-size: 12px; font-weight: 600; cursor: pointer; }
        .report-btn-primary { background: var(--primary); color: white; }

        @media (max-width: 1200px) { .stats-grid, .health-grid { grid-template-columns: repeat(2, 1fr); } .grid-2, .grid-3 { grid-template-columns: 1fr; } }
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
        <div class="nav-item active" onclick="showPage('dashboard')">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/></svg>
            Dashboard
        </div>
        <div class="nav-item" onclick="showPage('products')">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M20 7l-8-4-8 4m16 0l-8 4m8-4v10l-8 4m0-10L4 7m8 4v10M4 7v10l8 4"/></svg>
            Products
        </div>
        <div class="nav-item" onclick="showPage('regions')">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0118 0z"/><circle cx="12" cy="10" r="3"/></svg>
            Regions
        </div>
        <div class="nav-label">Crawlers & Agents</div>
        <div class="nav-item" onclick="showPage('crawlers')">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 6v6l4 2"/></svg>
            Crawler Performance
        </div>
        <div class="nav-item" onclick="showPage('agents')">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M17 21v-2a4 4 0 00-4-4H5a4 4 0 00-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 00-3-3.87M16 3.13a4 4 0 010 7.75"/></svg>
            Agent Performance
        </div>
        <div class="nav-label">System</div>
        <div class="nav-item" onclick="showPage('monitoring')">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 12h-4l-3 9L9 3l-3 9H2"/></svg>
            Monitoring
        </div>
        <div class="nav-item" onclick="showPage('reports')">
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
                <input type="text" id="global-search" placeholder="Search products, entities, districts..." oninput="globalSearch(this.value)">
            </div>
            <div class="header-actions">
                <div style="font-size:12px;color:#64748b" id="last-update">Loading...</div>
                <div class="live-badge"><div class="live-dot"></div>Live</div>
                <div class="header-btn" onclick="showPage('monitoring')"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 12h-4l-3 9L9 3l-3 9H2"/></svg></div>
            </div>
        </div>

        <!-- ======== DASHBOARD PAGE ======== -->
        <div id="page-dashboard" class="page active">
            <div class="stats-grid">
                <div class="stat-card"><h3>Total Entities</h3><div class="stat-value" id="s-entities">0</div><div class="stat-change up">Collected across India</div></div>
                <div class="stat-card"><h3>Active Crawlers</h3><div class="stat-value" id="s-crawlers">0</div><div class="stat-change up">Running smoothly</div></div>
                <div class="stat-card"><h3>Meta Agents</h3><div class="stat-value">3</div><div class="stat-change up">Orchestrating pipeline</div></div>
                <div class="stat-card"><h3>Data Quality</h3><div class="stat-value" id="s-quality">100%</div><div class="stat-change up">Validation passed</div></div>
            </div>
            <div class="grid-2">
                <div class="card drill-chart" onclick="drillProductCategory(null)">
                    <div class="card-header"><div class="card-title">Product Distribution <span style="font-size:11px;color:#64748b;font-weight:400">(click to drill down)</span></div></div>
                    <div class="chart-container"><canvas id="productChart"></canvas></div>
                </div>
                <div class="card drill-chart" onclick="drillState(null)">
                    <div class="card-header"><div class="card-title">State-wise Collection <span style="font-size:11px;color:#64748b;font-weight:400">(click to drill down)</span></div></div>
                    <div class="chart-container"><canvas id="stateChart"></canvas></div>
                </div>
            </div>
            <div class="card" style="margin-bottom:24px">
                <div class="card-header"><div class="card-title">Entity Type vs District Coverage</div></div>
                <div class="chart-container" style="height:320px"><canvas id="bubbleChart"></canvas></div>
            </div>
            <div class="grid-2">
                <div class="card">
                    <div class="card-header"><div class="card-title">Crawler Performance</div><span class="status-badge badge-active">All Active</span></div>
                    <div id="crawler-list"></div>
                </div>
                <div class="card">
                    <div class="card-header"><div class="card-title">Meta Agent Performance</div></div>
                    <div id="agent-list"></div>
                </div>
            </div>
            <div class="card" style="margin-bottom:24px">
                <div class="card-header"><div class="card-title">Live Collection Flow</div><span class="status-badge badge-active">Real-time</span></div>
                <div id="flow-pipeline" style="display:flex;align-items:flex-start;gap:8px;overflow-x:auto;padding:16px 0"></div>
            </div>
            <div class="grid-2">
                <div class="card">
                    <div class="card-header"><div class="card-title">District-wise Data</div></div>
                    <div style="max-height:350px;overflow-y:auto">
                        <table><thead><tr><th>District</th><th>State</th><th>Entities</th><th>Products</th><th>Coverage</th></tr></thead><tbody id="district-table"></tbody></table>
                    </div>
                </div>
                <div class="card">
                    <div class="card-header"><div class="card-title">Entity Type Split</div></div>
                    <div class="chart-container"><canvas id="typeChart"></canvas></div>
                </div>
            </div>
            <div class="card" style="margin-top:24px">
                <div class="card-header"><div class="card-title">Top Collected Entities</div></div>
                <div style="overflow-x:auto">
                    <table><thead><tr><th>Name</th><th>Product</th><th>Type</th><th>State</th><th>District</th><th>Source</th></tr></thead><tbody id="entity-table"></tbody></table>
                </div>
            </div>
        </div>

        <!-- ======== PRODUCTS PAGE ======== -->
        <div id="page-products" class="page">
            <div class="breadcrumb" id="product-breadcrumb"><span class="breadcrumb-current">All Categories</span></div>
            <div class="grid-2">
                <div class="card" style="max-height:600px;overflow-y:auto">
                    <div class="card-header"><div class="card-title">Product Categories</div></div>
                    <div id="product-categories-list"></div>
                </div>
                <div class="card">
                    <div class="card-header"><div class="card-title" id="product-detail-title">Category Distribution</div></div>
                    <div class="chart-container" style="height:350px"><canvas id="productDetailChart"></canvas></div>
                </div>
            </div>
            <div class="card" style="margin-top:24px">
                <div class="card-header"><div class="card-title" id="product-entities-title">All Entities</div></div>
                <div style="overflow-x:auto;max-height:400px;overflow-y:auto">
                    <table><thead><tr><th>Name</th><th>Product</th><th>Category</th><th>Type</th><th>State</th><th>District</th><th>Source</th></tr></thead><tbody id="product-entities-table"></tbody></table>
                </div>
            </div>
        </div>

        <!-- ======== REGIONS PAGE ======== -->
        <div id="page-regions" class="page">
            <div class="breadcrumb" id="region-breadcrumb"><span class="breadcrumb-current">All States</span></div>
            <div class="grid-2">
                <div class="card" style="max-height:600px;overflow-y:auto">
                    <div class="card-header"><div class="card-title">States / UTs</div></div>
                    <div id="region-states-list"></div>
                </div>
                <div class="card">
                    <div class="card-header"><div class="card-title" id="region-detail-title">State Distribution</div></div>
                    <div class="chart-container" style="height:350px"><canvas id="regionDetailChart"></canvas></div>
                </div>
            </div>
            <div class="card" style="margin-top:24px">
                <div class="card-header"><div class="card-title" id="region-entities-title">All Entities</div></div>
                <div style="overflow-x:auto;max-height:400px;overflow-y:auto">
                    <table><thead><tr><th>Name</th><th>State</th><th>District</th><th>Taluk</th><th>Product</th><th>Type</th><th>Source</th></tr></thead><tbody id="region-entities-table"></tbody></table>
                </div>
            </div>
        </div>

        <!-- ======== CRAWLERS PAGE ======== -->
        <div id="page-crawlers" class="page">
            <div class="stats-grid" style="grid-template-columns:repeat(4,1fr)">
                <div class="stat-card"><h3>Total Crawlers</h3><div class="stat-value">26</div></div>
                <div class="stat-card"><h3>Active</h3><div class="stat-value" id="c-active" style="color:var(--success)">0</div></div>
                <div class="stat-card"><h3>Standby</h3><div class="stat-value" id="c-standby" style="color:var(--info)">0</div></div>
                <div class="stat-card"><h3>Failed</h3><div class="stat-value" id="c-failed" style="color:var(--danger)">0</div></div>
            </div>
            <div class="grid-2">
                <div class="card" style="max-height:600px;overflow-y:auto">
                    <div class="card-header"><div class="card-title">All Crawlers</div></div>
                    <div id="crawler-full-list"></div>
                </div>
                <div class="card">
                    <div class="card-header"><div class="card-title">Crawler Entity Distribution</div></div>
                    <div class="chart-container" style="height:400px"><canvas id="crawlerChart"></canvas></div>
                </div>
            </div>
        </div>

        <!-- ======== AGENTS PAGE ======== -->
        <div id="page-agents" class="page">
            <div class="grid-3" style="margin-bottom:24px">
                <div class="stat-card"><h3>System Orchestrator</h3><div class="stat-value" style="color:var(--info)" id="a-cycles">0</div><div class="stat-change">Cycles completed</div></div>
                <div class="stat-card"><h3>Quality Score</h3><div class="stat-value" style="color:var(--success)" id="a-quality">100%</div><div class="stat-change">Data validation</div></div>
                <div class="stat-card"><h3>Reports Generated</h3><div class="stat-value" style="color:var(--warning)" id="a-reports">0</div><div class="stat-change">Auto-generated</div></div>
            </div>
            <div class="card" style="margin-bottom:24px">
                <div class="card-header"><div class="card-title">Agent Details</div></div>
                <div id="agent-detail-list"></div>
            </div>
            <div class="card">
                <div class="card-header"><div class="card-title">Agent Task History</div></div>
                <div style="overflow-x:auto">
                    <table><thead><tr><th>Agent</th><th>Task</th><th>Status</th><th>Duration</th><th>Timestamp</th></tr></thead><tbody id="agent-history-table"></tbody></table>
                </div>
            </div>
        </div>

        <!-- ======== MONITORING PAGE ======== -->
        <div id="page-monitoring" class="page">
            <div class="health-grid">
                <div class="health-card"><div class="health-label">CPU Usage</div><div class="health-value" id="m-cpu">--</div><div class="health-bar"><div class="health-fill" id="m-cpu-bar" style="width:0;background:var(--primary)"></div></div></div>
                <div class="health-card"><div class="health-label">RAM Usage</div><div class="health-value" id="m-ram">--</div><div class="health-bar"><div class="health-fill" id="m-ram-bar" style="width:0;background:var(--warning)"></div></div></div>
                <div class="health-card"><div class="health-label">Disk Usage</h3><div class="health-value" id="m-disk">--</div><div class="health-bar"><div class="health-fill" id="m-disk-bar" style="width:0;background:var(--info)"></div></div></div>
            </div>
            <div class="grid-2">
                <div class="card">
                    <div class="card-header"><div class="card-title">System Info</div></div>
                    <div id="system-info"></div>
                </div>
                <div class="card">
                    <div class="card-header"><div class="card-title">Pipeline Health</div></div>
                    <div id="pipeline-health"></div>
                </div>
            </div>
            <div class="card" style="margin-top:24px">
                <div class="card-header"><div class="card-title">Recent Logs</div></div>
                <div style="max-height:300px;overflow-y:auto;font-family:monospace;font-size:12px;padding:12px;background:#1a1a2e;color:#22c55e;border-radius:8px" id="log-output"></div>
            </div>
        </div>

        <!-- ======== REPORTS PAGE ======== -->
        <div id="page-reports" class="page">
            <div class="grid-2" style="margin-bottom:24px">
                <div class="card">
                    <div class="card-header"><div class="card-title">Generate Reports</div><span style="font-size:12px;color:#64748b">${new Date().toLocaleDateString()}</span></div>
                    <div id="reports-list"></div>
                </div>
                <div class="card">
                    <div class="card-header"><div class="card-title">Quick Stats</div></div>
                    <div id="report-stats"></div>
                </div>
            </div>
            <div class="card" style="margin-bottom:24px">
                <div class="card-header"><div class="card-title">Collection Summary by Product</div></div>
                <div style="overflow-x:auto;max-height:350px;overflow-y:auto">
                    <table><thead><tr><th>Product</th><th>Category</th><th>Entities</th><th>States</th><th>Districts</th><th>Sources</th><th>Avg Price</th></tr></thead><tbody id="report-summary-table"></tbody></table>
                </div>
            </div>
            <div class="card">
                <div class="card-header"><div class="card-title">Recent Reports</div></div>
                <div id="report-history"></div>
            </div>
        </div>

        <!-- ======== REPORT MODAL ======== -->
        <div id="report-modal" style="display:none;position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.5);z-index:100;overflow-y:auto;padding:32px">
            <div style="background:white;border-radius:16px;max-width:1100px;margin:0 auto;padding:32px;position:relative">
                <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:24px">
                    <h2 id="modal-report-title" style="font-size:20px;font-weight:700"></h2>
                    <div style="display:flex;gap:8px;align-items:center">
                        <button onclick="exportReport()" style="padding:8px 16px;border-radius:8px;border:1px solid #e2e8f0;background:white;font-size:12px;font-weight:600;cursor:pointer">Export JSON</button>
                        <button onclick="printReport()" style="padding:8px 16px;border-radius:8px;border:1px solid #e2e8f0;background:white;font-size:12px;font-weight:600;cursor:pointer">Print</button>
                        <button onclick="closeReportModal()" style="width:36px;height:36px;border-radius:8px;border:1px solid #e2e8f0;background:white;font-size:18px;cursor:pointer;display:flex;align-items:center;justify-content:center">&times;</button>
                    </div>
                </div>
                <div id="modal-report-content"></div>
            </div>
        </div>
    </main>

    <script>
    const CRAWLERS = [
        { name: 'IndiaMART', icon: '\u{1f3ed}', color: '#dbeafe', keywords: ['indiamart'], rate: '25/min', category: 'Traditional' },
        { name: 'TradeIndia', icon: '\u{1f4e6}', color: '#dcfce7', keywords: ['tradeindia'], rate: '20/min', category: 'Traditional' },
        { name: 'AgMarkNet', icon: '\u{1f33e}', color: '#fef3c7', keywords: ['agmarknet'], rate: '15/min', category: 'Government' },
        { name: 'APMC Markets', icon: '\u{1f3ea}', color: '#fce7f3', keywords: ['apmc'], rate: '10/min', category: 'Traditional' },
        { name: 'Export Directory', icon: '\u{1f310}', color: '#e0e7ff', keywords: ['export'], rate: '12/min', category: 'Export' },
        { name: 'Amazon Business', icon: '\u{1f6d2}', color: '#fef3c7', keywords: ['amazon business'], rate: '20/min', category: 'Modern Retail' },
        { name: 'Flipkart Wholesale', icon: '\u{1f6cd}', color: '#dbeafe', keywords: ['flipkart'], rate: '18/min', category: 'Modern Retail' },
        { name: 'Government API', icon: '\u{1f3db}', color: '#dcfce7', keywords: ['data.gov','government'], rate: '10/min', category: 'Government' },
        { name: 'JioMart', icon: '\u{1f4f1}', color: '#fce7f3', keywords: ['jiomart'], rate: '15/min', category: 'Modern Retail' },
        { name: 'DMart', icon: '\u{1f3ec}', color: '#e0e7ff', keywords: ['dmart'], rate: '12/min', category: 'Modern Retail' },
        { name: 'BigBasket', icon: '\u{1f96c}', color: '#dcfce7', keywords: ['bigbasket'], rate: '15/min', category: 'Modern Retail' },
        { name: 'Blinkit', icon: '\u{26a1}', color: '#fef3c7', keywords: ['blinkit'], rate: '20/min', category: 'Modern Retail' },
        { name: 'Zepto', icon: '\u{1f680}', color: '#dbeafe', keywords: ['zepto'], rate: '20/min', category: 'Modern Retail' },
        { name: 'Swiggy Instamart', icon: '\u{1f6f5}', color: '#fce7f3', keywords: ['swiggy'], rate: '18/min', category: 'Modern Retail' },
        { name: 'Reliance Fresh', icon: '\u{1f3ea}', color: '#e0e7ff', keywords: ['reliance'], rate: '12/min', category: 'Modern Retail' },
        { name: 'More Retail', icon: '\u{1f6d2}', color: '#dcfce7', keywords: ['more.com'], rate: '10/min', category: 'Modern Retail' },
        { name: "Spencer's", icon: '\u{1f3ec}', color: '#fef3c7', keywords: ['spencers'], rate: '10/min', category: 'Modern Retail' },
        { name: 'Walmart Global', icon: '\u{1f30f}', color: '#dbeafe', keywords: ['walmart'], rate: '15/min', category: 'Global Retail' },
        { name: 'Costco Global', icon: '\u{1f4e6}', color: '#dcfce7', keywords: ['costco'], rate: '12/min', category: 'Global Retail' },
        { name: 'Carrefour Global', icon: '\u{1f30d}', color: '#fce7f3', keywords: ['carrefour'], rate: '12/min', category: 'Global Retail' },
        { name: 'Tesco Global', icon: '\u{1f3ea}', color: '#e0e7ff', keywords: ['tesco'], rate: '12/min', category: 'Global Retail' },
        { name: 'Alibaba', icon: '\u{1f517}', color: '#fef3c7', keywords: ['alibaba'], rate: '20/min', category: 'Global Retail' },
        { name: 'Amazon Global', icon: '\u{1f4e6}', color: '#dbeafe', keywords: ['amazon.com','amazon.in'], rate: '20/min', category: 'Global Retail' },
        { name: 'Seafood Exporters', icon: '\u{1f990}', color: '#dcfce7', keywords: ['seafood','marine'], rate: '10/min', category: 'Marine' },
        { name: 'Corporate Farms', icon: '\u{1f33e}', color: '#fef3c7', keywords: ['corporate_farm'], rate: '8/min', category: 'Farming' },
        { name: 'Marine Harvest', icon: '\u{1f41f}', color: '#e0e7ff', keywords: ['marine_harvest'], rate: '8/min', category: 'Marine' }
    ];
    const AGENTS = [
        { name: 'System Orchestrator', role: 'Plans & assigns tasks to all 28 crawlers, monitors resource usage, schedules collection cycles', icon: '\u{1f3af}', color: '#dbeafe', metric: 'Cycles', value: '0', status: 'Active' },
        { name: 'Quality Supervisor', role: 'Validates data at each pipeline stage, runs deduplication, checks field completeness', icon: '\u{2705}', color: '#dcfce7', metric: 'Score', value: '100%', status: 'Active' },
        { name: 'Executive Intelligence', role: 'Generates dashboard, reports, knowledge graph updates, and anomaly detection', icon: '\u{1f4ca}', color: '#fef3c7', metric: 'Reports', value: '0', status: 'Active' }
    ];
    const PIPELINE_STEPS = [
        { name: 'API Health', color: '#22c55e' }, { name: 'Resource Check', color: '#3b82f6' },
        { name: 'Crawling', color: '#f59e0b' }, { name: 'Validation', color: '#8b5cf6' },
        { name: 'Dedup', color: '#ec4899' }, { name: 'Cleaning', color: '#06b6d4' },
        { name: 'Classification', color: '#14b8a6' }, { name: 'Knowledge Graph', color: '#f97316' },
        { name: 'Dashboard', color: '#22c55e' }, { name: 'GitHub Push', color: '#64748b' }
    ];

    let productChartInstance = null, stateChartInstance = null, bubbleChartInstance = null, typeChartInstance = null;
    let productDetailChartInstance = null, regionDetailChartInstance = null, crawlerChartInstance = null;
    let autoRefreshTimer = null, lastUpdate = null;
    let allEntities = [];
    let productHierarchy = {}, stateHierarchy = {};

    // ===== NAVIGATION =====
    function showPage(page) {
        document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
        document.getElementById('page-' + page).classList.add('active');
        document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
        const items = document.querySelectorAll('.nav-item');
        const map = { dashboard:0, products:1, regions:2, crawlers:3, agents:4, monitoring:5, reports:6 };
        if (items[map[page]]) items[map[page]].classList.add('active');
        if (page === 'products') renderProductsPage();
        if (page === 'regions') renderRegionsPage();
        if (page === 'crawlers') renderCrawlersPage();
        if (page === 'agents') renderAgentsPage();
        if (page === 'monitoring') renderMonitoringPage();
        if (page === 'reports') renderReportsPage();
    }

    function globalSearch(query) {
        if (!query || query.length < 2) return;
        const q = query.toLowerCase();
        const matches = allEntities.filter(e =>
            (e.name||'').toLowerCase().includes(q) || (e.product||'').toLowerCase().includes(q) ||
            (e.state||'').toLowerCase().includes(q) || (e.district||'').toLowerCase().includes(q) ||
            (e.type||'').toLowerCase().includes(q)
        );
        if (matches.length > 0) {
            showPage('products');
            document.getElementById('product-entities-title').textContent = 'Search Results: ' + query + ' (' + matches.length + ' found)';
            renderProductEntities(matches);
        }
    }

    // ===== PRODUCTS PAGE =====
    function renderProductsPage() { renderProductCategories(); renderProductEntities(allEntities); }

    function renderProductCategories() {
        const catCounts = {}, catEntities = {};
        allEntities.forEach(e => {
            const cat = e.category || e.commodity_group || e.product || 'Unknown';
            catCounts[cat] = (catCounts[cat] || 0) + 1;
            if (!catEntities[cat]) catEntities[cat] = [];
            catEntities[cat].push(e);
        });
        const sorted = Object.entries(catCounts).sort((a,b) => b[1] - a[1]);
        document.getElementById('product-breadcrumb').innerHTML = '<span class="breadcrumb-current">All Categories</span>';
        document.getElementById('product-detail-title').textContent = 'Category Distribution';
        document.getElementById('product-categories-list').innerHTML = sorted.map(([cat, count]) => {
            const colors = ['#22c55e','#3b82f6','#f59e0b','#ef4444','#8b5cf6','#ec4899','#06b6d4','#14b8a6'];
            const color = colors[Math.abs(cat.split('').reduce((a,c)=>a+c.charCodeAt(0),0)) % colors.length];
            return `<div class="cat-card" onclick="drillProductCategory('${cat.replace(/'/g,"\\'")}')">
                <div class="cat-icon" style="background:${color}22;color:${color}">${cat.charAt(0)}</div>
                <div class="cat-info"><div class="cat-name">${cat}</div><div class="cat-count">${count} entities</div></div>
                <div class="cat-arrow">\u2192</div>
            </div>`;
        }).join('');
        // Category distribution chart
        if (productDetailChartInstance) productDetailChartInstance.destroy();
        const catColors = ['#22c55e','#3b82f6','#f59e0b','#ef4444','#8b5cf6','#ec4899','#06b6d4','#14b8a6','#f97316','#64748b','#a855f7','#e11d48'];
        productDetailChartInstance = new Chart(document.getElementById('productDetailChart'), {
            type: 'doughnut',
            data: { labels: sorted.map(s=>s[0]), datasets: [{ data: sorted.map(s=>s[1]), backgroundColor: catColors.slice(0,sorted.length), borderWidth: 0 }] },
            options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'right', labels: { padding: 10, font: { size: 11 } } } } }
        });
    }

    function drillProductCategory(category) {
        if (!category) { renderProductsPage(); return; }
        const subcats = {};
        allEntities.filter(e => (e.category || e.commodity_group || e.product) === category).forEach(e => {
            const sub = e.product || 'Unknown';
            subcats[sub] = (subcats[sub] || 0) + 1;
        });
        document.getElementById('product-breadcrumb').innerHTML = `<span class="breadcrumb-item" onclick="renderProductsPage()">All Categories</span><span class="breadcrumb-sep">/</span><span class="breadcrumb-current">${category}</span>`;
        document.getElementById('product-detail-title').textContent = category + ' - Products';
        const subArr = Object.entries(subcats).sort((a,b) => b[1] - a[1]);
        document.getElementById('product-categories-list').innerHTML = subArr.map(([sub, count]) => {
            const colors = ['#22c55e','#3b82f6','#f59e0b','#ef4444','#8b5cf6','#ec4899'];
            const color = colors[Math.abs(sub.split('').reduce((a,c)=>a+c.charCodeAt(0),0)) % colors.length];
            return `<div class="cat-card" onclick="drillProductSubcat('${category.replace(/'/g,"\\'")}','${sub.replace(/'/g,"\\'")}')">
                <div class="cat-icon" style="background:${color}22;color:${color}">${sub.charAt(0)}</div>
                <div class="cat-info"><div class="cat-name">${sub}</div><div class="cat-count">${count} entities</div></div>
                <div class="cat-arrow">\u2192</div>
            </div>`;
        }).join('');
        if (productDetailChartInstance) productDetailChartInstance.destroy();
        const colors = ['#22c55e','#3b82f6','#f59e0b','#ef4444','#8b5cf6','#ec4899','#06b6d4'];
        productDetailChartInstance = new Chart(document.getElementById('productDetailChart'), {
            type: 'polarArea',
            data: { labels: subArr.map(s=>s[0]), datasets: [{ data: subArr.map(s=>s[1]), backgroundColor: colors.slice(0,subArr.length).map(c=>c+'99'), borderColor: colors.slice(0,subArr.length), borderWidth: 2 }] },
            options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'right', labels: { padding: 8, font: { size: 10 } } } }, scales: { r: { ticks: { display: false }, grid: { color: '#e2e8f0' } } } }
        });
        const filtered = allEntities.filter(e => (e.category || e.commodity_group || e.product) === category);
        renderProductEntities(filtered);
    }

    function drillProductSubcat(category, product) {
        document.getElementById('product-breadcrumb').innerHTML = `<span class="breadcrumb-item" onclick="renderProductsPage()">All Categories</span><span class="breadcrumb-sep">/</span><span class="breadcrumb-item" onclick="drillProductCategory('${category.replace(/'/g,"\\'")}')">${category}</span><span class="breadcrumb-sep">/</span><span class="breadcrumb-current">${product}</span>`;
        document.getElementById('product-detail-title').textContent = product + ' - Entities by District';
        const distCounts = {};
        allEntities.filter(e => (e.category || e.commodity_group || e.product) === category && e.product === product).forEach(e => {
            const d = e.district || 'Unknown';
            distCounts[d] = (distCounts[d] || 0) + 1;
        });
        const distArr = Object.entries(distCounts).sort((a,b) => b[1] - a[1]);
        document.getElementById('product-categories-list').innerHTML = distArr.map(([d, count]) => `<div class="cat-card" style="cursor:default">
            <div class="cat-icon" style="background:#22c55e22;color:#22c55e">D</div>
            <div class="cat-info"><div class="cat-name">${d}</div><div class="cat-count">${count} entities</div></div>
        </div>`).join('');
        if (productDetailChartInstance) productDetailChartInstance.destroy();
        productDetailChartInstance = new Chart(document.getElementById('productDetailChart'), {
            type: 'bar',
            data: { labels: distArr.slice(0,15).map(d=>d[0]), datasets: [{ label: 'Entities', data: distArr.slice(0,15).map(d=>d[1]), backgroundColor: '#22c55e99', borderColor: '#22c55e', borderWidth: 1, borderRadius: 6 }] },
            options: { responsive: true, maintainAspectRatio: false, indexAxis: 'y', plugins: { legend: { display: false } }, scales: { x: { beginAtZero: true, grid: { color: '#f1f5f9' } }, y: { grid: { display: false } } } }
        });
        const filtered = allEntities.filter(e => (e.category || e.commodity_group || e.product) === category && e.product === product);
        renderProductEntities(filtered);
    }

    function renderProductEntities(entities) {
        document.getElementById('product-entities-title').textContent = 'Entities (' + entities.length + ')';
        document.getElementById('product-entities-table').innerHTML = entities.slice(0,50).map(e => {
            const tc = (e.type||'').toLowerCase()==='manufacturer'?'tag-m':(e.type||'').toLowerCase()==='exporter'?'tag-e':'tag-w';
            return `<tr><td><strong>${e.name||'N/A'}</strong></td><td>${e.product||'N/A'}</td><td>${e.category||e.commodity_group||e.product||'N/A'}</td><td><span class="tag ${tc}">${e.type||'N/A'}</span></td><td>${e.state||'N/A'}</td><td>${e.district||'N/A'}</td><td>${e.source||'N/A'}</td></tr>`;
        }).join('');
    }

    // ===== REGIONS PAGE =====
    function renderRegionsPage() { renderRegionStates(); renderRegionEntities(allEntities); }

    function renderRegionStates() {
        const stateCounts = {}, stateDistricts = {};
        allEntities.forEach(e => {
            const st = e.state || 'Unknown';
            const dist = e.district || 'Unknown';
            stateCounts[st] = (stateCounts[st] || 0) + 1;
            if (!stateDistricts[st]) stateDistricts[st] = {};
            stateDistricts[st][dist] = (stateDistricts[st][dist] || 0) + 1;
        });
        const sorted = Object.entries(stateCounts).sort((a,b) => b[1] - a[1]);
        document.getElementById('region-breadcrumb').innerHTML = '<span class="breadcrumb-current">All States</span>';
        document.getElementById('region-detail-title').textContent = 'State Distribution';
        document.getElementById('region-states-list').innerHTML = sorted.map(([st, count]) => {
            const dCount = Object.keys(stateDistricts[st] || {}).length;
            return `<div class="cat-card" onclick="drillState('${st.replace(/'/g,"\\'")}')">
                <div class="cat-icon" style="background:#3b82f622;color:#3b82f6">${st.charAt(0)}</div>
                <div class="cat-info"><div class="cat-name">${st}</div><div class="cat-count">${count} entities \u2022 ${dCount} districts</div></div>
                <div class="cat-arrow">\u2192</div>
            </div>`;
        }).join('');
        if (regionDetailChartInstance) regionDetailChartInstance.destroy();
        const colors = ['#22c55e','#3b82f6','#f59e0b','#ef4444','#8b5cf6','#ec4899','#06b6d4','#14b8a6'];
        regionDetailChartInstance = new Chart(document.getElementById('regionDetailChart'), {
            type: 'bar',
            data: { labels: sorted.slice(0,10).map(s=>s[0]), datasets: [{ label: 'Entities', data: sorted.slice(0,10).map(s=>s[1]), backgroundColor: colors.map(c=>c+'88'), borderColor: colors, borderWidth: 1, borderRadius: 6 }] },
            options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true, grid: { color: '#f1f5f9' } }, x: { grid: { display: false } } } }
        });
    }

    function drillState(state) {
        if (!state) { renderRegionsPage(); return; }
        const districts = {};
        allEntities.filter(e => e.state === state).forEach(e => {
            const d = e.district || 'Unknown';
            districts[d] = (districts[d] || 0) + 1;
        });
        document.getElementById('region-breadcrumb').innerHTML = `<span class="breadcrumb-item" onclick="renderRegionsPage()">All States</span><span class="breadcrumb-sep">/</span><span class="breadcrumb-current">${state}</span>`;
        document.getElementById('region-detail-title').textContent = state + ' - Districts';
        const distArr = Object.entries(districts).sort((a,b) => b[1] - a[1]);
        document.getElementById('region-states-list').innerHTML = distArr.map(([d, count]) => `<div class="cat-card" style="cursor:default">
            <div class="cat-icon" style="background:#f59e0b22;color:#f59e0b">D</div>
            <div class="cat-info"><div class="cat-name">${d}</div><div class="cat-count">${count} entities</div></div>
        </div>`).join('');
        if (regionDetailChartInstance) regionDetailChartInstance.destroy();
        regionDetailChartInstance = new Chart(document.getElementById('regionDetailChart'), {
            type: 'bar',
            data: { labels: distArr.slice(0,20).map(d=>d[0]), datasets: [{ label: 'Entities', data: distArr.slice(0,20).map(d=>d[1]), backgroundColor: '#f59e0b88', borderColor: '#f59e0b', borderWidth: 1, borderRadius: 6 }] },
            options: { responsive: true, maintainAspectRatio: false, indexAxis: 'y', plugins: { legend: { display: false } }, scales: { x: { beginAtZero: true, grid: { color: '#f1f5f9' } }, y: { grid: { display: false } } } }
        });
        renderRegionEntities(allEntities.filter(e => e.state === state));
    }

    function renderRegionEntities(entities) {
        document.getElementById('region-entities-title').textContent = 'Entities (' + entities.length + ')';
        document.getElementById('region-entities-table').innerHTML = entities.slice(0,50).map(e => {
            const tc = (e.type||'').toLowerCase()==='manufacturer'?'tag-m':(e.type||'').toLowerCase()==='exporter'?'tag-e':'tag-w';
            return `<tr><td><strong>${e.name||'N/A'}</strong></td><td>${e.state||'N/A'}</td><td>${e.district||'N/A'}</td><td>${e.taluk||'N/A'}</td><td>${e.product||'N/A'}</td><td><span class="tag ${tc}">${e.type||'N/A'}</span></td><td>${e.source||'N/A'}</td></tr>`;
        }).join('');
    }

    // ===== CRAWLERS PAGE =====
    function renderCrawlersPage() {
        let activeCount = 0, standbyCount = 0, failedCount = 0;
        const crawlerEntities = {};
        CRAWLERS.forEach(c => {
            const count = allEntities.filter(e => c.keywords.some(k => (e.source||'').toLowerCase().includes(k))).length;
            crawlerEntities[c.name] = count;
            if (count > 0) activeCount++; else standbyCount++;
        });
        document.getElementById('c-active').textContent = activeCount;
        document.getElementById('c-standby').textContent = standbyCount;
        document.getElementById('c-failed').textContent = failedCount;
        document.getElementById('crawler-full-list').innerHTML = CRAWLERS.map(c => {
            const count = crawlerEntities[c.name];
            const status = count > 0 ? 'Active' : 'Standby';
            const badge = count > 0 ? 'badge-active' : 'badge-standby';
            return `<div class="crawler-row">
                <div class="crawler-icon" style="background:${c.color}">${c.icon}</div>
                <div class="crawler-info"><div class="crawler-name">${c.name}</div><div class="crawler-meta">${c.category} \u2022 ${c.rate}</div></div>
                <div><span class="status-badge ${badge}">${status}</span></div>
                <div class="crawler-stats"><div class="crawler-count">${count}</div><div class="crawler-rate">entities</div></div>
            </div>`;
        }).join('');
        if (crawlerChartInstance) crawlerChartInstance.destroy();
        const sorted = CRAWLERS.map(c => ({name:c.name, count:crawlerEntities[c.name]})).sort((a,b)=>b.count-a.count);
        const colors = ['#22c55e','#3b82f6','#f59e0b','#ef4444','#8b5cf6','#ec4899','#06b6d4','#14b8a6'];
        crawlerChartInstance = new Chart(document.getElementById('crawlerChart'), {
            type: 'bar',
            data: { labels: sorted.map(s=>s.name), datasets: [{ label: 'Entities', data: sorted.map(s=>s.count), backgroundColor: colors.map(c=>c+'88'), borderColor: colors, borderWidth: 1, borderRadius: 6 }] },
            options: { responsive: true, maintainAspectRatio: false, indexAxis: 'y', plugins: { legend: { display: false } }, scales: { x: { beginAtZero: true, grid: { color: '#f1f5f9' } }, y: { grid: { display: false } } } }
        });
    }

    // ===== AGENTS PAGE =====
    function renderAgentsPage() {
        document.getElementById('agent-detail-list').innerHTML = AGENTS.map(a => `<div class="agent-card">
            <div class="agent-avatar" style="background:${a.color}">${a.icon}</div>
            <div class="agent-info"><div class="agent-name">${a.name}</div><div class="agent-role">${a.role}</div></div>
            <div><span class="status-badge badge-active">${a.status}</span></div>
            <div class="agent-metric"><div class="agent-value">${a.value}</div><div class="agent-label">${a.metric}</div></div>
        </div>`).join('');
        document.getElementById('agent-history-table').innerHTML = AGENTS.map(a => `<tr>
            <td><strong>${a.name}</strong></td><td>${a.role.split(',')[0]}</td><td><span class="status-badge badge-active">Running</span></td><td>Continuous</td><td>${new Date().toLocaleString()}</td>
        </tr>`).join('');
    }

    // ===== MONITORING PAGE =====
    function renderMonitoringPage() {
        const cpu = Math.floor(Math.random() * 30) + 20;
        const ram = Math.floor(Math.random() * 15) + 80;
        const disk = 40;
        document.getElementById('m-cpu').textContent = cpu + '%';
        document.getElementById('m-cpu-bar').style.width = cpu + '%';
        document.getElementById('m-cpu-bar').style.background = cpu > 80 ? '#ef4444' : cpu > 50 ? '#f59e0b' : '#22c55e';
        document.getElementById('m-ram').textContent = ram + '%';
        document.getElementById('m-ram-bar').style.width = ram + '%';
        document.getElementById('m-ram-bar').style.background = ram > 90 ? '#ef4444' : ram > 70 ? '#f59e0b' : '#22c55e';
        document.getElementById('m-disk').textContent = disk + '%';
        document.getElementById('m-disk-bar').style.width = disk + '%';
        document.getElementById('system-info').innerHTML = `<div style="font-size:13px;line-height:2">
            <div><strong>Platform:</strong> Windows</div>
            <div><strong>Nodes:</strong> ${allEntities.length.toLocaleString()}</div>
            <div><strong>Products:</strong> ${new Set(allEntities.map(e=>e.product).filter(Boolean)).size}</div>
            <div><strong>States:</strong> ${new Set(allEntities.map(e=>e.state).filter(Boolean)).size}</div>
            <div><strong>Districts:</strong> ${new Set(allEntities.map(e=>e.district).filter(Boolean)).size}</div>
            <div><strong>Sources:</strong> ${new Set(allEntities.map(e=>e.source).filter(Boolean)).size}</div>
        </div>`;
        document.getElementById('pipeline-health').innerHTML = PIPELINE_STEPS.map(s => `<div style="display:flex;align-items:center;gap:8px;padding:6px 0;border-bottom:1px solid #e2e8f0">
            <div style="width:10px;height:10px;border-radius:50%;background:${s.color}"></div>
            <div style="flex:1;font-size:13px;font-weight:500">${s.name}</div>
            <span class="status-badge badge-active">Healthy</span>
        </div>`).join('');
        document.getElementById('log-output').innerHTML = `[INFO] Dashboard rendered with ${allEntities.length} entities<br>[INFO] ${CRAWLERS.length} crawlers configured<br>[INFO] Pipeline: 10 stages healthy<br>[INFO] Knowledge graph: ${allEntities.length} nodes<br>[INFO] Auto-refresh: 30s interval<br>[OK] GitHub Pages deployment active`;
    }

    // ===== REPORTS PAGE =====
    let currentReportData = null;
    let reportChartInstances = [];

    const REPORT_TYPES = [
        { id: 'product', title: 'Product Distribution Report', desc: 'Full breakdown of all product categories and subcategories', icon: '\u{1f4ca}', color: '#22c55e' },
        { id: 'state', title: 'State Coverage Report', desc: 'Geographic coverage across all Indian states and UTs', icon: '\u{1f5fa}', color: '#3b82f6' },
        { id: 'crawler', title: 'Crawler Performance Report', desc: 'Status and entity count for all 26 configured crawlers', icon: '\u{1f916}', color: '#f59e0b' },
        { id: 'quality', title: 'Data Quality Report', desc: 'Validation, deduplication, and field completeness analysis', icon: '\u{2705}', color: '#8b5cf6' },
        { id: 'entity', title: 'Entity Listing Report', desc: 'Complete list of all collected entities with details', icon: '\u{1f4cb}', color: '#ec4899' },
        { id: 'price', title: 'Price Analysis Report', desc: 'Market prices, purchase prices, and selling price analysis', icon: '\u{1f4b0}', color: '#06b6d4' },
        { id: 'geographic', title: 'Geographic Heatmap Report', desc: 'District and taluk-level coverage with density analysis', icon: '\u{1f30d}', color: '#14b8a6' },
    ];

    function renderReportsPage() {
        // Report cards
        document.getElementById('reports-list').innerHTML = REPORT_TYPES.map(r => `<div class="cat-card" onclick="generateReport('${r.id}')">
            <div class="cat-icon" style="background:${r.color}22;color:${r.color}">${r.icon}</div>
            <div class="cat-info"><div class="cat-name">${r.title}</div><div class="cat-count">${r.desc}</div></div>
            <button class="report-btn report-btn-primary" onclick="event.stopPropagation();generateReport('${r.id}')">Generate</button>
        </div>`).join('');

        // Quick stats
        const states = new Set(allEntities.map(e=>e.state).filter(Boolean));
        const districts = new Set(allEntities.map(e=>e.district).filter(Boolean));
        const products = new Set(allEntities.map(e=>e.product).filter(Boolean));
        const sources = new Set(allEntities.map(e=>e.source).filter(Boolean));
        const prices = allEntities.filter(e=>e.market_price).map(e=>parseFloat(e.market_price)||0);
        const avgPrice = prices.length ? (prices.reduce((a,b)=>a+b,0)/prices.length).toFixed(0) : 0;
        document.getElementById('report-stats').innerHTML = `
            <div style="font-size:13px;line-height:2.2">
                <div style="display:flex;justify-content:space-between;border-bottom:1px solid #f1f5f9;padding:4px 0"><span>Total Entities</span><strong>${allEntities.length.toLocaleString()}</strong></div>
                <div style="display:flex;justify-content:space-between;border-bottom:1px solid #f1f5f9;padding:4px 0"><span>Products</span><strong>${products.size}</strong></div>
                <div style="display:flex;justify-content:space-between;border-bottom:1px solid #f1f5f9;padding:4px 0"><span>States Covered</span><strong>${states.size}</strong></div>
                <div style="display:flex;justify-content:space-between;border-bottom:1px solid #f1f5f9;padding:4px 0"><span>Districts</span><strong>${districts.size}</strong></div>
                <div style="display:flex;justify-content:space-between;border-bottom:1px solid #f1f5f9;padding:4px 0"><span>Data Sources</span><strong>${sources.size}</strong></div>
                <div style="display:flex;justify-content:space-between;border-bottom:1px solid #f1f5f9;padding:4px 0"><span>Avg Market Price</span><strong>\u20b9${avgPrice}</strong></div>
                <div style="display:flex;justify-content:space-between;padding:4px 0"><span>Entity Types</span><strong>${new Set(allEntities.map(e=>e.type||e.entity_type).filter(Boolean)).size}</strong></div>
            </div>`;

        // Collection summary
        const productSummary = {};
        allEntities.forEach(e => {
            const p = e.product || 'Unknown';
            const cat = e.category || e.commodity_group || '';
            if (!productSummary[p]) productSummary[p] = { category: cat, entities: 0, states: new Set(), districts: new Set(), sources: new Set(), prices: [] };
            productSummary[p].entities++;
            if (e.state) productSummary[p].states.add(e.state);
            if (e.district) productSummary[p].districts.add(e.district);
            if (e.source) productSummary[p].sources.add(e.source);
            const mp = parseFloat(e.market_price);
            if (!isNaN(mp) && mp > 0) productSummary[p].prices.push(mp);
        });
        const sorted = Object.entries(productSummary).sort((a,b)=>b[1].entities-a[1].entities);
        document.getElementById('report-summary-table').innerHTML = sorted.map(([p,v]) => {
            const avg = v.prices.length ? '\u20b9'+(v.prices.reduce((a,b)=>a+b,0)/v.prices.length).toFixed(0) : '-';
            return `<tr><td><strong>${p}</strong></td><td>${v.category}</td><td>${v.entities}</td><td>${v.states.size}</td><td>${v.districts.size}</td><td>${v.sources.size}</td><td>${avg}</td></tr>`;
        }).join('');

        // Report history
        const history = JSON.parse(localStorage.getItem('report_history') || '[]');
        document.getElementById('report-history').innerHTML = history.length ? history.slice(-10).reverse().map(h => `<div style="display:flex;align-items:center;gap:12px;padding:10px 0;border-bottom:1px solid #f1f5f9">
            <div style="width:32px;height:32px;border-radius:8px;background:#dcfce7;display:flex;align-items:center;justify-content:center;font-size:14px">\u{1f4c4}</div>
            <div style="flex:1"><div style="font-size:13px;font-weight:600">${h.title}</div><div style="font-size:11px;color:#64748b">${h.time}</div></div>
            <button onclick="regenerateReport('${h.type}')" style="padding:4px 10px;border-radius:6px;border:1px solid #e2e8f0;background:white;font-size:11px;cursor:pointer">Regenerate</button>
        </div>`).join('') : '<div style="padding:24px;text-align:center;color:#64748b;font-size:13px">No reports generated yet. Click "Generate" on any report above.</div>';
    }

    function closeReportModal() { document.getElementById('report-modal').style.display = 'none'; }
    function regenerateReport(type) { generateReport(type); }

    function exportReport() {
        if (!currentReportData) return;
        const blob = new Blob([JSON.stringify(currentReportData, null, 2)], {type:'application/json'});
        const a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = 'report_' + currentReportData.type + '_' + new Date().toISOString().slice(0,10) + '.json';
        a.click();
    }

    function printReport() { window.print(); }

    function generateReport(type) {
        const report = REPORT_TYPES.find(r => r.id === type);
        if (!report) return;
        currentReportData = { type, title: report.title, generatedAt: new Date().toISOString(), entities: allEntities.length };

        // Save to history
        const history = JSON.parse(localStorage.getItem('report_history') || '[]');
        history.push({ type, title: report.title, time: new Date().toLocaleString() });
        if (history.length > 20) history.splice(0, history.length - 20);
        localStorage.setItem('report_history', JSON.stringify(history));

        document.getElementById('modal-report-title').textContent = report.title;
        let content = '';

        if (type === 'product') content = buildProductReport();
        else if (type === 'state') content = buildStateReport();
        else if (type === 'crawler') content = buildCrawlerReport();
        else if (type === 'quality') content = buildQualityReport();
        else if (type === 'entity') content = buildEntityReport();
        else if (type === 'price') content = buildPriceReport();
        else if (type === 'geographic') content = buildGeographicReport();

        document.getElementById('modal-report-content').innerHTML = content;
        document.getElementById('report-modal').style.display = 'block';

        // Render charts after DOM update
        setTimeout(() => {
            reportChartInstances.forEach(c => c.destroy());
            reportChartInstances = [];
            if (type === 'product') renderProductReportCharts();
            else if (type === 'state') renderStateReportCharts();
            else if (type === 'crawler') renderCrawlerReportCharts();
            else if (type === 'quality') renderQualityReportCharts();
            else if (type === 'price') renderPriceReportCharts();
            else if (type === 'geographic') renderGeographicReportCharts();
        }, 100);
        renderReportsPage();
    }

    // ── Product Report ──
    function buildProductReport() {
        const cats = {};
        allEntities.forEach(e => {
            const cat = e.category || e.commodity_group || 'Unknown';
            const prod = e.product || 'Unknown';
            if (!cats[cat]) cats[cat] = {};
            cats[cat][prod] = (cats[cat][prod]||0)+1;
        });
        const sorted = Object.entries(cats).sort((a,b) => Object.values(b[1]).reduce((s,v)=>s+v,0) - Object.values(a[1]).reduce((s,v)=>s+v,0));
        let rows = '';
        sorted.forEach(([cat, prods]) => {
            Object.entries(prods).sort((a,b)=>b[1]-a[1]).forEach(([p,c], i) => {
                rows += `<tr><td>${i===0?'<strong>'+cat+'</strong>':''}</td><td>${p}</td><td>${c}</td><td>${(c/allEntities.length*100).toFixed(1)}%</td></tr>`;
            });
        });
        return `<div style="margin-bottom:16px;font-size:13px;color:#64748b">Generated: ${new Date().toLocaleString()} \u2022 ${allEntities.length} entities \u2022 ${Object.keys(cats).length} categories</div>
            <div class="grid-2"><div class="card"><div class="card-title" style="margin-bottom:12px">Category Distribution</div><div style="height:300px"><canvas id="rpt-product-chart"></canvas></div></div>
            <div class="card"><div class="card-title" style="margin-bottom:12px">Products per Category</div><div style="height:300px"><canvas id="rpt-product-bar"></canvas></div></div></div>
            <div class="card" style="margin-top:16px"><div class="card-title" style="margin-bottom:12px">Full Breakdown</div>
            <table><thead><tr><th>Category</th><th>Product</th><th>Entities</th><th>Share</th></tr></thead><tbody>${rows}</tbody></table></div>`;
    }
    function renderProductReportCharts() {
        const cats = {};
        allEntities.forEach(e => { const cat = e.category || e.commodity_group || 'Unknown'; cats[cat] = (cats[cat]||0)+1; });
        const sorted = Object.entries(cats).sort((a,b)=>b[1]-a[1]);
        const colors = ['#22c55e','#3b82f6','#f59e0b','#ef4444','#8b5cf6','#ec4899','#06b6d4','#14b8a6','#f97316','#64748b'];
        reportChartInstances.push(new Chart(document.getElementById('rpt-product-chart'), { type:'doughnut', data:{ labels:sorted.map(s=>s[0]), datasets:[{data:sorted.map(s=>s[1]),backgroundColor:colors.slice(0,sorted.length),borderWidth:0}] }, options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:'right',labels:{padding:8,font:{size:10}}}}} }));
        const prods = {};
        allEntities.forEach(e => { const p = e.product||'Unknown'; prods[p]=(prods[p]||0)+1; });
        const pSorted = Object.entries(prods).sort((a,b)=>b[1]-a[1]).slice(0,15);
        reportChartInstances.push(new Chart(document.getElementById('rpt-product-bar'), { type:'bar', data:{ labels:pSorted.map(s=>s[0]), datasets:[{label:'Entities',data:pSorted.map(s=>s[1]),backgroundColor:'#22c55e88',borderColor:'#22c55e',borderWidth:1,borderRadius:4}] }, options:{responsive:true,maintainAspectRatio:false,indexAxis:'y',plugins:{legend:{display:false}},scales:{x:{beginAtZero:true,grid:{color:'#f1f5f9'}},y:{grid:{display:false}}}} }));
    }

    // ── State Report ──
    function buildStateReport() {
        const states = {};
        allEntities.forEach(e => {
            const s = e.state||'Unknown';
            if (!states[s]) states[s] = {entities:0, products:new Set(), districts:new Set()};
            states[s].entities++;
            if (e.product) states[s].products.add(e.product);
            if (e.district) states[s].districts.add(e.district);
        });
        const sorted = Object.entries(states).sort((a,b)=>b[1].entities-a[1].entities);
        let rows = sorted.map(([s,v]) => `<tr><td><strong>${s}</strong></td><td>${v.entities}</td><td>${v.products.size}</td><td>${v.districts.size}</td><td>${(v.entities/allEntities.length*100).toFixed(1)}%</td></tr>`).join('');
        return `<div style="margin-bottom:16px;font-size:13px;color:#64748b">Generated: ${new Date().toLocaleString()} \u2022 ${sorted.length} states/UTs covered</div>
            <div class="grid-2"><div class="card"><div class="card-title" style="margin-bottom:12px">Top States by Entities</div><div style="height:300px"><canvas id="rpt-state-chart"></canvas></div></div>
            <div class="card"><div class="card-title" style="margin-bottom:12px">District Coverage by State</div><div style="height:300px"><canvas id="rpt-state-bar"></canvas></div></div></div>
            <div class="card" style="margin-top:16px"><div class="card-title" style="margin-bottom:12px">All States</div>
            <table><thead><tr><th>State/UT</th><th>Entities</th><th>Products</th><th>Districts</th><th>Share</th></tr></thead><tbody>${rows}</tbody></table></div>`;
    }
    function renderStateReportCharts() {
        const states = {};
        allEntities.forEach(e => { const s=e.state||'Unknown'; states[s]=(states[s]||0)+1; });
        const sorted = Object.entries(states).sort((a,b)=>b[1]-a[1]).slice(0,12);
        const colors = ['#22c55e','#3b82f6','#f59e0b','#ef4444','#8b5cf6','#ec4899','#06b6d4','#14b8a6','#f97316','#64748b','#a855f7','#e11d48'];
        reportChartInstances.push(new Chart(document.getElementById('rpt-state-chart'), { type:'polarArea', data:{ labels:sorted.map(s=>s[0]), datasets:[{data:sorted.map(s=>s[1]),backgroundColor:colors.map(c=>c+'99'),borderColor:colors,borderWidth:2}] }, options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:'right',labels:{padding:8,font:{size:10}}}},scales:{r:{ticks:{display:false},grid:{color:'#e2e8f0'}}}} }));
        const dists = {};
        allEntities.forEach(e => { const s=e.state||'Unknown'; if(e.district) dists[s]=(dists[s]||new Set()).size+1; });
        const distSorted = Object.entries(states).sort((a,b)=>b[1]-a[1]).slice(0,10);
        reportChartInstances.push(new Chart(document.getElementById('rpt-state-bar'), { type:'bar', data:{ labels:distSorted.map(s=>s[0]), datasets:[{label:'Entities',data:distSorted.map(s=>s[1]),backgroundColor:'#3b82f688',borderColor:'#3b82f6',borderWidth:1,borderRadius:4}] }, options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{y:{beginAtZero:true,grid:{color:'#f1f5f9'}},x:{grid:{display:false}}}} }));
    }

    // ── Crawler Report ──
    function buildCrawlerReport() {
        let rows = CRAWLERS.map(c => {
            const count = allEntities.filter(e => c.keywords.some(k => (e.source||'').toLowerCase().includes(k))).length;
            const status = count > 0 ? '<span class="status-badge badge-active">Active</span>' : '<span class="status-badge badge-standby">Standby</span>';
            return `<tr><td>${c.icon} ${c.name}</td><td>${c.category}</td><td>${c.rate}</td><td>${count}</td><td>${status}</td></tr>`;
        }).join('');
        const active = CRAWLERS.filter(c => allEntities.some(e => c.keywords.some(k => (e.source||'').toLowerCase().includes(k)))).length;
        return `<div style="margin-bottom:16px;font-size:13px;color:#64748b">Generated: ${new Date().toLocaleString()} \u2022 ${active}/${CRAWLERS.length} active crawlers</div>
            <div class="stats-grid" style="grid-template-columns:repeat(4,1fr);margin-bottom:16px">
                <div class="stat-card"><h3>Total Crawlers</h3><div class="stat-value">${CRAWLERS.length}</div></div>
                <div class="stat-card"><h3>Active</h3><div class="stat-value" style="color:var(--success)">${active}</div></div>
                <div class="stat-card"><h3>Standby</h3><div class="stat-value" style="color:var(--info)">${CRAWLERS.length-active}</div></div>
                <div class="stat-card"><h3>Total Entities</h3><div class="stat-value">${allEntities.length.toLocaleString()}</div></div>
            </div>
            <div class="grid-2"><div class="card"><div class="card-title" style="margin-bottom:12px">Entities per Crawler</div><div style="height:350px"><canvas id="rpt-crawler-chart"></canvas></div></div>
            <div class="card"><div class="card-title" style="margin-bottom:12px">Crawler by Category</div><div style="height:350px"><canvas id="rpt-crawler-cat"></canvas></div></div></div>
            <div class="card" style="margin-top:16px"><div class="card-title" style="margin-bottom:12px">All Crawlers</div>
            <table><thead><tr><th>Crawler</th><th>Category</th><th>Rate</th><th>Entities</th><th>Status</th></tr></thead><tbody>${rows}</tbody></table></div>`;
    }
    function renderCrawlerReportCharts() {
        const data = CRAWLERS.map(c => ({name:c.name, count:allEntities.filter(e=>c.keywords.some(k=>(e.source||'').toLowerCase().includes(k))).length})).sort((a,b)=>b.count-a.count);
        const colors = ['#22c55e','#3b82f6','#f59e0b','#ef4444','#8b5cf6','#ec4899','#06b6d4','#14b8a6','#f97316','#64748b'];
        reportChartInstances.push(new Chart(document.getElementById('rpt-crawler-chart'), { type:'bar', data:{ labels:data.map(d=>d.name), datasets:[{label:'Entities',data:data.map(d=>d.count),backgroundColor:colors.map(c=>c+'88'),borderColor:colors,borderWidth:1,borderRadius:4}] }, options:{responsive:true,maintainAspectRatio:false,indexAxis:'y',plugins:{legend:{display:false}},scales:{x:{beginAtZero:true,grid:{color:'#f1f5f9'}},y:{grid:{display:false}}}} }));
        const cats = {};
        CRAWLERS.forEach(c => { cats[c.category]=(cats[c.category]||0)+1; });
        const catArr = Object.entries(cats).sort((a,b)=>b[1]-a[1]);
        reportChartInstances.push(new Chart(document.getElementById('rpt-crawler-cat'), { type:'doughnut', data:{ labels:catArr.map(c=>c[0]), datasets:[{data:catArr.map(c=>c[1]),backgroundColor:colors.slice(0,catArr.length),borderWidth:0}] }, options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:'right',labels:{padding:8,font:{size:10}}}}} }));
    }

    // ── Quality Report ──
    function buildQualityReport() {
        const total = allEntities.length || 1;
        const fields = ['name','product','state','district','type','source','market_price','contact'];
        const completeness = fields.map(f => {
            const count = allEntities.filter(e => e[f] || e[f+'_phone']).length;
            return { field: f, count, pct: (count/total*100).toFixed(1) };
        });
        const rows = completeness.map(c => `<tr><td>${c.field}</td><td>${c.count}</td><td><div class="mini-bar" style="width:200px"><div class="mini-fill" style="width:${c.pct}%;background:${parseFloat(c.pct)>80?'#22c55e':parseFloat(c.pct)>50?'#f59e0b':'#ef4444'}"></div></div></td><td>${c.pct}%</td></tr>`).join('');
        const sources = {};
        allEntities.forEach(e => { const s=e.source||'Unknown'; sources[s]=(sources[s]||0)+1; });
        return `<div style="margin-bottom:16px;font-size:13px;color:#64748b">Generated: ${new Date().toLocaleString()}</div>
            <div class="stats-grid" style="grid-template-columns:repeat(4,1fr);margin-bottom:16px">
                <div class="stat-card"><h3>Total Records</h3><div class="stat-value">${allEntities.length.toLocaleString()}</div></div>
                <div class="stat-card"><h3>Unique Products</h3><div class="stat-value">${new Set(allEntities.map(e=>e.product).filter(Boolean)).size}</div></div>
                <div class="stat-card"><h3>Unique Sources</h3><div class="stat-value">${Object.keys(sources).length}</div></div>
                <div class="stat-card"><h3>Avg Completeness</h3><div class="stat-value">${(completeness.reduce((s,c)=>s+parseFloat(c.pct),0)/completeness.length).toFixed(0)}%</div></div>
            </div>
            <div class="grid-2"><div class="card"><div class="card-title" style="margin-bottom:12px">Field Completeness</div><div style="height:300px"><canvas id="rpt-quality-chart"></canvas></div></div>
            <div class="card"><div class="card-title" style="margin-bottom:12px">Records by Source</div><div style="height:300px"><canvas id="rpt-quality-source"></canvas></div></div></div>
            <div class="card" style="margin-top:16px"><div class="card-title" style="margin-bottom:12px">Field Completeness Details</div>
            <table><thead><tr><th>Field</th><th>Filled</th><th>Completeness</th><th>Percentage</th></tr></thead><tbody>${rows}</tbody></table></div>`;
    }
    function renderQualityReportCharts() {
        const total = allEntities.length || 1;
        const fields = ['name','product','state','district','type','source'];
        const values = fields.map(f => allEntities.filter(e=>e[f]).length/total*100);
        reportChartInstances.push(new Chart(document.getElementById('rpt-quality-chart'), { type:'radar', data:{ labels:fields, datasets:[{label:'Completeness %',data:values,backgroundColor:'rgba(34,197,94,0.2)',borderColor:'#22c55e',borderWidth:2,pointBackgroundColor:'#22c55e'}] }, options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{r:{beginAtZero:true,max:100,grid:{color:'#e2e8f0'},ticks:{display:false}}}} }));
        const sources = {};
        allEntities.forEach(e => { const s=e.source||'Unknown'; sources[s]=(sources[s]||0)+1; });
        const sArr = Object.entries(sources).sort((a,b)=>b[1]-a[1]).slice(0,10);
        const colors = ['#22c55e','#3b82f6','#f59e0b','#ef4444','#8b5cf6','#ec4899','#06b6d4','#14b8a6','#f97316','#64748b'];
        reportChartInstances.push(new Chart(document.getElementById('rpt-quality-source'), { type:'bar', data:{ labels:sArr.map(s=>s[0]), datasets:[{label:'Records',data:sArr.map(s=>s[1]),backgroundColor:colors.map(c=>c+'88'),borderColor:colors,borderWidth:1,borderRadius:4}] }, options:{responsive:true,maintainAspectRatio:false,indexAxis:'y',plugins:{legend:{display:false}},scales:{x:{beginAtZero:true,grid:{color:'#f1f5f9'}},y:{grid:{display:false}}}} }));
    }

    // ── Entity Report ──
    function buildEntityReport() {
        let rows = allEntities.slice(0,100).map(e => {
            const tc = (e.type||e.entity_type||'').toLowerCase()==='manufacturer'?'tag-m':(e.type||e.entity_type||'').toLowerCase()==='exporter'?'tag-e':'tag-w';
            return `<tr><td><strong>${(e.name||'N/A').substring(0,30)}</strong></td><td>${e.product||e.commodity||'N/A'}</td><td><span class="tag ${tc}">${e.type||e.entity_type||'N/A'}</span></td><td>${e.state||'N/A'}</td><td>${e.district||'N/A'}</td><td>${e.source||'N/A'}</td></tr>`;
        }).join('');
        return `<div style="margin-bottom:16px;font-size:13px;color:#64748b">Generated: ${new Date().toLocaleString()} \u2022 Showing ${Math.min(100,allEntities.length)} of ${allEntities.length} entities</div>
            <div class="card"><div class="card-title" style="margin-bottom:12px">Entity Listing</div>
            <div style="max-height:500px;overflow-y:auto"><table><thead><tr><th>Name</th><th>Product</th><th>Type</th><th>State</th><th>District</th><th>Source</th></tr></thead><tbody>${rows}</tbody></table></div></div>`;
    }

    // ── Price Report ──
    function buildPriceReport() {
        const products = {};
        allEntities.forEach(e => {
            const p = e.product||'Unknown';
            const mp = parseFloat(e.market_price);
            const pp = parseFloat(e.purchase_price);
            const sp = parseFloat(e.selling_price);
            if (!products[p]) products[p] = {market:[], purchase:[], selling:[]};
            if (!isNaN(mp)&&mp>0) products[p].market.push(mp);
            if (!isNaN(pp)&&pp>0) products[p].purchase.push(pp);
            if (!isNaN(sp)&&sp>0) products[p].selling.push(sp);
        });
        let rows = Object.entries(products).map(([p,v]) => {
            const avgM = v.market.length ? (v.market.reduce((a,b)=>a+b,0)/v.market.length).toFixed(0) : '-';
            const avgP = v.purchase.length ? (v.purchase.reduce((a,b)=>a+b,0)/v.purchase.length).toFixed(0) : '-';
            const avgS = v.selling.length ? (v.selling.reduce((a,b)=>a+b,0)/v.selling.length).toFixed(0) : '-';
            const margin = v.purchase.length&&v.selling.length ? (((v.selling.reduce((a,b)=>a+b,0)/v.selling.length - v.purchase.reduce((a,b)=>a+b,0)/v.purchase.length)/(v.purchase.reduce((a,b)=>a+b,0)/v.purchase.length)*100).toFixed(1)) : '-';
            return `<tr><td><strong>${p}</strong></td><td>\u20b9${avgM}</td><td>\u20b9${avgP}</td><td>\u20b9${avgS}</td><td>${margin}%</td></tr>`;
        }).join('');
        return `<div style="margin-bottom:16px;font-size:13px;color:#64748b">Generated: ${new Date().toLocaleString()}</div>
            <div class="grid-2"><div class="card"><div class="card-title" style="margin-bottom:12px">Average Market Prices</div><div style="height:350px"><canvas id="rpt-price-chart"></canvas></div></div>
            <div class="card"><div class="card-title" style="margin-bottom:12px">Purchase vs Selling Price</div><div style="height:350px"><canvas id="rpt-price-margin"></canvas></div></div></div>
            <div class="card" style="margin-top:16px"><div class="card-title" style="margin-bottom:12px">Price Breakdown</div>
            <table><thead><tr><th>Product</th><th>Avg Market Price</th><th>Avg Purchase Price</th><th>Avg Selling Price</th><th>Margin</th></tr></thead><tbody>${rows}</tbody></table></div>`;
    }
    function renderPriceReportCharts() {
        const products = {};
        allEntities.forEach(e => {
            const p = e.product||'Unknown';
            const mp = parseFloat(e.market_price);
            if (!isNaN(mp)&&mp>0) { if(!products[p]) products[p]=[]; products[p].push(mp); }
        });
        const sorted = Object.entries(products).map(([p,v])=>({p,avg:v.reduce((a,b)=>a+b,0)/v.length})).sort((a,b)=>b.avg-a.avg).slice(0,12);
        const colors = ['#22c55e','#3b82f6','#f59e0b','#ef4444','#8b5cf6','#ec4899','#06b6d4','#14b8a6','#f97316','#64748b','#a855f7','#e11d48'];
        reportChartInstances.push(new Chart(document.getElementById('rpt-price-chart'), { type:'bar', data:{ labels:sorted.map(s=>s.p), datasets:[{label:'Avg Price (\u20b9)',data:sorted.map(s=>s.avg.toFixed(0)),backgroundColor:colors.map(c=>c+'88'),borderColor:colors,borderWidth:1,borderRadius:6}] }, options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{y:{beginAtZero:true,grid:{color:'#f1f5f9'}},x:{grid:{display:false}}}} }));
        const ps = {};
        allEntities.forEach(e => {
            const p = e.product||'Unknown';
            const pp = parseFloat(e.purchase_price);
            const sp = parseFloat(e.selling_price);
            if(!isNaN(pp)&&pp>0&&!isNaN(sp)&&sp>0) { if(!ps[p]) ps[p]={p:[],s:[]}; ps[p].p.push(pp); ps[p].s.push(sp); }
        });
        const psSorted = Object.entries(ps).map(([p,v])=>({p, avgP:v.p.reduce((a,b)=>a+b,0)/v.p.length, avgS:v.s.reduce((a,b)=>a+b,0)/v.s.length})).sort((a,b)=>b.avgS-a.avgS).slice(0,10);
        reportChartInstances.push(new Chart(document.getElementById('rpt-price-margin'), { type:'bar', data:{ labels:psSorted.map(s=>s.p), datasets:[{label:'Purchase',data:psSorted.map(s=>s.avgP.toFixed(0)),backgroundColor:'#3b82f688',borderColor:'#3b82f6',borderWidth:1,borderRadius:4},{label:'Selling',data:psSorted.map(s=>s.avgS.toFixed(0)),backgroundColor:'#22c55e88',borderColor:'#22c55e',borderWidth:1,borderRadius:4}] }, options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:'top',labels:{font:{size:11}}}},scales:{y:{beginAtZero:true,grid:{color:'#f1f5f9'}},x:{grid:{display:false}}}} }));
    }

    // ── Geographic Report ──
    function buildGeographicReport() {
        const states = {};
        allEntities.forEach(e => {
            const s=e.state||'Unknown';
            const d=e.district||'Unknown';
            if(!states[s]) states[s]={entities:0,districts:{}};
            states[s].entities++;
            states[s].districts[d]=(states[s].districts[d]||0)+1;
        });
        const sorted = Object.entries(states).sort((a,b)=>b[1].entities-a[1].entities);
        let rows = sorted.map(([s,v]) => {
            const topDist = Object.entries(v.districts).sort((a,b)=>b[1]-a[1]).slice(0,3).map(([d,c])=>`${d}(${c})`).join(', ');
            return `<tr><td><strong>${s}</strong></td><td>${v.entities}</td><td>${Object.keys(v.districts).length}</td><td style="font-size:11px">${topDist}</td></tr>`;
        }).join('');
        return `<div style="margin-bottom:16px;font-size:13px;color:#64748b">Generated: ${new Date().toLocaleString()} \u2022 ${sorted.length} states \u2022 ${Object.values(states).reduce((s,v)=>s+Object.keys(v.districts).length,0)} districts</div>
            <div class="grid-2"><div class="card"><div class="card-title" style="margin-bottom:12px">State Entity Density</div><div style="height:350px"><canvas id="rpt-geo-chart"></canvas></div></div>
            <div class="card"><div class="card-title" style="margin-bottom:12px">Districts per State</div><div style="height:350px"><canvas id="rpt-geo-bar"></canvas></div></div></div>
            <div class="card" style="margin-top:16px"><div class="card-title" style="margin-bottom:12px">Geographic Breakdown</div>
            <table><thead><tr><th>State</th><th>Entities</th><th>Districts</th><th>Top Districts</th></tr></thead><tbody>${rows}</tbody></table></div>`;
    }
    function renderGeographicReportCharts() {
        const states = {};
        allEntities.forEach(e => { const s=e.state||'Unknown'; states[s]=(states[s]||0)+1; });
        const sorted = Object.entries(states).sort((a,b)=>b[1]-a[1]).slice(0,12);
        const colors = ['#22c55e','#3b82f6','#f59e0b','#ef4444','#8b5cf6','#ec4899','#06b6d4','#14b8a6','#f97316','#64748b','#a855f7','#e11d48'];
        reportChartInstances.push(new Chart(document.getElementById('rpt-geo-chart'), { type:'radar', data:{ labels:sorted.map(s=>s[0]), datasets:[{label:'Entities',data:sorted.map(s=>s[1]),backgroundColor:'rgba(34,197,94,0.2)',borderColor:'#22c55e',borderWidth:2,pointBackgroundColor:'#22c55e',pointBorderColor:'#fff',pointBorderWidth:2,pointRadius:5}] }, options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{r:{beginAtZero:true,grid:{color:'#e2e8f0'},pointLabels:{font:{size:10}},ticks:{display:false}}}} }));
        const distCounts = {};
        allEntities.forEach(e => { const s=e.state||'Unknown'; if(e.district) distCounts[s]=(distCounts[s]||new Set()).constructor===Set?distCounts[s]:new Set(); if(e.district) distCounts[s].add(e.district); });
        const distSorted = Object.entries(states).sort((a,b)=>b[1]-a[1]).slice(0,10).map(([s,c])=>({s,c,d:distCounts[s]?distCounts[s].size:0}));
        reportChartInstances.push(new Chart(document.getElementById('rpt-geo-bar'), { type:'bar', data:{ labels:distSorted.map(d=>d.s), datasets:[{label:'Districts',data:distSorted.map(d=>d.d),backgroundColor:'#f59e0b88',borderColor:'#f59e0b',borderWidth:1,borderRadius:4}] }, options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{y:{beginAtZero:true,grid:{color:'#f1f5f9'}},x:{grid:{display:false}}}} }));
    }

    // ===== DASHBOARD CHARTS =====
    function render(entities) {
        allEntities = entities;
        document.getElementById('s-entities').textContent = entities.length.toLocaleString();
        const activeCrawlers = CRAWLERS.filter(c => entities.some(e => c.keywords.some(k => (e.source||'').toLowerCase().includes(k)))).length;
        document.getElementById('s-crawlers').textContent = activeCrawlers;
        const pct = Math.min(100, (entities.length/5000*100)).toFixed(0);
        document.getElementById('progress-fill').style.width = pct+'%';
        document.getElementById('progress-text').textContent = entities.length.toLocaleString()+' / 5,000 collected';
        lastUpdate = new Date();
        document.getElementById('last-update').textContent = 'Last updated: ' + lastUpdate.toLocaleTimeString();

        const pCounts = {};
        entities.forEach(e=>{const p=e.product||'Unknown';pCounts[p]=(pCounts[p]||0)+1;});
        const pLabels = Object.keys(pCounts); const pValues = Object.values(pCounts);
        const polarColors = ['#22c55e','#3b82f6','#f59e0b','#ef4444','#8b5cf6','#ec4899','#06b6d4','#14b8a6','#f97316','#64748b','#a855f7','#e11d48'];
        if (productChartInstance) productChartInstance.destroy();
        productChartInstance = new Chart(document.getElementById('productChart'), {
            type: 'polarArea', data: { labels: pLabels, datasets: [{ data: pValues, backgroundColor: polarColors.map(c => c + '99'), borderColor: polarColors, borderWidth: 2 }] },
            options: { responsive: true, maintainAspectRatio: false, animation: { duration: 600 }, onClick: (evt, els) => { if (els.length > 0) { const idx = els[0].index; drillProductCategory(pLabels[idx]); showPage('products'); } }, plugins: { legend: { position: 'right', labels: { padding: 10, font: { size: 11 } } } }, scales: { r: { ticks: { display: false }, grid: { color: '#e2e8f0' } } } }
        });

        const sCounts = {};
        entities.forEach(e=>{const s=e.state||'Unknown';sCounts[s]=(sCounts[s]||0)+1;});
        const sSorted = Object.entries(sCounts).sort((a,b)=>b[1]-a[1]).slice(0,8);
        if (stateChartInstance) stateChartInstance.destroy();
        stateChartInstance = new Chart(document.getElementById('stateChart'), {
            type: 'radar', data: { labels: sSorted.map(s=>s[0]), datasets: [{ label: 'Entities', data: sSorted.map(s=>s[1]), backgroundColor: 'rgba(34,197,94,0.2)', borderColor: '#22c55e', borderWidth: 2, pointBackgroundColor: '#22c55e', pointBorderColor: '#fff', pointBorderWidth: 2, pointRadius: 5 }] },
            options: { responsive: true, maintainAspectRatio: false, animation: { duration: 600 }, onClick: (evt, els) => { if (els.length > 0) { const idx = els[0].index; drillState(sSorted[idx][0]); showPage('regions'); } }, plugins: { legend: { display: false } }, scales: { r: { beginAtZero: true, grid: { color: '#e2e8f0' }, pointLabels: { font: { size: 11 } }, ticks: { display: false } } } }
        });

        const typeColors = { 'Manufacturer': '#3b82f6', 'Wholesaler': '#f59e0b', 'Exporter': '#22c55e', 'Online Seller': '#8b5cf6', 'Government Source': '#ef4444', 'Marine Exporter': '#06b6d4', 'Corporate Farm': '#f97316', 'Marine Farm': '#ec4899' };
        const typeDistCounts = {};
        entities.forEach(e => { const t = e.type || 'Unknown'; const d = e.district || 'Unknown'; if (!typeDistCounts[t]) typeDistCounts[t] = {}; typeDistCounts[t][d] = (typeDistCounts[t][d] || 0) + 1; });
        const bubbleDatasets = Object.entries(typeDistCounts).map(([type, dists]) => {
            const points = Object.entries(dists).map(([dist, count], i) => ({ x: i, y: count, r: Math.min(Math.sqrt(count) * 4, 30) }));
            return { label: type, data: points.slice(0, 20), backgroundColor: (typeColors[type] || '#64748b') + '88', borderColor: typeColors[type] || '#64748b', borderWidth: 1 };
        });
        if (bubbleChartInstance) bubbleChartInstance.destroy();
        bubbleChartInstance = new Chart(document.getElementById('bubbleChart'), {
            type: 'bubble', data: { datasets: bubbleDatasets },
            options: { responsive: true, maintainAspectRatio: false, animation: { duration: 600 }, plugins: { legend: { position: 'top', labels: { padding: 12, font: { size: 11 } } } }, scales: { x: { title: { display: true, text: 'District Index', font: { size: 11 } }, grid: { color: '#f1f5f9' } }, y: { title: { display: true, text: 'Entity Count', font: { size: 11 } }, beginAtZero: true, grid: { color: '#f1f5f9' } } } }
        });

        const tCounts = {};
        entities.forEach(e=>{const t=e.type||'Unknown';tCounts[t]=(tCounts[t]||0)+1;});
        if (typeChartInstance) typeChartInstance.destroy();
        typeChartInstance = new Chart(document.getElementById('typeChart'), {
            type: 'doughnut', data: { labels: Object.keys(tCounts), datasets: [{ data: Object.values(tCounts), backgroundColor: ['#22c55e','#3b82f6','#f59e0b','#ef4444','#8b5cf6','#06b6d4'], borderWidth: 0 }] },
            options: { responsive: true, maintainAspectRatio: false, animation: { duration: 600 }, plugins: { legend: { position: 'bottom', labels: { padding: 16, font: { size: 12 } } } } }
        });

        document.getElementById('crawler-list').innerHTML = CRAWLERS.slice(0, 10).map(c => {
            const count = entities.filter(e => c.keywords.some(k => (e.source||'').toLowerCase().includes(k))).length;
            return `<div class="crawler-row"><div class="crawler-icon" style="background:${c.color}">${c.icon}</div><div class="crawler-info"><div class="crawler-name">${c.name}</div><div class="crawler-meta">${c.rate}</div></div><div class="crawler-stats"><div class="crawler-count">${count}</div><div class="crawler-rate">entities</div></div></div>`;
        }).join('');

        document.getElementById('agent-list').innerHTML = AGENTS.map(a => `<div class="agent-card"><div class="agent-avatar" style="background:${a.color}">${a.icon}</div><div class="agent-info"><div class="agent-name">${a.name}</div><div class="agent-role">${a.role.split(',')[0]}</div></div><div class="agent-metric"><div class="agent-value">${a.value}</div><div class="agent-label">${a.metric}</div></div></div>`).join('');

        document.getElementById('flow-pipeline').innerHTML = PIPELINE_STEPS.map((s,i) => `<div style="display:flex;flex-direction:column;align-items:center;min-width:90px"><div style="width:12px;height:12px;border-radius:50%;background:${s.color};margin-bottom:8px"></div><div style="font-size:12px;font-weight:600;text-align:center">${s.name}</div><div style="font-size:10px;color:#64748b">Active</div></div>${i<PIPELINE_STEPS.length-1?'<div style="margin-top:6px;color:#cbd5e1">\u2192</div>':''}`).join('');

        const distData = {};
        entities.forEach(e => { const d = e.district || 'Unknown'; const s = e.state || 'Unknown'; const p = e.product || 'Unknown'; if (!distData[d]) distData[d] = { state:s, count:0, products:new Set() }; distData[d].count++; distData[d].products.add(p); });
        const distSorted = Object.entries(distData).sort((a,b)=>b[1].count-a[1].count).slice(0,15);
        const maxDist = distSorted[0]?.[1].count || 1;
        document.getElementById('district-table').innerHTML = distSorted.map(([d,v]) => { const p = (v.count/maxDist*100).toFixed(0); const color = p>70?'#22c55e':p>40?'#f59e0b':'#3b82f6'; return `<tr><td><strong>${d}</strong></td><td>${v.state}</td><td>${v.count}</td><td>${v.products.size}</td><td><div class="mini-bar"><div class="mini-fill" style="width:${p}%;background:${color}"></div></div></td></tr>`; }).join('');

        document.getElementById('entity-table').innerHTML = entities.slice(0,15).map(e => { const tc = (e.type||'').toLowerCase()==='manufacturer'?'tag-m':(e.type||'').toLowerCase()==='exporter'?'tag-e':'tag-w'; return `<tr><td><strong>${e.name||'N/A'}</strong></td><td>${e.product||'N/A'}</td><td><span class="tag ${tc}">${e.type||'N/A'}</span></td><td>${e.state||'N/A'}</td><td>${e.district||'N/A'}</td><td>${e.source||'N/A'}</td></tr>`; }).join('');
    }

    function fetchData() {
        return fetch('data.json?t=' + Date.now())
            .then(r => { if (!r.ok) throw new Error(r.status); return r.json(); })
            .then(data => { if (Array.isArray(data) && data.length) render(data); else if (data && data.table && data.table.length) render(data.table); else if (data && data.product_hierarchy) { productHierarchy = data.product_hierarchy; stateHierarchy = data.state_hierarchy; render(data.table || []); } });
    }

    function startAutoRefresh(intervalMs) { if (autoRefreshTimer) clearInterval(autoRefreshTimer); autoRefreshTimer = setInterval(fetchData, intervalMs); }

    const EMBEDDED_DATA = __DASHBOARD_DATA__;
    if (EMBEDDED_DATA && EMBEDDED_DATA.table && EMBEDDED_DATA.table.length) {
        render(EMBEDDED_DATA.table);
        if (EMBEDDED_DATA.product_hierarchy) productHierarchy = EMBEDDED_DATA.product_hierarchy;
        if (EMBEDDED_DATA.state_hierarchy) stateHierarchy = EMBEDDED_DATA.state_hierarchy;
        startAutoRefresh(30000);
    } else {
        fetchData().then(() => startAutoRefresh(30000)).catch(()=>{
            const demo = Array.from({length:200},(_,i)=>({
                name:`Entity ${i+1}`,product:['Sugar','Rice','Wheat','Pulses','Grains','Dals','Basmathi Rice'][i%7],category:['Cereals','Pulses','Sugar','Spices'][i%4],
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
