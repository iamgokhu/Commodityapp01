"""Report generation for Commodity OS - produces MD, HTML, and JSON reports."""
import json
import logging
import os
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

from commodity_os.core.events import EventType, event_bus

logger = logging.getLogger(__name__)

REPORTS_DIR = Path("output/reports")


class ReportType(str, Enum):
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    YEARLY = "yearly"


@dataclass
class ReportMetadata:
    report_type: ReportType
    generated_at: datetime
    period_start: datetime
    period_end: datetime
    report_id: str = ""
    version: str = "1.0"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "report_type": self.report_type.value,
            "generated_at": self.generated_at.isoformat(),
            "period_start": self.period_start.isoformat(),
            "period_end": self.period_end.isoformat(),
            "report_id": self.report_id,
            "version": self.version,
        }


class ReportGenerator:
    """Generates reports in Markdown, HTML, and JSON formats."""

    def __init__(self, output_dir: Path = REPORTS_DIR):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    # ── public API ────────────────────────────────────────────────

    async def generate_report(
        self,
        report_type: ReportType,
        entities: List[Dict[str, Any]],
        stats: Dict[str, Any],
        knowledge_graph_data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, str]:
        """Generate a full report set (md, html, json). Returns dict with keys md, html, json."""
        now = datetime.utcnow()
        metadata = self._build_metadata(report_type, now)
        knowledge_graph_data = knowledge_graph_data or {}

        sections = {
            "metadata": metadata.to_dict(),
            "executive_summary": self._executive_summary(entities, stats, metadata),
            "data_collection_stats": self._data_collection_stats(stats),
            "entity_breakdown": self._entity_breakdown(entities),
            "price_trends": self._price_trends(entities, knowledge_graph_data),
            "top_entities": self._top_entities(entities),
            "data_quality": self._data_quality(entities, stats),
            "system_health": self._system_health(stats),
            "notable_changes": self._notable_changes(entities, stats),
        }

        ts = now.strftime("%Y%m%d_%H%M%S")
        base = f"{report_type.value}_{ts}"

        md = self._render_markdown(sections)
        html = self._render_html(sections, base)
        json_str = json.dumps(sections, indent=2, default=str)

        paths = self._save_all(base, md, html, json_str)

        await event_bus.emit(
            EventType.REPORT_GENERATED,
            {
                "report_type": report_type.value,
                "paths": paths,
                "entity_count": len(entities),
            },
            source="report_generator",
        )
        logger.info("Report generated: %s", base)
        return paths

    async def save_reports(self, report_data: Dict[str, str], report_type: ReportType) -> Dict[str, str]:
        """Persist pre-built report content dict keyed by format."""
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        base = f"{report_type.value}_{ts}"
        paths: Dict[str, str] = {}
        for fmt, content in report_data.items():
            ext = {"md": ".md", "html": ".html", "json": ".json"}.get(fmt, f".{fmt}")
            path = self.output_dir / f"{base}{ext}"
            path.write_text(content, encoding="utf-8")
            paths[fmt] = str(path)
        logger.info("Reports saved: %s", paths)
        return paths

    # ── metadata helpers ──────────────────────────────────────────

    @staticmethod
    def _build_metadata(report_type: ReportType, now: datetime) -> ReportMetadata:
        delta = {
            ReportType.HOURLY: timedelta(hours=1),
            ReportType.DAILY: timedelta(days=1),
            ReportType.WEEKLY: timedelta(weeks=1),
            ReportType.MONTHLY: timedelta(days=30),
            ReportType.QUARTERLY: timedelta(days=90),
            ReportType.YEARLY: timedelta(days=365),
        }[report_type]
        return ReportMetadata(
            report_type=report_type,
            generated_at=now,
            period_start=now - delta,
            period_end=now,
            report_id=f"rpt_{report_type.value}_{now.strftime('%Y%m%d%H%M%S')}",
        )

    # ── section builders ──────────────────────────────────────────

    def _executive_summary(
        self,
        entities: List[Dict[str, Any]],
        stats: Dict[str, Any],
        meta: ReportMetadata,
    ) -> Dict[str, Any]:
        total = len(entities)
        products = {e.get("product", "Unknown") for e in entities}
        states = {e.get("state", "Unknown") for e in entities}
        prices = [e.get("market_price") for e in entities if e.get("market_price") is not None]
        avg_price = sum(prices) / len(prices) if prices else 0
        return {
            "total_entities": total,
            "unique_products": len(products),
            "unique_states": len(states),
            "average_market_price": round(avg_price, 2),
            "period": f"{meta.period_start.date()} to {meta.period_end.date()}",
            "highlights": self._highlights(entities, stats),
        }

    def _highlights(self, entities: List[Dict], stats: Dict) -> List[str]:
        highlights: List[str] = []
        new_count = stats.get("new_entities", 0)
        if new_count:
            highlights.append(f"{new_count} new entities added")
        price_changes = stats.get("price_changes", 0)
        if price_changes:
            highlights.append(f"{price_changes} price changes detected")
        products = set(e.get("product", "") for e in entities)
        highlights.append(f"Tracking {len(entities)} entities across {len(products)} products")
        return highlights

    def _data_collection_stats(self, stats: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "total_crawl_cycles": stats.get("total_crawl_cycles", 0),
            "successful_crawls": stats.get("successful_crawls", 0),
            "failed_crawls": stats.get("failed_crawls", 0),
            "success_rate": self._pct(stats.get("successful_crawls", 0), stats.get("total_crawl_cycles", 0)),
            "avg_response_time_ms": stats.get("avg_response_time_ms", 0),
            "data_points_collected": stats.get("data_points_collected", 0),
            "deduplication_rate": stats.get("deduplication_rate", 0),
            "api_calls_made": stats.get("api_calls_made", 0),
        }

    def _entity_breakdown(self, entities: List[Dict[str, Any]]) -> Dict[str, Any]:
        by_type: Dict[str, int] = {}
        by_product: Dict[str, int] = {}
        by_state: Dict[str, int] = {}
        for e in entities:
            by_type[e.get("entity_type", "Unknown")] = by_type.get(e.get("entity_type", "Unknown"), 0) + 1
            by_product[e.get("product", "Unknown")] = by_product.get(e.get("product", "Unknown"), 0) + 1
            by_state[e.get("state", "Unknown")] = by_state.get(e.get("state", "Unknown"), 0) + 1
        return {
            "by_type": dict(sorted(by_type.items(), key=lambda x: -x[1])),
            "by_product": dict(sorted(by_product.items(), key=lambda x: -x[1])),
            "by_state": dict(sorted(by_state.items(), key=lambda x: -x[1])),
        }

    def _price_trends(
        self, entities: List[Dict[str, Any]], kg_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        price_map: Dict[str, List[float]] = {}
        for e in entities:
            p = e.get("product", "Unknown")
            mp = e.get("market_price")
            if mp is not None:
                price_map.setdefault(p, []).append(float(mp))
        averages = {k: round(sum(v) / len(v), 2) for k, v in price_map.items()}
        historical = kg_data.get("price_history", {})
        return {
            "current_averages": averages,
            "historical": historical,
            "trend_direction": self._compute_trend_direction(averages, historical),
        }

    @staticmethod
    def _compute_trend_direction(current: Dict[str, float], historical: Dict[str, Any]) -> Dict[str, str]:
        trends: Dict[str, str] = {}
        for product, avg in current.items():
            hist = historical.get(product, [])
            if isinstance(hist, list) and len(hist) >= 2:
                prev = hist[-1] if isinstance(hist[-1], (int, float)) else None
                if prev and prev > 0:
                    pct = (avg - prev) / prev * 100
                    trends[product] = "up" if pct > 1 else "down" if pct < -1 else "stable"
                    continue
            trends[product] = "unknown"
        return trends

    def _top_entities(self, entities: List[Dict[str, Any]], limit: int = 10) -> List[Dict[str, Any]]:
        scored = []
        for e in entities:
            score = 0
            if e.get("market_price"):
                score += 1
            if e.get("contact"):
                score += 1
            if e.get("year_established"):
                score += 1
            if e.get("gst"):
                score += 1
            scored.append({**e, "_score": score})
        scored.sort(key=lambda x: -x["_score"])
        return [{k: v for k, v in e.items() if k != "_score"} for e in scored[:limit]]

    def _data_quality(self, entities: List[Dict[str, Any]], stats: Dict[str, Any]) -> Dict[str, Any]:
        total = len(entities) or 1
        completeness = {
            "contact": sum(1 for e in entities if e.get("contact")) / total,
            "price": sum(1 for e in entities if e.get("market_price") is not None) / total,
            "year_established": sum(1 for e in entities if e.get("year_established")) / total,
            "gst": sum(1 for e in entities if e.get("gst")) / total,
            "address": sum(1 for e in entities if e.get("office_address")) / total,
        }
        return {
            "completeness": {k: round(v * 100, 1) for k, v in completeness.items()},
            "overall_score": round(sum(completeness.values()) / len(completeness) * 100, 1),
            "duplicate_count": stats.get("duplicates_removed", 0),
            "stale_records": stats.get("stale_records", 0),
        }

    def _system_health(self, stats: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "uptime_hours": stats.get("uptime_hours", 0),
            "error_rate": stats.get("error_rate", 0),
            "avg_latency_ms": stats.get("avg_latency_ms", 0),
            "memory_usage_mb": stats.get("memory_usage_mb", 0),
            "active_agents": stats.get("active_agents", 0),
            "queue_depth": stats.get("queue_depth", 0),
            "last_cycle_duration_s": stats.get("last_cycle_duration_s", 0),
            "health_score": stats.get("health_score", 100),
        }

    def _notable_changes(self, entities: List[Dict[str, Any]], stats: Dict[str, Any]) -> Dict[str, Any]:
        new_entities = [e for e in entities if e.get("is_new")]
        price_spikes = [e for e in entities if e.get("price_change_pct", 0) > 10]
        price_drops = [e for e in entities if e.get("price_change_pct", 0) < -10]
        return {
            "new_entities_count": len(new_entities),
            "new_entities": [{"name": e.get("name"), "product": e.get("product")} for e in new_entities[:20]],
            "price_increases": len(price_spikes),
            "price_decreases": len(price_drops),
            "largest_increase": max(price_spikes, key=lambda e: e.get("price_change_pct", 0)) if price_spikes else None,
            "largest_decrease": min(price_drops, key=lambda e: e.get("price_change_pct", 0)) if price_drops else None,
        }

    # ── renderers ─────────────────────────────────────────────────

    def _render_markdown(self, sections: Dict[str, Any]) -> str:
        m = sections["metadata"]
        lines = [
            f"# Commodity OS Report - {m['report_type'].upper()}",
            "",
            f"**Generated:** {m['generated_at']}  ",
            f"**Period:** {m['period_start']} to {m['period_end']}  ",
            f"**Report ID:** {m['report_id']}",
            "",
            "---",
            "",
        ]
        lines.append("## Executive Summary\n")
        es = sections["executive_summary"]
        lines.append(f"- **Total Entities:** {es['total_entities']}")
        lines.append(f"- **Unique Products:** {es['unique_products']}")
        lines.append(f"- **Unique States:** {es['unique_states']}")
        lines.append(f"- **Average Market Price:** {es['average_market_price']}")
        for h in es.get("highlights", []):
            lines.append(f"- {h}")
        lines.append("")

        lines.append("## Data Collection Statistics\n")
        dc = sections["data_collection_stats"]
        for k, v in dc.items():
            lines.append(f"- **{k.replace('_', ' ').title()}:** {v}")
        lines.append("")

        lines.append("## Entity Breakdown\n")
        eb = sections["entity_breakdown"]
        for label, grouping in [("By Type", "by_type"), ("By Product", "by_product"), ("By State", "by_state")]:
            lines.append(f"### {label}\n")
            lines.append("| Category | Count |")
            lines.append("|----------|-------|")
            for cat, cnt in eb[grouping].items():
                lines.append(f"| {cat} | {cnt} |")
            lines.append("")

        lines.append("## Price Trends\n")
        pt = sections["price_trends"]
        lines.append("### Current Averages\n")
        lines.append("| Product | Avg Price |")
        lines.append("|---------|-----------|")
        for prod, avg in pt["current_averages"].items():
            lines.append(f"| {prod} | {avg} |")
        lines.append("")
        lines.append("### Trend Directions\n")
        for prod, direction in pt["trend_direction"].items():
            icon = {"up": "↑", "down": "↓", "stable": "→"}.get(direction, "?")
            lines.append(f"- **{prod}:** {icon} {direction}")
        lines.append("")

        lines.append("## Top Entities\n")
        for i, e in enumerate(sections["top_entities"], 1):
            lines.append(f"{i}. **{e.get('name', 'N/A')}** ({e.get('entity_type', 'N/A')}) - {e.get('product', 'N/A')} - {e.get('state', 'N/A')}")
        lines.append("")

        lines.append("## Data Quality Metrics\n")
        dq = sections["data_quality"]
        lines.append(f"**Overall Score:** {dq['overall_score']}%\n")
        lines.append("| Field | Completeness |")
        lines.append("|-------|-------------|")
        for field_name, pct in dq["completeness"].items():
            lines.append(f"| {field_name} | {pct}% |")
        lines.append(f"\n- Duplicates removed: {dq['duplicate_count']}")
        lines.append(f"- Stale records: {dq['stale_records']}")
        lines.append("")

        lines.append("## System Health Summary\n")
        sh = sections["system_health"]
        for k, v in sh.items():
            lines.append(f"- **{k.replace('_', ' ').title()}:** {v}")
        lines.append("")

        lines.append("## Notable Changes\n")
        nc = sections["notable_changes"]
        lines.append(f"- New entities: {nc['new_entities_count']}")
        lines.append(f"- Price increases (>10%): {nc['price_increases']}")
        lines.append(f"- Price decreases (<-10%): {nc['price_decreases']}")
        if nc.get("largest_increase"):
            li = nc["largest_increase"]
            lines.append(f"- Largest increase: {li.get('name')} ({li.get('price_change_pct', 0):.1f}%)")
        if nc.get("largest_decrease"):
            ld = nc["largest_decrease"]
            lines.append(f"- Largest decrease: {ld.get('name')} ({ld.get('price_change_pct', 0):.1f}%)")
        lines.append("")
        lines.append("---\n*Generated by Commodity OS Report Generator*\n")
        return "\n".join(lines)

    def _render_html(self, sections: Dict[str, Any], base_name: str) -> str:
        m = sections["metadata"]
        es = sections["executive_summary"]
        dc = sections["data_collection_stats"]
        eb = sections["entity_breakdown"]
        pt = sections["price_trends"]
        dq = sections["data_quality"]
        sh = sections["system_health"]
        nc = sections["notable_changes"]
        top = sections["top_entities"]

        product_labels = list(pt["current_averages"].keys())
        product_values = [pt["current_averages"][p] for p in product_labels]
        bar_max = max(product_values) if product_values else 1
        bars_html = ""
        for label, val in zip(product_labels, product_values):
            pct = (val / bar_max * 100) if bar_max else 0
            bars_html += f'<div class="bar-row"><span class="bar-label">{label}</span><div class="bar-track"><div class="bar-fill" style="width:{pct:.1f}%"></div></div><span class="bar-value">{val}</span></div>\n'

        type_rows = "".join(f"<tr><td>{k}</td><td>{v}</td></tr>" for k, v in eb["by_type"].items())
        product_rows = "".join(f"<tr><td>{k}</td><td>{v}</td></tr>" for k, v in eb["by_product"].items())
        state_rows = "".join(f"<tr><td>{k}</td><td>{v}</td></tr>" for k, v in eb["by_state"].items())
        quality_rows = "".join(f"<tr><td>{k}</td><td>{v}%</td></tr>" for k, v in dq["completeness"].items())
        top_rows = "".join(
            f"<tr><td>{i}</td><td>{e.get('name','N/A')}</td><td>{e.get('entity_type','N/A')}</td>"
            f"<td>{e.get('product','N/A')}</td><td>{e.get('state','N/A')}</td></tr>"
            for i, e in enumerate(top, 1)
        )

        health_score = sh.get("health_score", 100)
        health_color = "#22c55e" if health_score >= 80 else "#eab308" if health_score >= 50 else "#ef4444"

        new_entities_html = ""
        for ne in nc.get("new_entities", [])[:10]:
            new_entities_html += f"<li>{ne.get('name', 'N/A')} ({ne.get('product', 'N/A')})</li>"

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Commodity OS Report - {m['report_type'].upper()}</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'Segoe UI',system-ui,-apple-system,sans-serif;background:#f8fafc;color:#1e293b;line-height:1.6}}
.container{{max-width:1100px;margin:0 auto;padding:24px}}
header{{background:linear-gradient(135deg,#0f172a 0%,#1e40af 100%);color:#fff;padding:40px 32px;border-radius:12px;margin-bottom:32px}}
header h1{{font-size:28px;margin-bottom:8px}}
header .meta{{font-size:14px;opacity:.85}}
.toc{{background:#fff;border:1px solid #e2e8f0;border-radius:8px;padding:20px 24px;margin-bottom:32px}}
.toc h2{{font-size:16px;margin-bottom:12px;color:#475569}}
.toc ol{{padding-left:20px}}
.toc a{{color:#2563eb;text-decoration:none}}
.toc a:hover{{text-decoration:underline}}
section{{background:#fff;border:1px solid #e2e8f0;border-radius:8px;padding:24px;margin-bottom:24px}}
section h2{{font-size:20px;margin-bottom:16px;padding-bottom:8px;border-bottom:2px solid #e2e8f0;color:#1e293b}}
.stat-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:16px;margin-bottom:16px}}
.stat-card{{background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:16px;text-align:center}}
.stat-card .value{{font-size:28px;font-weight:700;color:#1e40af}}
.stat-card .label{{font-size:13px;color:#64748b;margin-top:4px}}
table{{width:100%;border-collapse:collapse;margin-top:12px}}
th,td{{padding:10px 14px;text-align:left;border-bottom:1px solid #e2e8f0;font-size:14px}}
th{{background:#f1f5f9;font-weight:600;color:#475569}}
tr:nth-child(even){{background:#f8fafc}}
.bar-row{{display:flex;align-items:center;margin-bottom:8px}}
.bar-label{{width:140px;font-size:13px;color:#475569;text-align:right;padding-right:12px;flex-shrink:0}}
.bar-track{{flex:1;background:#e2e8f0;border-radius:4px;height:22px;overflow:hidden}}
.bar-fill{{height:100%;background:linear-gradient(90deg,#2563eb,#7c3aed);border-radius:4px;transition:width .3s}}
.bar-value{{width:70px;font-size:13px;color:#1e293b;font-weight:600;padding-left:8px}}
.health-badge{{display:inline-block;padding:4px 12px;border-radius:12px;font-weight:600;font-size:13px;color:#fff;background:{health_color}}}
ul{{padding-left:20px;margin-top:8px}}
li{{margin-bottom:4px;font-size:14px}}
footer{{text-align:center;padding:24px;color:#94a3b8;font-size:13px}}
@media print{{
  body{{background:#fff}}
  header{{background:#0f172a!important;-webkit-print-color-adjust:exact;print-color-adjust:exact}}
  section{{break-inside:avoid}}
}}
</style>
</head>
<body>
<div class="container">
<header>
  <h1>Commodity OS Report &mdash; {m['report_type'].upper()}</h1>
  <div class="meta">
    Generated: {m['generated_at']}<br>
    Period: {m['period_start']} &mdash; {m['period_end']}<br>
    Report ID: {m['report_id']}
  </div>
</header>

<nav class="toc">
  <h2>Table of Contents</h2>
  <ol>
    <li><a href="#summary">Executive Summary</a></li>
    <li><a href="#collection">Data Collection Statistics</a></li>
    <li><a href="#breakdown">Entity Breakdown</a></li>
    <li><a href="#prices">Price Trends</a></li>
    <li><a href="#top">Top Entities</a></li>
    <li><a href="#quality">Data Quality Metrics</a></li>
    <li><a href="#health">System Health</a></li>
    <li><a href="#changes">Notable Changes</a></li>
  </ol>
</nav>

<section id="summary">
  <h2>Executive Summary</h2>
  <div class="stat-grid">
    <div class="stat-card"><div class="value">{es['total_entities']}</div><div class="label">Total Entities</div></div>
    <div class="stat-card"><div class="value">{es['unique_products']}</div><div class="label">Unique Products</div></div>
    <div class="stat-card"><div class="value">{es['unique_states']}</div><div class="label">Unique States</div></div>
    <div class="stat-card"><div class="value">{es['average_market_price']}</div><div class="label">Avg Market Price</div></div>
  </div>
  <ul>{"".join(f"<li>{h}</li>" for h in es.get('highlights', []))}</ul>
</section>

<section id="collection">
  <h2>Data Collection Statistics</h2>
  <table>
    <thead><tr><th>Metric</th><th>Value</th></tr></thead>
    <tbody>{"".join(f"<tr><td>{k.replace('_',' ').title()}</td><td>{v}</td></tr>" for k,v in dc.items())}</tbody>
  </table>
</section>

<section id="breakdown">
  <h2>Entity Breakdown</h2>
  <h3 style="font-size:15px;margin:12px 0 8px;color:#475569">By Type</h3>
  <table><thead><tr><th>Type</th><th>Count</th></tr></thead><tbody>{type_rows}</tbody></table>
  <h3 style="font-size:15px;margin:16px 0 8px;color:#475569">By Product</h3>
  <table><thead><tr><th>Product</th><th>Count</th></tr></thead><tbody>{product_rows}</tbody></table>
  <h3 style="font-size:15px;margin:16px 0 8px;color:#475569">By State</h3>
  <table><thead><tr><th>State</th><th>Count</th></tr></thead><tbody>{state_rows}</tbody></table>
</section>

<section id="prices">
  <h2>Price Trends</h2>
  <h3 style="font-size:15px;margin:0 0 12px;color:#475569">Current Average Prices</h3>
  {bars_html}
</section>

<section id="top">
  <h2>Top Entities</h2>
  <table>
    <thead><tr><th>#</th><th>Name</th><th>Type</th><th>Product</th><th>State</th></tr></thead>
    <tbody>{top_rows}</tbody>
  </table>
</section>

<section id="quality">
  <h2>Data Quality Metrics</h2>
  <p><strong>Overall Score:</strong> <span class="health-badge">{dq['overall_score']}%</span></p>
  <table>
    <thead><tr><th>Field</th><th>Completeness</th></tr></thead>
    <tbody>{quality_rows}</tbody>
  </table>
  <ul style="margin-top:12px">
    <li>Duplicates removed: {dq['duplicate_count']}</li>
    <li>Stale records: {dq['stale_records']}</li>
  </ul>
</section>

<section id="health">
  <h2>System Health Summary</h2>
  <div class="stat-grid">
    <div class="stat-card"><div class="value"><span class="health-badge">{health_score}</span></div><div class="label">Health Score</div></div>
    <div class="stat-card"><div class="value">{sh.get('uptime_hours',0)}</div><div class="label">Uptime (hrs)</div></div>
    <div class="stat-card"><div class="value">{sh.get('avg_latency_ms',0)}</div><div class="label">Avg Latency (ms)</div></div>
    <div class="stat-card"><div class="value">{sh.get('active_agents',0)}</div><div class="label">Active Agents</div></div>
  </div>
</section>

<section id="changes">
  <h2>Notable Changes</h2>
  <ul>
    <li>New entities: <strong>{nc['new_entities_count']}</strong></li>
    <li>Price increases (&gt;10%): <strong>{nc['price_increases']}</strong></li>
    <li>Price decreases (&lt;-10%): <strong>{nc['price_decreases']}</strong></li>
  </ul>
  {f"<h3 style='font-size:15px;margin:12px 0 8px;color:#475569'>New Entities</h3><ul>{new_entities_html}</ul>" if new_entities_html else ""}
</section>

<footer>
  Generated by Commodity OS Report Generator &mdash; {m['generated_at']}
</footer>
</div>
</body>
</html>"""

    # ── file I/O ──────────────────────────────────────────────────

    def _save_all(self, base: str, md: str, html: str, json_str: str) -> Dict[str, str]:
        paths: Dict[str, str] = {}
        for ext, content in ((".md", md), (".html", html), (".json", json_str)):
            fmt = ext.lstrip(".")
            path = self.output_dir / f"{base}{ext}"
            path.write_text(content, encoding="utf-8")
            paths[fmt] = str(path)
        return paths

    @staticmethod
    def _pct(part: int, whole: int) -> float:
        return round(part / whole * 100, 1) if whole else 0.0
