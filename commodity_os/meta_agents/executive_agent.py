"""Meta Agent 3: Executive Intelligence - generates dashboards, reports, summaries."""
import logging
import time
from typing import Any, Dict, List

from commodity_os.core.events import Event, EventType, event_bus

logger = logging.getLogger(__name__)


class ExecutiveIntelligenceAgent:
    """Meta Agent 3: Generates dashboards, produces reports, creates summaries, publishes updates."""

    def __init__(self):
        self.reports_generated: List[Dict[str, Any]] = []
        self.dashboards_generated: List[Dict[str, Any]] = []
        self.summaries: List[Dict[str, Any]] = []
        self._running = False

    async def initialize(self):
        self._running = True
        event_bus.subscribe(EventType.CYCLE_COMPLETE, self._on_cycle_complete)
        event_bus.subscribe(EventType.KNOWLEDGE_GRAPH_UPDATED, self._on_kg_updated)
        event_bus.subscribe(EventType.FORECAST_COMPLETED, self._on_forecast_completed)
        logger.info("Executive Intelligence Agent initialized")

    async def _on_cycle_complete(self, event: Event):
        cycle = event.payload.get("cycle", 0)
        completed = event.payload.get("completed", 0)

        summary = {
            "cycle": cycle,
            "timestamp": time.time(),
            "tasks_completed": completed,
            "status": "success",
        }
        self.summaries.append(summary)

        await event_bus.emit(EventType.EXECUTIVE_SUMMARY, {
            "summary": summary,
        }, source="executive_intelligence")

        await event_bus.emit(EventType.DASHBOARD_JSON, {
            "action": "generate",
            "cycle": cycle,
        }, source="executive_intelligence")

        await event_bus.emit(EventType.REPORT_GENERATED, {
            "report_type": "cycle_summary",
            "cycle": cycle,
        }, source="executive_intelligence")

        logger.info(f"Executive Intelligence: Generated summary for cycle {cycle}")

    async def _on_kg_updated(self, event: Event):
        stats = event.payload.get("stats", {})
        logger.info(f"Executive Intelligence: KG updated - {stats.get('total_nodes', 0)} nodes")

    async def _on_forecast_completed(self, event: Event):
        forecasts = event.payload.get("forecasts", [])
        logger.info(f"Executive Intelligence: Received {len(forecasts)} forecasts")

    def get_executive_summary(self) -> Dict[str, Any]:
        return {
            "total_cycles": len(self.summaries),
            "recent_summaries": self.summaries[-5:],
            "reports_generated": len(self.reports_generated),
            "dashboards_generated": len(self.dashboards_generated),
        }

    async def shutdown(self):
        self._running = False
        logger.info("Executive Intelligence Agent shut down")
