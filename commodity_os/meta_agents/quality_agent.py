"""Meta Agent 2: Quality & Reliability Supervisor - validates outputs, requests reprocessing."""
import logging
import time
from typing import Any, Dict, List

from commodity_os.core.events import Event, EventType, event_bus

logger = logging.getLogger(__name__)


class QualityAgent:
    """Meta Agent 2: Detects low-quality outputs, validates results, maintains audit logs."""

    def __init__(self):
        self.quality_scores: Dict[str, float] = {}
        self.audit_log: List[Dict[str, Any]] = []
        self.reprocessing_requests: List[str] = []
        self._running = False

    async def initialize(self):
        self._running = True
        event_bus.subscribe(EventType.DATA_VALIDATED, self._on_data_validated)
        event_bus.subscribe(EventType.DATA_DEDUPED, self._on_data_deduped)
        event_bus.subscribe(EventType.CRAWLER_COMPLETED, self._on_crawler_completed)
        event_bus.subscribe(EventType.DASHBOARD_HTML, self._on_dashboard_generated)
        event_bus.subscribe(EventType.REPORT_GENERATED, self._on_report_generated)
        logger.info("Quality Agent initialized")

    async def _on_data_validated(self, event: Event):
        stats = event.payload.get("stats", {})
        total = stats.get("total", 0)
        valid = stats.get("valid", 0)
        if total > 0:
            score = valid / total
            self.quality_scores["data_validation"] = score
            self._audit("data_validation", score, stats)
            if score < 0.7:
                logger.warning(f"Low data quality score: {score:.2%}")
                self.reprocessing_requests.append("data_validation")

    async def _on_data_deduped(self, event: Event):
        stats = event.payload.get("stats", {})
        total = stats.get("total", 0)
        unique = stats.get("unique", 0)
        if total > 0:
            dedup_rate = 1 - (unique / total) if total > 0 else 0
            self.quality_scores["deduplication"] = dedup_rate
            self._audit("deduplication", dedup_rate, stats)

    async def _on_crawler_completed(self, event: Event):
        records = event.payload.get("records_collected", 0)
        crawler = event.payload.get("crawler", "unknown")
        score = min(1.0, records / 100) if records > 0 else 0
        self.quality_scores[f"crawler_{crawler}"] = score
        self._audit(f"crawler_{crawler}", score, {"records": records})

    async def _on_dashboard_generated(self, event: Event):
        self._audit("dashboard_generation", 1.0, {"status": "success"})

    async def _on_report_generated(self, event: Event):
        self._audit("report_generation", 1.0, {"status": "success"})

    def _audit(self, component: str, score: float, details: Dict[str, Any]):
        entry = {
            "timestamp": time.time(),
            "component": component,
            "score": score,
            "details": details,
            "level": "INFO" if score >= 0.8 else ("WARNING" if score >= 0.5 else "CRITICAL"),
        }
        self.audit_log.append(entry)
        if len(self.audit_log) > 1000:
            self.audit_log = self.audit_log[-500:]

    def get_overall_quality_score(self) -> float:
        if not self.quality_scores:
            return 1.0
        return sum(self.quality_scores.values()) / len(self.quality_scores)

    def get_quality_report(self) -> Dict[str, Any]:
        return {
            "overall_score": self.get_overall_quality_score(),
            "component_scores": dict(self.quality_scores),
            "audit_log_size": len(self.audit_log),
            "reprocessing_requests": len(self.reprocessing_requests),
            "recent_audits": self.audit_log[-10:],
        }

    async def shutdown(self):
        self._running = False
        logger.info("Quality Agent shut down")
