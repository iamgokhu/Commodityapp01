"""Event-driven core for the Market Intelligence OS."""
import asyncio
import logging
import time
from enum import Enum
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, Dict, List, Optional
from uuid import uuid4

logger = logging.getLogger(__name__)


class EventType(str, Enum):
    SYSTEM_START = "SystemStart"
    SYSTEM_STOP = "SystemStop"
    CYCLE_START = "CycleStart"
    CYCLE_COMPLETE = "CycleComplete"
    API_HEALTH_CHECK = "ApiHealthCheck"
    RESOURCE_CHECK = "ResourceCheck"
    CRAWLER_ASSIGNED = "CrawlerAssigned"
    CRAWLER_COMPLETED = "CrawlerCompleted"
    CRAWLER_FAILED = "CrawlerFailed"
    DATA_COLLECTED = "DataCollected"
    DATA_VALIDATED = "DataValidated"
    DATA_DEDUPED = "DataDeduped"
    DATA_CLEANED = "DataCleaned"
    DATA_NORMALIZED = "DataNormalized"
    DATA_TRANSLATED = "DataTranslated"
    ENTITY_RECOGNIZED = "EntityRecognized"
    COMMODITY_CLASSIFIED = "CommodityClassified"
    SECTOR_CLASSIFIED = "SectorClassified"
    KNOWLEDGE_GRAPH_UPDATED = "KnowledgeGraphUpdated"
    EMBEDDING_GENERATED = "EmbeddingGenerated"
    TREND_ANALYZED = "TrendAnalyzed"
    RISK_ANALYZED = "RiskAnalyzed"
    FORECAST_COMPLETED = "ForecastCompleted"
    EXECUTIVE_SUMMARY = "ExecutiveSummary"
    DASHBOARD_JSON = "DashboardJson"
    DASHBOARD_HTML = "DashboardHtml"
    DASHBOARD_PUBLISHED = "DashboardPublished"
    REPORT_GENERATED = "ReportGenerated"
    GITHUB_COMMIT = "GitHubCommit"
    GITHUB_PUSH = "GitHubPush"
    GITHUB_PAGES = "GitHubPages"
    NOTIFICATION_SENT = "NotificationSent"
    MONITORING_UPDATE = "MonitoringUpdate"
    SELF_EVAL = "SelfEval"
    NEWS_COLLECTED = "NewsCollected"
    TRADE_DATA_UPDATED = "TradeDataUpdated"
    PRICE_CHANGED = "PriceChanged"
    AGENT_COMPLETED = "AgentCompleted"
    AGENT_FAILED = "AgentFailed"
    RESOURCE_LOW = "ResourceLow"
    GPU_UNAVAILABLE = "GpuUnavailable"
    SESSION_DISCONNECTED = "SessionDisconnected"
    RECOVERY_COMPLETED = "RecoveryCompleted"
    ERROR = "Error"


@dataclass
class Event:
    event_type: EventType
    payload: Dict[str, Any] = field(default_factory=dict)
    source: str = ""
    event_id: str = field(default_factory=lambda: str(uuid4()))
    timestamp: float = field(default_factory=time.time)
    correlation_id: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3


EventHandler = Callable[[Event], Coroutine[Any, Any, None]]


class EventBus:
    def __init__(self):
        self._handlers: Dict[EventType, List[EventHandler]] = {}
        self._all_handlers: List[EventHandler] = []
        self._event_log: List[Event] = []
        self._max_log_size: int = 10000
        self._processing = False
        self._queue: asyncio.Queue = asyncio.Queue()
        self._stats: Dict[str, int] = {}

    def subscribe(self, event_type: EventType, handler: EventHandler):
        if event_type not in self._handlers:
            self._handlers[event_type] = []
        self._handlers[event_type].append(handler)

    def subscribe_all(self, handler: EventHandler):
        self._all_handlers.append(handler)

    async def publish(self, event: Event):
        self._event_log.append(event)
        if len(self._event_log) > self._max_log_size:
            self._event_log = self._event_log[-self._max_log_size:]

        self._stats[event.event_type.value] = self._stats.get(event.event_type.value, 0) + 1

        handlers = list(self._handlers.get(event.event_type, []))
        handlers.extend(self._all_handlers)

        if not handlers:
            logger.debug(f"No handlers for {event.event_type.value}")
            return

        tasks = []
        for handler in handlers:
            tasks.append(self._safe_call(handler, event))
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _safe_call(self, handler: EventHandler, event: Event):
        try:
            await handler(event)
        except Exception as e:
            logger.error(f"Handler {handler.__qualname__} failed on {event.event_type.value}: {e}")
            if event.retry_count < event.max_retries:
                event.retry_count += 1
                await self.publish(event)

    async def emit(self, event_type: EventType, payload: Dict[str, Any] = None, source: str = "", correlation_id: str = None):
        event = Event(
            event_type=event_type,
            payload=payload or {},
            source=source,
            correlation_id=correlation_id,
        )
        await self.publish(event)

    def get_stats(self) -> Dict[str, int]:
        return dict(self._stats)

    def get_recent_events(self, count: int = 50) -> List[Event]:
        return self._event_log[-count:]


event_bus = EventBus()
