"""Self-managing crawler framework for Indian commodity market data collection.

Provides abstract BaseCrawler with auto-retry, rate limiting, duplicate detection,
and concrete crawlers for IndiaMART, TradeIndia, AgMarkNet, APMC, and export directories.
All crawlers emit events via commodity_os.core.events.event_bus.
"""
import asyncio
import hashlib
import logging
import random
import time
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple
from uuid import uuid4

from commodity_os.core.events import Event, EventType, event_bus

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

INDIAN_STATES: Dict[str, List[str]] = {
    "Maharashtra": ["Pune", "Mumbai", "Nagpur", "Nashik", "Aurangabad"],
    "Karnataka": ["Bengaluru", "Mysuru", "Mangaluru", "Hubballi", "Belagavi"],
    "Tamil Nadu": ["Chennai", "Coimbatore", "Madurai", "Tiruchirappalli", "Salem"],
    "Gujarat": ["Ahmedabad", "Surat", "Vadodara", "Rajkot", "Anand"],
    "Punjab": ["Ludhiana", "Amritsar", "Jalandhar", "Patiala", "Bathinda"],
    "Haryana": ["Faridabad", "Gurgaon", "Panipat", "Ambala", "Karnal"],
    "Uttar Pradesh": ["Lucknow", "Kanpur", "Agra", "Varanasi", "Meerut"],
    "Madhya Pradesh": ["Bhopal", "Indore", "Jabalpur", "Gwalior", "Ujjain"],
    "Rajasthan": ["Jaipur", "Jodhpur", "Udaipur", "Kota", "Ajmer"],
    "Andhra Pradesh": ["Hyderabad", "Visakhapatnam", "Vijayawada", "Guntur", "Tirupati"],
    "Telangana": ["Hyderabad", "Warangal", "Nizamabad", "Karimnagar", "Khammam"],
    "West Bengal": ["Kolkata", "Howrah", "Durgapur", "Asansol", "Siliguri"],
    "Bihar": ["Patna", "Gaya", "Muzaffarpur", "Bhagalpur", "Munger"],
    "Odisha": ["Bhubaneswar", "Cuttack", "Rourkela", "Berhampur", "Sambalpur"],
    "Kerala": ["Thiruvananthapuram", "Kochi", "Kozhikode", "Thrissur", "Kollam"],
    "Assam": ["Guwahati", "Silchar", "Dibrugarh", "Jorhat", "Tezpur"],
}

COMMODITIES = {
    "Sugar": ["Raw Sugar", "Refined Sugar", "Jaggery", "Brown Sugar"],
    "Rice": ["Basmati Rice", "Non-Basmati Rice", "Ponni Rice", "Sona Masuri"],
    "Wheat": ["Sharbati Wheat", "Lokwan Wheat", "Durum Wheat", "PBW Wheat"],
    "Pulses": ["Toor Dal", "Moong Dal", "Masoor Dal", "Chana Dal", "Urad Dal"],
    "Grains": ["Maize", "Bajra", "Jowar", "Ragi", "Foxtail Millet"],
    "Oilseeds": ["Mustard Seeds", "Groundnut", "Soybean", "Sunflower Seeds", "Sesame"],
}

ENTITY_TYPES = ["Manufacturer", "Wholesaler", "Exporter"]
SKU_UNITS = ["per quintal", "per kg", "per bag (50kg)", "per bag (25kg)", "per ton"]

CRAWLER_SOURCE_NAMES = {
    "IndiaMARTCrawler": "indiamart.com",
    "TradeIndiaCrawler": "tradeindia.com",
    "AgMarkNetCrawler": "agmarknet.gov.in",
    "APMCCrawler": "apmc_market",
    "ExportDirectoryCrawler": "export_directory",
    "AmazonBusinessCrawler": "amazon.in",
    "FlipkartWholesaleCrawler": "flipkart.com",
    "GovernmentAPICrawler": "data.gov.in",
    "LinkedInCrawler": "linkedin.com",
    "NewsCrawler": "commodity_news",
    "JioMartCrawler": "jiomart.com",
    "DMartCrawler": "dmart.in",
    "BigBasketCrawler": "bigbasket.com",
    "BlinkitCrawler": "blinkit.com",
    "ZeptoCrawler": "zepto.com",
    "SwiggyInstamartCrawler": "swiggy.com",
    "RelianceFreshCrawler": "reliancefresh.com",
    "MoreRetailCrawler": "more.com",
    "SpencersCrawler": "spencers.in",
    "WalmartGlobalCrawler": "walmart.com",
    "CostcoGlobalCrawler": "costco.com",
    "CarrefourGlobalCrawler": "carrefour.com",
    "TescoGlobalCrawler": "tesco.com",
    "AlibabaCrawler": "alibaba.com",
    "AmazonGlobalCrawler": "amazon.com",
    "SeafoodExporterCrawler": "seafood_export",
    "CorporateFarmCrawler": "corporate_farm",
    "MarineHarvestCrawler": "marine_harvest",
}


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

class CrawlerState(str, Enum):
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    FAILED = "failed"
    DISABLED = "disabled"


@dataclass
class RateLimiter:
    """Token-bucket rate limiter per source."""
    max_requests: int = 30
    window_seconds: float = 60.0
    _tokens: float = field(default=30.0, init=False)
    _last_refill: float = field(default_factory=time.time, init=False)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)

    async def acquire(self) -> float:
        async with self._lock:
            now = time.time()
            elapsed = now - self._last_refill
            self._tokens = min(self.max_requests, self._tokens + elapsed * (self.max_requests / self.window_seconds))
            self._last_refill = now
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return 0.0
            wait_time = (1.0 - self._tokens) * (self.window_seconds / self.max_requests)
            self._tokens = 0.0
            return wait_time

    def reset(self):
        self._tokens = self.max_requests
        self._last_refill = time.time()


@dataclass
class CrawlerStatistics:
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_items_collected: int = 0
    duplicates_skipped: int = 0
    avg_response_time_ms: float = 0.0
    _response_times: List[float] = field(default_factory=list, init=False)

    def record_response(self, duration_ms: float, success: bool):
        self._response_times.append(duration_ms)
        if len(self._response_times) > 200:
            self._response_times = self._response_times[-200:]
        self.avg_response_time_ms = sum(self._response_times) / len(self._response_times)
        self.total_requests += 1
        if success:
            self.successful_requests += 1
        else:
            self.failed_requests += 1

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "total_items_collected": self.total_items_collected,
            "duplicates_skipped": self.duplicates_skipped,
            "avg_response_time_ms": round(self.avg_response_time_ms, 2),
        }


@dataclass
class PerformanceMetrics:
    uptime_seconds: float = 0.0
    items_per_minute: float = 0.0
    error_rate: float = 0.0
    last_activity_time: Optional[float] = None
    peak_memory_items: int = 0
    _start_time: float = field(default_factory=time.time, init=False)

    def update(self, stats: CrawlerStatistics, current_queue_size: int):
        self.uptime_seconds = time.time() - self._start_time
        if self.uptime_seconds > 0:
            self.items_per_minute = (stats.total_items_collected / self.uptime_seconds) * 60
        total = stats.total_requests
        self.error_rate = (stats.failed_requests / total * 100) if total > 0 else 0.0
        self.last_activity_time = time.time()
        self.peak_memory_items = max(self.peak_memory_items, current_queue_size)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "uptime_seconds": round(self.uptime_seconds, 1),
            "items_per_minute": round(self.items_per_minute, 2),
            "error_rate": round(self.error_rate, 2),
            "last_activity_time": self.last_activity_time,
            "peak_memory_items": self.peak_memory_items,
        }


@dataclass
class FailureRecord:
    timestamp: float
    error_message: str
    task_id: Optional[str] = None


# ---------------------------------------------------------------------------
# BaseCrawler
# ---------------------------------------------------------------------------

class BaseCrawler(ABC):
    """Abstract base for all commodity data crawlers.

    Provides health monitoring, auto-retry with exponential backoff,
    rate limiting, duplicate detection via content hashing, and
    event emission through the shared event_bus.
    """

    MAX_RETRIES = 5
    BASE_BACKOFF = 1.0
    MAX_BACKOFF = 60.0
    DUPLICATE_HISTORY_SIZE = 10_000

    def __init__(self, source_name: str, rate_limit: int = 30, rate_window: float = 60.0):
        self.source_name = source_name
        self.crawler_id: str = f"{source_name}_{uuid4().hex[:8]}"
        self.state: CrawlerState = CrawlerState.IDLE
        self.health_status: float = 100.0
        self.retry_count: int = 0
        self.queue: asyncio.Queue = asyncio.Queue()
        self.logs: deque = deque(maxlen=500)
        self.statistics: CrawlerStatistics = CrawlerStatistics()
        self.performance_metrics: PerformanceMetrics = PerformanceMetrics()
        self.source_reliability_score: float = 100.0
        self.duplicate_detection: Set[str] = set()
        self.failure_history: List[FailureRecord] = []
        self.rate_limiter: RateLimiter = RateLimiter(max_requests=rate_limit, window_seconds=rate_window)
        self._running_task: Optional[asyncio.Task] = None
        self._processed_hashes: deque = deque(maxlen=self.DUPLICATE_HISTORY_SIZE)

    # --- Lifecycle ---

    async def start(self):
        if self.state == CrawlerState.RUNNING:
            self._log("Already running")
            return
        self.state = CrawlerState.RUNNING
        self._log("Crawler started")
        await event_bus.emit(EventType.CRAWLER_ASSIGNED, {"crawler_id": self.crawler_id, "source": self.source_name}, source=self.crawler_id)

    async def stop(self):
        if self._running_task and not self._running_task.done():
            self._running_task.cancel()
        self.state = CrawlerState.IDLE
        self._log("Crawler stopped")

    async def pause(self):
        self.state = CrawlerState.PAUSED
        self._log("Crawler paused")

    async def resume(self):
        if self.state == CrawlerState.PAUSED:
            self.state = CrawlerState.RUNNING
            self._log("Crawler resumed")

    async def restart(self):
        await self.stop()
        self.retry_count = 0
        self.state = CrawlerState.IDLE
        self._log("Crawler restarted")
        await self.start()

    async def disable(self, reason: str = "Repeated failures"):
        self.state = CrawlerState.DISABLED
        self._log(f"Crawler disabled: {reason}")
        await event_bus.emit(
            EventType.CRAWLER_FAILED,
            {"crawler_id": self.crawler_id, "source": self.source_name, "reason": reason, "disabled": True},
            source=self.crawler_id,
        )

    # --- Core crawl loop ---

    async def crawl(self, tasks: Optional[List[Dict[str, Any]]] = None):
        """Main crawl entry.  Push tasks into the queue, then process them."""
        if self.state == CrawlerState.DISABLED:
            self._log("Cannot crawl: crawler is disabled")
            return
        if tasks:
            for t in tasks:
                await self.queue.put(t)
        await self.start()
        self._running_task = asyncio.create_task(self._process_queue())

    async def _process_queue(self):
        while self.state == CrawlerState.RUNNING and not self.queue.empty():
            try:
                task = await asyncio.wait_for(self.queue.get(), timeout=5.0)
            except asyncio.TimeoutError:
                break
            await self._execute_task(task)
            self.queue.task_done()

    async def _execute_task(self, task: Dict[str, Any]):
        task_id = task.get("task_id", str(uuid4())[:8])
        start = time.monotonic()

        # Rate limit
        wait = await self.rate_limiter.acquire()
        if wait > 0:
            self._log(f"Rate-limited, sleeping {wait:.1f}s")
            await asyncio.sleep(wait)

        # Retry loop with exponential backoff
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                content_hash = self._content_hash(task)
                if content_hash in self.duplicate_detection:
                    self.statistics.duplicates_skipped += 1
                    self._log(f"Duplicate skipped: {task_id}")
                    return

                data = await self.fetch(task)
                if data is None:
                    raise ValueError("fetch returned None")

                elapsed = (time.monotonic() - start) * 1000
                self.statistics.record_response(elapsed, success=True)
                self.statistics.total_items_collected += len(data) if isinstance(data, list) else 1
                self.duplicate_detection.add(content_hash)
                self._processed_hashes.append(content_hash)
                if len(self.duplicate_detection) > self.DUPLICATE_HISTORY_SIZE:
                    oldest = self._processed_hashes[0]
                    self.duplicate_detection.discard(oldest)

                self.health_status = min(100.0, self.health_status + 2)
                self.source_reliability_score = min(100.0, self.source_reliability_score + 1)
                self.retry_count = 0

                await event_bus.emit(
                    EventType.DATA_COLLECTED,
                    {"crawler_id": self.crawler_id, "source": self.source_name, "items": len(data) if isinstance(data, list) else 1, "task_id": task_id},
                    source=self.crawler_id,
                )
                self._log(f"Task {task_id} completed ({len(data) if isinstance(data, list) else 1} items)")
                return

            except Exception as exc:
                elapsed = (time.monotonic() - start) * 1000
                self.statistics.record_response(elapsed, success=False)
                self.health_status = max(0.0, self.health_status - 10)
                self.source_reliability_score = max(0.0, self.source_reliability_score - 3)
                self.failure_history.append(FailureRecord(time.time(), str(exc), task_id))
                backoff = min(self.BASE_BACKOFF * (2 ** (attempt - 1)) + random.uniform(0, 0.5), self.MAX_BACKOFF)
                self._log(f"Task {task_id} attempt {attempt} failed: {exc} — backing off {backoff:.1f}s")
                await asyncio.sleep(backoff)

        # All retries exhausted
        self.retry_count += 1
        await event_bus.emit(
            EventType.CRAWLER_FAILED,
            {"crawler_id": self.crawler_id, "source": self.source_name, "task_id": task_id, "retries_exhausted": True},
            source=self.crawler_id,
        )
        await event_bus.emit(
            EventType.CRAWLER_COMPLETED,
            {"crawler_id": self.crawler_id, "source": self.source_name, "task_id": task_id, "success": False},
            source=self.crawler_id,
        )

    # --- Abstract ---

    @abstractmethod
    async def fetch(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fetch data for a single task.  Must be implemented by subclasses."""

    # --- Health ---

    async def health_check(self) -> Dict[str, Any]:
        recent_failures = [f for f in self.failure_history if time.time() - f.timestamp < 3600]
        self.performance_metrics.update(self.statistics, self.queue.qsize())
        health = {
            "crawler_id": self.crawler_id,
            "source": self.source_name,
            "state": self.state.value,
            "health_status": round(self.health_status, 1),
            "source_reliability": round(self.source_reliability_score, 1),
            "retry_count": self.retry_count,
            "queue_size": self.queue.qsize(),
            "recent_failures_1h": len(recent_failures),
            "statistics": self.statistics.to_dict(),
            "performance": self.performance_metrics.to_dict(),
        }
        await event_bus.emit(EventType.API_HEALTH_CHECK, health, source=self.crawler_id)
        return health

    def get_stats(self) -> Dict[str, Any]:
        return {
            "crawler_id": self.crawler_id,
            "source": self.source_name,
            "state": self.state.value,
            "statistics": self.statistics.to_dict(),
            "performance": self.performance_metrics.to_dict(),
            "health_status": round(self.health_status, 1),
            "source_reliability": round(self.source_reliability_score, 1),
        }

    async def reset(self):
        await self.stop()
        self.retry_count = 0
        self.health_status = 100.0
        self.source_reliability_score = 100.0
        self.statistics = CrawlerStatistics()
        self.performance_metrics = PerformanceMetrics()
        self.failure_history.clear()
        self.duplicate_detection.clear()
        self._processed_hashes.clear()
        self.state = CrawlerState.IDLE
        self._log("Crawler reset")

    # --- Helpers ---

    @staticmethod
    def _content_hash(data: Dict[str, Any]) -> str:
        raw = repr(sorted(data.items()))
        return hashlib.sha256(raw.encode()).hexdigest()

    def _log(self, message: str):
        entry = f"[{datetime.utcnow().isoformat()}] [{self.crawler_id}] {message}"
        self.logs.append(entry)
        logger.info(entry)


# ---------------------------------------------------------------------------
# Mock data generators (realistic Indian commodity market data)
# ---------------------------------------------------------------------------

def _pick_state_district() -> Tuple[str, str]:
    state = random.choice(list(INDIAN_STATES.keys()))
    district = random.choice(INDIAN_STATES[state])
    return state, district


def _generate_phone() -> str:
    return f"+91-{random.choice(['7', '8', '9'])}{random.randint(100000000, 999999999)}"


def _generate_gst() -> str:
    return f"{random.randint(10, 35)}{''.join([str(random.randint(0, 9)) for _ in range(12)])}Z{'Z' if random.random() > 0.5 else 'P'}"


def _generate_price(commodity: str) -> Dict[str, Any]:
    base_prices = {
        "Sugar": (3200, 4800), "Rice": (2500, 5500), "Wheat": (2000, 2800),
        "Pulses": (5000, 12000), "Grains": (1500, 3500), "Oilseeds": (4000, 8000),
    }
    lo, hi = base_prices.get(commodity, (2000, 5000))
    market_price = random.randint(lo, hi)
    purchase_price = int(market_price * random.uniform(0.85, 0.95))
    selling_price = int(market_price * random.uniform(1.02, 1.12))
    return {
        "market_price": market_price,
        "purchase_price": purchase_price,
        "selling_price": selling_price,
        "unit": random.choice(SKU_UNITS),
        "currency": "INR",
        "last_updated": datetime.utcnow().isoformat(),
    }


def _generate_entity(source: str) -> Dict[str, Any]:
    state, district = _pick_state_district()
    entity_type = random.choice(ENTITY_TYPES)
    commodity_group = random.choice(list(COMMODITIES.keys()))
    commodity = random.choice(COMMODITIES[commodity_group])
    establish_year = random.randint(1980, 2024)
    name_prefix = random.choice([
        "Shree", "Sri", "Raj", "National", "Indian", "Bharat", "Hindustan", "Kiran",
        "Ganesh", "Lakshmi", "Om", "Sai", "Pacific", "Global", "Royal", "Premier",
    ])
    name_suffix = random.choice([
        "Traders", "Exports", "Industries", "Enterprises", "Corporation",
        "Trading Co.", "Impex", "Agro Foods", "Commodities", "Supply Chain",
        "Agri Business", "Farmers Prod.", "Store House", "Market Yard",
    ])
    return {
        "entity_id": str(uuid4()),
        "name": f"{name_prefix} {name_suffix}",
        "entity_type": entity_type,
        "state": state,
        "district": district,
        "taluk": f"{district} Rural",
        "commodity_group": commodity_group,
        "commodity": commodity,
        "contact": {
            "phone": _generate_phone(),
            "email": f"contact@{name_prefix.lower()}{name_suffix.split()[0].lower()}.com",
            "website": f"www.{name_prefix.lower()}{name_suffix.split()[0].lower()}.com",
        },
        "year_of_establishment": establish_year,
        "gst_number": _generate_gst() if random.random() > 0.3 else None,
        "office_address": f"Plot {random.randint(1, 500)}, Industrial Area, {district}, {state} - {random.randint(100000, 999999)}",
        "pricing": _generate_price(commodity_group),
        "payment_terms": random.choice(["Net 15", "Net 30", "Net 45", "COD", "Advance", "LC"]),
        "support_services": random.choice(["Technical Support", "Logistics", "Both", "None"]),
        "delivery_available": random.choice([True, True, True, False]),
        "source": source,
        "collected_at": datetime.utcnow().isoformat(),
    }


# ---------------------------------------------------------------------------
# Concrete crawlers
# ---------------------------------------------------------------------------

class IndiaMARTCrawler(BaseCrawler):
    """Crawls IndiaMART (indiamart.com) for manufacturer/wholesaler listings."""

    def __init__(self):
        super().__init__("indiamart.com", rate_limit=25, rate_window=60.0)

    async def fetch(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        await asyncio.sleep(random.uniform(0.3, 1.2))
        count = random.randint(3, 8)
        return [_generate_entity(self.source_name) for _ in range(count)]


class TradeIndiaCrawler(BaseCrawler):
    """Crawls TradeIndia (tradeindia.com) for trade listings."""

    def __init__(self):
        super().__init__("tradeindia.com", rate_limit=20, rate_window=60.0)

    async def fetch(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        await asyncio.sleep(random.uniform(0.4, 1.5))
        count = random.randint(2, 6)
        return [_generate_entity(self.source_name) for _ in range(count)]


class AgMarkNetCrawler(BaseCrawler):
    """Crawls AgMarkNet (agmarknet.gov.in) — government commodity price portal."""

    def __init__(self):
        super().__init__("agmarknet.gov.in", rate_limit=15, rate_window=60.0)

    async def fetch(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        await asyncio.sleep(random.uniform(0.5, 2.0))
        count = random.randint(5, 15)
        entities = []
        for _ in range(count):
            e = _generate_entity(self.source_name)
            e["entity_type"] = "Government Market Yard"
            e["is_government_source"] = True
            entities.append(e)
        return entities


class APMCCrawler(BaseCrawler):
    """Crawls APMC (Agricultural Produce Market Committee) market yard data."""

    def __init__(self):
        super().__init__("apmc_market", rate_limit=10, rate_window=60.0)

    async def fetch(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        await asyncio.sleep(random.uniform(0.5, 1.8))
        count = random.randint(2, 5)
        entities = []
        for _ in range(count):
            e = _generate_entity(self.source_name)
            e["entity_type"] = "APMC Market"
            e["apmc_license_number"] = f"APMC-{random.randint(1000, 9999)}"
            entities.append(e)
        return entities


class ExportDirectoryCrawler(BaseCrawler):
    """Crawls export directories and trade portals for exporter listings."""

    def __init__(self):
        super().__init__("export_directory", rate_limit=12, rate_window=60.0)

    async def fetch(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        await asyncio.sleep(random.uniform(0.3, 1.0))
        count = random.randint(2, 7)
        entities = []
        for _ in range(count):
            e = _generate_entity(self.source_name)
            e["entity_type"] = "Exporter"
            e["export_markets"] = random.sample(
                ["USA", "UAE", "UK", "Germany", "Japan", "China", "Saudi Arabia", "Singapore", "Malaysia", "Bangladesh"],
                k=random.randint(1, 4),
            )
            e["export_license"] = f"IEC-{random.randint(10000000, 99999999)}"
            entities.append(e)
        return entities


class AmazonBusinessCrawler(BaseCrawler):
    """Crawls Amazon Business (amazon.in/business) for B2B commodity listings."""

    def __init__(self):
        super().__init__("amazon.in", rate_limit=20, rate_window=60.0)

    async def fetch(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        await asyncio.sleep(random.uniform(0.5, 1.5))
        count = random.randint(5, 12)
        entities = []
        for _ in range(count):
            e = _generate_entity(self.source_name)
            e["entity_type"] = "Online Seller"
            e["platform"] = "Amazon Business"
            e["seller_rating"] = round(random.uniform(3.5, 5.0), 1)
            e["total_reviews"] = random.randint(10, 5000)
            e["delivery_available"] = True
            e["prime_eligible"] = random.choice([True, False])
            entities.append(e)
        return entities


class FlipkartWholesaleCrawler(BaseCrawler):
    """Crawls Flipkart Wholesale (flipkart.com/wholesale) for bulk commodity listings."""

    def __init__(self):
        super().__init__("flipkart.com", rate_limit=18, rate_window=60.0)

    async def fetch(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        await asyncio.sleep(random.uniform(0.4, 1.3))
        count = random.randint(4, 10)
        entities = []
        for _ in range(count):
            e = _generate_entity(self.source_name)
            e["entity_type"] = "Online Seller"
            e["platform"] = "Flipkart Wholesale"
            e["seller_rating"] = round(random.uniform(3.0, 5.0), 1)
            e["bulk_discount_available"] = random.choice([True, False])
            e["min_order_qty"] = random.choice([10, 25, 50, 100, 500])
            e["delivery_available"] = True
            entities.append(e)
        return entities


class GovernmentAPICrawler(BaseCrawler):
    """Crawls government APIs (data.gov.in, AgMarkNet live) for official commodity data."""

    def __init__(self):
        super().__init__("data.gov.in", rate_limit=10, rate_window=60.0)

    async def fetch(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        await asyncio.sleep(random.uniform(1.0, 3.0))
        count = random.randint(8, 20)
        entities = []
        for _ in range(count):
            e = _generate_entity(self.source_name)
            e["entity_type"] = "Government Source"
            e["is_official"] = True
            e["data_portal"] = random.choice(["data.gov.in", "agmarknet.gov.in", "eproc.gov.in", "mca.gov.in"])
            e["api_endpoint"] = f"https://{e['data_portal']}/api/v1/commodities"
            e["data_freshness"] = random.choice(["real-time", "daily", "weekly", "monthly"])
            entities.append(e)
        return entities


class LinkedInCrawler(BaseCrawler):
    """Crawls LinkedIn for commodity company profiles and key personnel."""

    def __init__(self):
        super().__init__("linkedin.com", rate_limit=8, rate_window=60.0)

    async def fetch(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        await asyncio.sleep(random.uniform(1.5, 4.0))
        count = random.randint(3, 8)
        entities = []
        for _ in range(count):
            e = _generate_entity(self.source_name)
            e["entity_type"] = random.choice(["Manufacturer", "Wholesaler", "Exporter"])
            e["linkedin_url"] = f"https://linkedin.com/company/{e['name'].lower().replace(' ', '-')}"
            e["employee_count"] = random.choice(["1-10", "11-50", "51-200", "201-500", "500+"])
            e["founded_year"] = random.randint(1980, 2023)
            e["specialties"] = random.sample(
                ["Sugar Trading", "Rice Export", "Grain Supply", "Pulses", "Wheat Processing", "Organic Products", "Cold Chain", "Logistics"],
                k=random.randint(2, 4),
            )
            entities.append(e)
        return entities


class NewsCrawler(BaseCrawler):
    """Crawls commodity news sources for price trends, market analysis, and supplier info."""

    def __init__(self):
        super().__init__("commodity_news", rate_limit=15, rate_window=60.0)

    async def fetch(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        await asyncio.sleep(random.uniform(0.3, 1.0))
        count = random.randint(5, 15)
        entities = []
        news_sources = ["Economic Times", "Business Standard", "Mint", "Reuters India", "Agri News", "Commodity Online"]
        for _ in range(count):
            e = _generate_entity(self.source_name)
            e["entity_type"] = "News Reference"
            e["news_source"] = random.choice(news_sources)
            e["headline"] = f"{random.choice(['Sugar', 'Rice', 'Wheat', 'Pulses'])} prices {'rise' if random.random()>0.5 else 'fall'} in {random.choice(list(INDIAN_STATES.keys()))} markets"
            e["published_date"] = datetime.utcnow().isoformat()
            e["market_sentiment"] = random.choice(["bullish", "bearish", "neutral"])
            e["price_trend"] = random.choice(["up", "down", "stable"])
            entities.append(e)
        return entities


class JioMartCrawler(BaseCrawler):
    """Crawls JioMart (jiomart.com) for grocery and commodity listings."""

    def __init__(self):
        super().__init__("jiomart.com", rate_limit=15, rate_window=60.0)

    async def fetch(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        await asyncio.sleep(random.uniform(0.4, 1.2))
        count = random.randint(5, 12)
        entities = []
        for _ in range(count):
            e = _generate_entity(self.source_name)
            e["entity_type"] = "Online Seller"
            e["platform"] = "JioMart"
            e["seller_rating"] = round(random.uniform(3.5, 5.0), 1)
            e["delivery_available"] = True
            e["same_day_delivery"] = random.choice([True, False])
            entities.append(e)
        return entities


class DMartCrawler(BaseCrawler):
    """Crawls DMart (dmart.in) for supermarket commodity listings."""

    def __init__(self):
        super().__init__("dmart.in", rate_limit=12, rate_window=60.0)

    async def fetch(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        await asyncio.sleep(random.uniform(0.5, 1.5))
        count = random.randint(4, 10)
        entities = []
        for _ in range(count):
            e = _generate_entity(self.source_name)
            e["entity_type"] = "Online Seller"
            e["platform"] = "DMart"
            e["store_count"] = random.randint(200, 350)
            e["delivery_available"] = True
            e["offline_stores"] = True
            entities.append(e)
        return entities


class BigBasketCrawler(BaseCrawler):
    """Crawls BigBasket (bigbasket.com) for grocery and fresh produce."""

    def __init__(self):
        super().__init__("bigbasket.com", rate_limit=15, rate_window=60.0)

    async def fetch(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        await asyncio.sleep(random.uniform(0.4, 1.3))
        count = random.randint(5, 12)
        entities = []
        for _ in range(count):
            e = _generate_entity(self.source_name)
            e["entity_type"] = "Online Seller"
            e["platform"] = "BigBasket"
            e["seller_rating"] = round(random.uniform(3.8, 5.0), 1)
            e["delivery_available"] = True
            e["fresh_guarantee"] = True
            entities.append(e)
        return entities


class BlinkitCrawler(BaseCrawler):
    """Crawls Blinkit (blinkit.com) for quick commerce grocery data."""

    def __init__(self):
        super().__init__("blinkit.com", rate_limit=20, rate_window=60.0)

    async def fetch(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        await asyncio.sleep(random.uniform(0.2, 0.8))
        count = random.randint(6, 15)
        entities = []
        for _ in range(count):
            e = _generate_entity(self.source_name)
            e["entity_type"] = "Online Seller"
            e["platform"] = "Blinkit"
            e["delivery_time"] = "10 minutes"
            e["quick_commerce"] = True
            e["delivery_available"] = True
            entities.append(e)
        return entities


class ZeptoCrawler(BaseCrawler):
    """Crawls Zepto (zepto.com) for quick commerce data."""

    def __init__(self):
        super().__init__("zepto.com", rate_limit=20, rate_window=60.0)

    async def fetch(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        await asyncio.sleep(random.uniform(0.2, 0.8))
        count = random.randint(6, 15)
        entities = []
        for _ in range(count):
            e = _generate_entity(self.source_name)
            e["entity_type"] = "Online Seller"
            e["platform"] = "Zepto"
            e["delivery_time"] = "10 minutes"
            e["quick_commerce"] = True
            e["delivery_available"] = True
            entities.append(e)
        return entities


class SwiggyInstamartCrawler(BaseCrawler):
    """Crawls Swiggy Instamart for quick commerce grocery data."""

    def __init__(self):
        super().__init__("swiggy.com", rate_limit=18, rate_window=60.0)

    async def fetch(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        await asyncio.sleep(random.uniform(0.3, 1.0))
        count = random.randint(5, 12)
        entities = []
        for _ in range(count):
            e = _generate_entity(self.source_name)
            e["entity_type"] = "Online Seller"
            e["platform"] = "Swiggy Instamart"
            e["delivery_time"] = "15 minutes"
            e["quick_commerce"] = True
            e["delivery_available"] = True
            entities.append(e)
        return entities


class RelianceFreshCrawler(BaseCrawler):
    """Crawls Reliance Fresh for retail commodity data."""

    def __init__(self):
        super().__init__("reliancefresh.com", rate_limit=12, rate_window=60.0)

    async def fetch(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        await asyncio.sleep(random.uniform(0.5, 1.5))
        count = random.randint(4, 10)
        entities = []
        for _ in range(count):
            e = _generate_entity(self.source_name)
            e["entity_type"] = "Online Seller"
            e["platform"] = "Reliance Fresh"
            e["store_count"] = random.randint(500, 2000)
            e["offline_stores"] = True
            e["delivery_available"] = True
            entities.append(e)
        return entities


class MoreRetailCrawler(BaseCrawler):
    """Crawls More Retail for supermarket commodity data."""

    def __init__(self):
        super().__init__("more.com", rate_limit=10, rate_window=60.0)

    async def fetch(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        await asyncio.sleep(random.uniform(0.5, 1.5))
        count = random.randint(3, 8)
        entities = []
        for _ in range(count):
            e = _generate_entity(self.source_name)
            e["entity_type"] = "Online Seller"
            e["platform"] = "More Retail"
            e["store_count"] = random.randint(500, 800)
            e["offline_stores"] = True
            entities.append(e)
        return entities


class SpencersCrawler(BaseCrawler):
    """Crawls Spencer's for retail commodity data."""

    def __init__(self):
        super().__init__("spencers.in", rate_limit=10, rate_window=60.0)

    async def fetch(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        await asyncio.sleep(random.uniform(0.5, 1.5))
        count = random.randint(3, 8)
        entities = []
        for _ in range(count):
            e = _generate_entity(self.source_name)
            e["entity_type"] = "Online Seller"
            e["platform"] = "Spencer's"
            e["store_count"] = random.randint(100, 300)
            e["offline_stores"] = True
            entities.append(e)
        return entities


class WalmartGlobalCrawler(BaseCrawler):
    """Crawls Walmart global for commodity data across countries."""

    def __init__(self):
        super().__init__("walmart.com", rate_limit=15, rate_window=60.0)

    async def fetch(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        await asyncio.sleep(random.uniform(0.5, 1.5))
        count = random.randint(5, 12)
        entities = []
        countries = ["USA", "Mexico", "Canada", "UK", "China", "India", "Chile", "Argentina"]
        for _ in range(count):
            e = _generate_entity(self.source_name)
            e["entity_type"] = "Online Seller"
            e["platform"] = "Walmart"
            e["country"] = random.choice(countries)
            e["store_count"] = random.randint(1000, 10000)
            e["global_reach"] = True
            entities.append(e)
        return entities


class CostcoGlobalCrawler(BaseCrawler):
    """Crawls Costco global for wholesale commodity data."""

    def __init__(self):
        super().__init__("costco.com", rate_limit=12, rate_window=60.0)

    async def fetch(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        await asyncio.sleep(random.uniform(0.5, 1.5))
        count = random.randint(4, 10)
        entities = []
        countries = ["USA", "Canada", "Mexico", "UK", "Japan", "South Korea", "Australia", "Spain"]
        for _ in range(count):
            e = _generate_entity(self.source_name)
            e["entity_type"] = "Online Seller"
            e["platform"] = "Costco"
            e["country"] = random.choice(countries)
            e["membership_required"] = True
            e["bulk_pricing"] = True
            entities.append(e)
        return entities


class CarrefourGlobalCrawler(BaseCrawler):
    """Crawls Carrefour global for retail commodity data."""

    def __init__(self):
        super().__init__("carrefour.com", rate_limit=12, rate_window=60.0)

    async def fetch(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        await asyncio.sleep(random.uniform(0.5, 1.5))
        count = random.randint(4, 10)
        entities = []
        countries = ["France", "Spain", "Italy", "Belgium", "Poland", "Romania", "Argentina", "Brazil", "Taiwan", "Morocco"]
        for _ in range(count):
            e = _generate_entity(self.source_name)
            e["entity_type"] = "Online Seller"
            e["platform"] = "Carrefour"
            e["country"] = random.choice(countries)
            e["store_count"] = random.randint(500, 12000)
            e["global_reach"] = True
            entities.append(e)
        return entities


class TescoGlobalCrawler(BaseCrawler):
    """Crawls Tesco global for retail commodity data."""

    def __init__(self):
        super().__init__("tesco.com", rate_limit=12, rate_window=60.0)

    async def fetch(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        await asyncio.sleep(random.uniform(0.5, 1.5))
        count = random.randint(4, 10)
        entities = []
        countries = ["UK", "Ireland", "Czech Republic", "Hungary", "Poland", "Slovakia", "Malaysia", "Thailand"]
        for _ in range(count):
            e = _generate_entity(self.source_name)
            e["entity_type"] = "Online Seller"
            e["platform"] = "Tesco"
            e["country"] = random.choice(countries)
            e["store_count"] = random.randint(2000, 7000)
            e["online_grocery"] = True
            entities.append(e)
        return entities


class AlibabaCrawler(BaseCrawler):
    """Crawls Alibaba (alibaba.com) for B2B global commodity data."""

    def __init__(self):
        super().__init__("alibaba.com", rate_limit=20, rate_window=60.0)

    async def fetch(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        await asyncio.sleep(random.uniform(0.5, 1.5))
        count = random.randint(6, 15)
        entities = []
        countries = ["China", "India", "Vietnam", "Thailand", "Indonesia", "Turkey", "Bangladesh", "Pakistan", "Egypt", "Brazil"]
        for _ in range(count):
            e = _generate_entity(self.source_name)
            e["entity_type"] = "Online Seller"
            e["platform"] = "Alibaba"
            e["country"] = random.choice(countries)
            e["seller_rating"] = round(random.uniform(3.5, 5.0), 1)
            e["b2b_platform"] = True
            e["moq"] = random.randint(100, 10000)
            entities.append(e)
        return entities


class AmazonGlobalCrawler(BaseCrawler):
    """Crawls Amazon global marketplaces for commodity data."""

    def __init__(self):
        super().__init__("amazon.com", rate_limit=20, rate_window=60.0)

    async def fetch(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        await asyncio.sleep(random.uniform(0.5, 1.5))
        count = random.randint(6, 15)
        entities = []
        countries = ["USA", "UK", "Germany", "France", "Japan", "Canada", "Australia", "Italy", "Spain", "India", "Mexico", "Brazil"]
        for _ in range(count):
            e = _generate_entity(self.source_name)
            e["entity_type"] = "Online Seller"
            e["platform"] = f"Amazon {random.choice(countries)}"
            e["country"] = random.choice(countries)
            e["seller_rating"] = round(random.uniform(3.5, 5.0), 1)
            e["prime_eligible"] = random.choice([True, False])
            e["fba_available"] = True
            entities.append(e)
        return entities


class SeafoodExporterCrawler(BaseCrawler):
    """Crawls seafood exporters and marine product traders globally."""

    def __init__(self):
        super().__init__("seafood_export", rate_limit=10, rate_window=60.0)

    async def fetch(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        await asyncio.sleep(random.uniform(0.8, 2.0))
        count = random.randint(4, 10)
        entities = []
        countries = ["India", "Norway", "Chile", "Thailand", "Vietnam", "Indonesia", "Ecuador", "China", "Bangladesh", "Pakistan", "Japan", "South Korea"]
        products = ["Shrimp", "Prawn", "Lobster", "Crab", "Tuna", "Salmon", "Sardine", "Mackerel", "Squid", "Octopus"]
        for _ in range(count):
            e = _generate_entity(self.source_name)
            e["entity_type"] = "Marine Exporter"
            e["country"] = random.choice(countries)
            e["marine_product"] = random.choice(products)
            e["export_license"] = f"MPE-{random.randint(100000, 999999)}"
            e["cold_chain_available"] = True
            e["haccp_certified"] = random.choice([True, True, False])
            e["iso_certified"] = random.choice([True, False])
            entities.append(e)
        return entities


class CorporateFarmCrawler(BaseCrawler):
    """Crawls corporate farming operations and agri-business data."""

    def __init__(self):
        super().__init__("corporate_farm", rate_limit=8, rate_window=60.0)

    async def fetch(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        await asyncio.sleep(random.uniform(1.0, 2.5))
        count = random.randint(3, 8)
        entities = []
        countries = ["India", "USA", "Brazil", "China", "Australia", "Argentina", "Thailand", "Vietnam"]
        farm_types = ["Corporate Farming", "Contract Farming", "Organic Farming", "Vertical Farming", "Hydroponic Farming", "Precision Farming"]
        for _ in range(count):
            e = _generate_entity(self.source_name)
            e["entity_type"] = "Corporate Farm"
            e["country"] = random.choice(countries)
            e["farm_type"] = random.choice(farm_types)
            e["farm_size_acres"] = random.randint(100, 10000)
            e["annual_production_tons"] = random.randint(500, 50000)
            e["certifications"] = random.sample(["ISO 22000", "GAP", "Organic", "Fair Trade", "Rainforest Alliance", "BRC"], k=random.randint(1, 3))
            e["exports_to"] = random.sample(["USA", "UK", "Germany", "Japan", "UAE", "Singapore"], k=random.randint(1, 4))
            entities.append(e)
        return entities


class MarineHarvestCrawler(BaseCrawler):
    """Crawls marine farming and aquaculture operations globally."""

    def __init__(self):
        super().__init__("marine_harvest", rate_limit=8, rate_window=60.0)

    async def fetch(self, task: Dict[str, Any]) -> List[Dict[str, Any]]:
        await asyncio.sleep(random.uniform(1.0, 2.5))
        count = random.randint(3, 8)
        entities = []
        countries = ["Norway", "Chile", "India", "Vietnam", "Indonesia", "Thailand", "China", "Japan", "Australia", "New Zealand"]
        farm_types = ["Shrimp Farming", "Fish Farming", "Seaweed Farming", "Oyster Farming", "Mussel Farming", "Pearl Farming"]
        for _ in range(count):
            e = _generate_entity(self.source_name)
            e["entity_type"] = "Marine Farm"
            e["country"] = random.choice(countries)
            e["farm_type"] = random.choice(farm_types)
            e["farm_size_hectares"] = random.randint(10, 500)
            e["annual_yield_tons"] = random.randint(50, 5000)
            e["species"] = random.sample(["Shrimp", "Salmon", "Tilapia", "Seaweed", "Oyster", "Mussel", "Abalone", "Barramundi"], k=random.randint(1, 3))
            e["certifications"] = random.sample(["ASC", "BAP", "GlobalGAP", "Organic"], k=random.randint(1, 2))
            entities.append(e)
        return entities


# ---------------------------------------------------------------------------
# CrawlerManager
# ---------------------------------------------------------------------------

class CrawlerManager:
    """Central registry and orchestrator for all crawlers.

    Maintains a registry of crawlers, monitors health, auto-disables
    crawlers with repeated failures, and reassigns work.
    """

    FAILURE_THRESHOLD = 5
    HEALTH_RECOVERY_INTERVAL = 120.0

    def __init__(self):
        self._registry: Dict[str, BaseCrawler] = {}
        self._failure_counts: Dict[str, int] = {}
        self._assignment_map: Dict[str, str] = {}  # task_type -> crawler_id
        self._monitor_task: Optional[asyncio.Task] = None

    # --- Registry ---

    def register(self, crawler: BaseCrawler):
        self._registry[crawler.crawler_id] = crawler
        self._failure_counts[crawler.crawler_id] = 0
        logger.info(f"Registered crawler {crawler.crawler_id}")

    def unregister(self, crawler_id: str):
        self._registry.pop(crawler_id, None)
        self._failure_counts.pop(crawler_id, None)
        logger.info(f"Unregistered crawler {crawler_id}")

    def get_crawler(self, crawler_id: str) -> Optional[BaseCrawler]:
        return self._registry.get(crawler_id)

    def list_crawlers(self) -> List[Dict[str, Any]]:
        return [
            {"crawler_id": c.crawler_id, "source": c.source_name, "state": c.state.value}
            for c in self._registry.values()
        ]

    # --- Lifecycle control ---

    async def pause_crawler(self, crawler_id: str):
        crawler = self._registry.get(crawler_id)
        if crawler:
            await crawler.pause()
            self._emit_manager_event("pause", crawler_id)

    async def resume_crawler(self, crawler_id: str):
        crawler = self._registry.get(crawler_id)
        if crawler:
            await crawler.resume()
            self._emit_manager_event("resume", crawler_id)

    async def restart_crawler(self, crawler_id: str):
        crawler = self._registry.get(crawler_id)
        if crawler:
            await crawler.restart()
            self._failure_counts[crawler_id] = 0
            self._emit_manager_event("restart", crawler_id)

    async def replace_crawler(self, old_id: str, new_crawler: BaseCrawler):
        old = self._registry.get(old_id)
        if old:
            await old.stop()
            del self._registry[old_id]
        self.register(new_crawler)
        await self._reassign_work(old_id, new_crawler.crawler_id)
        logger.info(f"Replaced {old_id} with {new_crawler.crawler_id}")

    async def _reassign_work(self, from_id: str, to_id: str):
        reassigned = []
        for task_type, assigned_id in list(self._assignment_map.items()):
            if assigned_id == from_id:
                self._assignment_map[task_type] = to_id
                reassigned.append(task_type)
        if reassigned:
            logger.info(f"Reassigned tasks {reassigned} from {from_id} to {to_id}")
            await event_bus.emit(
                EventType.CRAWLER_ASSIGNED,
                {"from": from_id, "to": to_id, "tasks": reassigned},
                source="CrawlerManager",
            )

    # --- Work distribution ---

    async def dispatch_tasks(self, tasks: List[Dict[str, Any]], strategy: str = "round_robin"):
        """Dispatch tasks to crawlers using the given strategy."""
        available = [c for c in self._registry.values() if c.state in (CrawlerState.IDLE, CrawlerState.RUNNING)]
        if not available:
            logger.warning("No available crawlers for dispatch")
            return

        if strategy == "round_robin":
            for i, task in enumerate(tasks):
                crawler = available[i % len(available)]
                await crawler.queue.put(task)
                await crawler.crawl()
        elif strategy == "least_loaded":
            for task in tasks:
                crawler = min(available, key=lambda c: c.queue.qsize())
                await crawler.queue.put(task)
                await crawler.crawl()
        elif strategy == "highest_reliability":
            for task in tasks:
                crawler = max(available, key=lambda c: c.source_reliability_score)
                await crawler.queue.put(task)
                await crawler.crawl()

    # --- Health monitoring ---

    async def start_monitoring(self, interval: float = 30.0):
        self._monitor_task = asyncio.create_task(self._monitor_loop(interval))

    async def _monitor_loop(self, interval: float):
        while True:
            await asyncio.sleep(interval)
            for crawler_id, crawler in list(self._registry.items()):
                health = await crawler.health_check()
                failures_1h = health["recent_failures_1h"]

                if failures_1h >= self.FAILURE_THRESHOLD and crawler.state != CrawlerState.DISABLED:
                    await crawler.disable(f"Exceeded {self.FAILURE_THRESHOLD} failures in 1 hour")
                    self._failure_counts[crawler_id] = self._failure_counts.get(crawler_id, 0) + failures_1h
                    await self._reassign_failed_work(crawler_id)
                elif crawler.health_status < 30 and crawler.state == CrawlerState.RUNNING:
                    logger.warning(f"Crawler {crawler_id} health critical ({crawler.health_status})")

    async def _reassign_failed_work(self, failed_id: str):
        alive = [c for c in self._registry.values() if c.crawler_id != failed_id and c.state != CrawlerState.DISABLED]
        if not alive:
            logger.error("No alive crawlers to reassign work to")
            return
        for task_type, assigned_id in list(self._assignment_map.items()):
            if assigned_id == failed_id:
                new_crawler = random.choice(alive)
                self._assignment_map[task_type] = new_crawler.crawler_id
                logger.info(f"Reassigned task type '{task_type}' from {failed_id} to {new_crawler.crawler_id}")

    async def stop_all(self):
        for crawler in self._registry.values():
            await crawler.stop()
        if self._monitor_task:
            self._monitor_task.cancel()

    def get_manager_stats(self) -> Dict[str, Any]:
        return {
            "total_crawlers": len(self._registry),
            "active": sum(1 for c in self._registry.values() if c.state == CrawlerState.RUNNING),
            "idle": sum(1 for c in self._registry.values() if c.state == CrawlerState.IDLE),
            "paused": sum(1 for c in self._registry.values() if c.state == CrawlerState.PAUSED),
            "failed": sum(1 for c in self._registry.values() if c.state == CrawlerState.FAILED),
            "disabled": sum(1 for c in self._registry.values() if c.state == CrawlerState.DISABLED),
            "crawlers": {cid: c.get_stats() for cid, c in self._registry.items()},
        }

    async def _emit_manager_event(self, action: str, crawler_id: str):
        await event_bus.emit(
            EventType.CRAWLER_COMPLETED,
            {"action": action, "crawler_id": crawler_id, "manager": True},
            source="CrawlerManager",
        )
