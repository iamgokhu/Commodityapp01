"""Monitoring and self-healing system for the Commodity OS."""
import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from commodity_os.core.events import Event, EventType, event_bus

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
CHECKPOINT_FILE = DATA_DIR / "checkpoint.json"


class AlertLevel(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class ComponentType(str, Enum):
    CRAWLER = "crawler"
    AGENT = "agent"
    DATA_QUALITY = "data_quality"
    API = "api"
    SYSTEM = "system"
    STORAGE = "storage"
    PERFORMANCE = "performance"
    DASHBOARD = "dashboard"


@dataclass
class ComponentHealth:
    component_type: ComponentType
    name: str
    health_score: float = 100.0
    alert_level: AlertLevel = AlertLevel.INFO
    last_check: float = 0.0
    metrics: Dict[str, Any] = field(default_factory=dict)
    error_message: str = ""
    is_healthy: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "component_type": self.component_type.value,
            "name": self.name,
            "health_score": round(self.health_score, 2),
            "alert_level": self.alert_level.value,
            "last_check": self.last_check,
            "metrics": self.metrics,
            "error_message": self.error_message,
            "is_healthy": self.is_healthy,
        }


@dataclass
class HealthReport:
    overall_score: float = 100.0
    components: Dict[str, ComponentHealth] = field(default_factory=dict)
    timestamp: float = 0.0
    recent_events: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "overall_score": round(self.overall_score, 2),
            "timestamp": self.timestamp,
            "components": {k: v.to_dict() for k, v in self.components.items()},
            "recent_events": self.recent_events,
        }


@dataclass
class MonitorEvent:
    timestamp: float
    level: AlertLevel
    component: str
    message: str
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "level": self.level.value,
            "component": self.component,
            "message": self.message,
            "details": self.details,
        }


class MonitoringSystem:
    def __init__(self):
        self._components: Dict[str, ComponentHealth] = {}
        self._event_log: List[MonitorEvent] = []
        self._max_log_size = 500
        self._checkpoint: Dict[str, Any] = {}
        self._performance_history: List[Dict[str, Any]] = []
        self._max_history = 1000
        self._running = False
        self._monitor_interval = 30
        self._crawlers: Dict[str, Dict[str, Any]] = {}
        self._agents: Dict[str, Dict[str, Any]] = {}
        self._api_endpoints: Dict[str, Dict[str, Any]] = {}
        self._pipeline_stages: List[str] = []
        self._current_stage_index = 0

        DATA_DIR.mkdir(parents=True, exist_ok=True)

    async def initialize(self):
        logger.info("MonitoringSystem initializing...")
        self._running = True

        event_bus.subscribe(EventType.CRAWLER_COMPLETED, self._on_crawler_completed)
        event_bus.subscribe(EventType.CRAWLER_FAILED, self._on_crawler_failed)
        event_bus.subscribe(EventType.AGENT_COMPLETED, self._on_agent_completed)
        event_bus.subscribe(EventType.AGENT_FAILED, self._on_agent_failed)
        event_bus.subscribe(EventType.DATA_VALIDATED, self._on_data_validated)
        event_bus.subscribe(EventType.API_HEALTH_CHECK, self._on_api_health_check)
        event_bus.subscribe(EventType.RESOURCE_CHECK, self._on_resource_check)
        event_bus.subscribe(EventType.DASHBOARD_PUBLISHED, self._on_dashboard_published)
        event_bus.subscribe(EventType.SESSION_DISCONNECTED, self._on_session_disconnect)
        event_bus.subscribe(EventType.RECOVERY_COMPLETED, self._on_recovery_completed)

        self._load_checkpoint()
        self._init_default_components()

        await event_bus.emit(
            EventType.MONITORING_UPDATE,
            {"status": "initialized", "components": len(self._components)},
            source="monitoring_system",
        )
        logger.info(
            f"MonitoringSystem ready. {len(self._components)} components tracked."
        )

    def _init_default_components(self):
        for i in range(1, 33):
            self._components[f"crawler_{i}"] = ComponentHealth(
                component_type=ComponentType.CRAWLER,
                name=f"crawler_{i}",
            )
        for i in range(1, 33):
            self._components[f"agent_{i}"] = ComponentHealth(
                component_type=ComponentType.AGENT,
                name=f"agent_{i}",
            )
        for ep in ["api_market_data", "api_prices", "api_trade", "api_news"]:
            self._components[ep] = ComponentHealth(
                component_type=ComponentType.API, name=ep
            )
        for comp in ["data_quality", "storage", "performance", "dashboard"]:
            self._components[comp] = ComponentHealth(
                component_type=ComponentType(comp), name=comp
            )

    async def check_all_health(self) -> HealthReport:
        report = HealthReport(timestamp=time.time())

        for name, comp in self._components.items():
            await self._check_component_health(comp)
            report.components[name] = comp

        scores = [c.health_score for c in report.components.values()]
        report.overall_score = sum(scores) / len(scores) if scores else 0.0

        report.recent_events = [
            e.to_dict() for e in self._event_log[-20:]
        ]

        await event_bus.emit(
            EventType.MONITORING_UPDATE,
            {
                "overall_score": report.overall_score,
                "component_count": len(report.components),
                "timestamp": report.timestamp,
            },
            source="monitoring_system",
        )

        return report

    async def _check_component_health(self, comp: ComponentHealth):
        comp.last_check = time.time()

        if comp.component_type == ComponentType.CRAWLER:
            await self._check_crawler_health(comp)
        elif comp.component_type == ComponentType.AGENT:
            await self._check_agent_health(comp)
        elif comp.component_type == ComponentType.DATA_QUALITY:
            await self._check_data_quality(comp)
        elif comp.component_type == ComponentType.API:
            await self._check_api_health(comp)
        elif comp.component_type == ComponentType.SYSTEM:
            await self._check_system_health(comp)
        elif comp.component_type == ComponentType.STORAGE:
            await self._check_storage_health(comp)
        elif comp.component_type == ComponentType.PERFORMANCE:
            await self._check_performance_health(comp)
        elif comp.component_type == ComponentType.DASHBOARD:
            await self._check_dashboard_health(comp)

        comp.is_healthy = comp.health_score >= 60
        if comp.health_score >= 80:
            comp.alert_level = AlertLevel.INFO
        elif comp.health_score >= 50:
            comp.alert_level = AlertLevel.WARNING
        else:
            comp.alert_level = AlertLevel.CRITICAL

    async def _check_crawler_health(self, comp: ComponentHealth):
        stats = self._crawlers.get(comp.name, {})
        total = stats.get("total", 0)
        success = stats.get("success", 0)
        errors = stats.get("errors", 0)
        avg_response = stats.get("avg_response_time", 0)

        success_rate = (success / total * 100) if total > 0 else 100.0
        error_rate = (errors / total * 100) if total > 0 else 0.0

        score = 100.0
        if success_rate < 50:
            score -= 50
        elif success_rate < 80:
            score -= 20
        if error_rate > 20:
            score -= 30
        elif error_rate > 5:
            score -= 10
        if avg_response > 5000:
            score -= 20
        elif avg_response > 2000:
            score -= 10

        comp.health_score = max(0.0, score)
        comp.metrics = {
            "total_requests": total,
            "success_rate": round(success_rate, 2),
            "error_rate": round(error_rate, 2),
            "avg_response_time_ms": round(avg_response, 2),
        }

        if comp.health_score < 50 and total > 0:
            comp.error_message = f"Low success rate: {success_rate:.1f}%"
            await self._attempt_crawler_restart(comp.name)

    async def _check_agent_health(self, comp: ComponentHealth):
        stats = self._agents.get(comp.name, {})
        completed = stats.get("completed", 0)
        failed = stats.get("failed", 0)
        total = completed + failed

        completion_rate = (completed / total * 100) if total > 0 else 100.0
        error_rate = (failed / total * 100) if total > 0 else 0.0
        avg_duration = stats.get("avg_duration", 0)

        score = 100.0
        if completion_rate < 50:
            score -= 50
        elif completion_rate < 80:
            score -= 20
        if error_rate > 20:
            score -= 30
        elif error_rate > 5:
            score -= 10
        if avg_duration > 300:
            score -= 15

        comp.health_score = max(0.0, score)
        comp.metrics = {
            "tasks_completed": completed,
            "tasks_failed": failed,
            "completion_rate": round(completion_rate, 2),
            "error_rate": round(error_rate, 2),
            "avg_duration_s": round(avg_duration, 2),
        }

        if comp.health_score < 50 and total > 0:
            comp.error_message = f"High failure rate: {error_rate:.1f}%"
            await self._attempt_agent_restart(comp.name)

    async def _check_data_quality(self, comp: ComponentHealth):
        completeness = comp.metrics.get("completeness_score", 100.0)
        accuracy = comp.metrics.get("accuracy_score", 100.0)
        freshness = comp.metrics.get("freshness_score", 100.0)

        comp.health_score = (completeness * 0.4 + accuracy * 0.4 + freshness * 0.2)
        comp.metrics["completeness_score"] = round(completeness, 2)
        comp.metrics["accuracy_score"] = round(accuracy, 2)
        comp.metrics["freshness_score"] = round(freshness, 2)

        if comp.health_score < 60:
            comp.error_message = (
                f"Quality below threshold: {comp.health_score:.1f}%"
            )

    async def _check_api_health(self, comp: ComponentHealth):
        stats = self._api_endpoints.get(comp.name, {})
        is_up = stats.get("is_up", True)
        latency = stats.get("latency_ms", 0)
        rate_limited = stats.get("rate_limited", False)
        last_200 = stats.get("last_200_time", time.time())

        score = 100.0
        if not is_up:
            score -= 60
        if latency > 3000:
            score -= 20
        elif latency > 1000:
            score -= 10
        if rate_limited:
            score -= 25
        staleness = time.time() - last_200
        if staleness > 3600:
            score -= 30
        elif staleness > 600:
            score -= 10

        comp.health_score = max(0.0, score)
        comp.metrics = {
            "is_up": is_up,
            "latency_ms": round(latency, 2),
            "rate_limited": rate_limited,
            "staleness_s": round(staleness, 2),
        }

    async def _check_system_health(self, comp: ComponentHealth):
        import psutil

        cpu = psutil.cpu_percent(interval=0.1)
        vm = psutil.virtual_memory()

        score = 100.0
        if cpu > 90:
            score -= 30
        elif cpu > 70:
            score -= 15
        if vm.percent > 90:
            score -= 30
        elif vm.percent > 75:
            score -= 10

        uptime = time.time() - self._checkpoint.get("start_time", time.time())

        comp.health_score = max(0.0, score)
        comp.metrics = {
            "cpu_percent": round(cpu, 2),
            "ram_percent": round(vm.percent, 2),
            "ram_used_gb": round(vm.used / (1024**3), 2),
            "uptime_s": round(uptime, 2),
        }

    async def _check_storage_health(self, comp: ComponentHealth):
        du = None
        try:
            import psutil
            try:
                du = psutil.disk_usage("C:\\")
            except Exception:
                try:
                    du = psutil.disk_usage("/")
                except Exception:
                    du = None
        except ImportError:
            pass

        score = 100.0
        disk_percent = 0.0
        if du:
            disk_percent = du.percent
            if disk_percent > 95:
                score -= 50
            elif disk_percent > 85:
                score -= 20

        db_size = 0
        data_dir = BASE_DIR / "data"
        if data_dir.exists():
            db_size = sum(f.stat().st_size for f in data_dir.rglob("*") if f.is_file())

        comp.health_score = max(0.0, score)
        comp.metrics = {
            "disk_percent": round(disk_percent, 2),
            "disk_total_gb": round(du.total / (1024**3), 2) if du else 0,
            "disk_used_gb": round(du.used / (1024**3), 2) if du else 0,
            "data_dir_size_mb": round(db_size / (1024**2), 2),
        }

    async def _check_performance_health(self, comp: ComponentHealth):
        recent = self._performance_history[-10:] if self._performance_history else []
        if not recent:
            comp.health_score = 100.0
            comp.metrics = {"throughput_rps": 0, "avg_latency_ms": 0}
            return

        avg_throughput = sum(p.get("throughput", 0) for p in recent) / len(recent)
        avg_latency = sum(p.get("latency_ms", 0) for p in recent) / len(recent)

        score = 100.0
        if avg_latency > 5000:
            score -= 30
        elif avg_latency > 2000:
            score -= 15
        if avg_throughput < 1:
            score -= 20

        comp.health_score = max(0.0, score)
        comp.metrics = {
            "throughput_rps": round(avg_throughput, 2),
            "avg_latency_ms": round(avg_latency, 2),
            "samples": len(recent),
        }

    async def _check_dashboard_health(self, comp: ComponentHealth):
        last_update = comp.metrics.get("last_update_time", 0)
        staleness = time.time() - last_update if last_update else float("inf")

        score = 100.0
        if staleness > 3600:
            score -= 40
        elif staleness > 600:
            score -= 20
        elif staleness > 120:
            score -= 10

        comp.health_score = max(0.0, score)
        comp.metrics["staleness_s"] = round(
            staleness if staleness != float("inf") else -1, 2
        )

    async def heal_component(self, component_name: str) -> bool:
        comp = self._components.get(component_name)
        if not comp:
            logger.warning(f"Unknown component: {component_name}")
            return False

        logger.info(f"Healing component: {component_name}")
        healed = False

        if comp.component_type == ComponentType.CRAWLER:
            healed = await self._attempt_crawler_restart(component_name)
        elif comp.component_type == ComponentType.AGENT:
            healed = await self._attempt_agent_restart(component_name)
        elif comp.component_type == ComponentType.DATA_QUALITY:
            healed = await self._repair_data_quality(comp)
        elif comp.component_type == ComponentType.API:
            healed = await self._handle_api_failure(comp)
        elif comp.component_type == ComponentType.STORAGE:
            healed = await self._handle_storage_issue(comp)
        elif comp.component_type == ComponentType.DASHBOARD:
            healed = await self._refresh_dashboard(comp)

        if healed:
            self._log_event(
                AlertLevel.INFO,
                component_name,
                f"Component healed successfully",
            )
            await event_bus.emit(
                EventType.RECOVERY_COMPLETED,
                {"component": component_name, "healed": True},
                source="monitoring_system",
            )
        else:
            self._log_event(
                AlertLevel.CRITICAL,
                component_name,
                f"Failed to heal component",
            )

        return healed

    async def _attempt_crawler_restart(self, crawler_name: str) -> bool:
        logger.info(f"Attempting restart for crawler: {crawler_name}")
        stats = self._crawlers.get(crawler_name, {})
        stats["restart_count"] = stats.get("restart_count", 0) + 1
        stats["last_restart"] = time.time()
        stats["total"] = 0
        stats["success"] = 0
        stats["errors"] = 0
        self._crawlers[crawler_name] = stats

        comp = self._components.get(crawler_name)
        if comp:
            comp.health_score = 80.0
            comp.error_message = ""

        await event_bus.emit(
            EventType.CRAWLER_COMPLETED,
            {"crawler": crawler_name, "action": "restarted"},
            source="monitoring_system",
        )
        return True

    async def _attempt_agent_restart(self, agent_name: str) -> bool:
        logger.info(f"Attempting restart for agent: {agent_name}")
        stats = self._agents.get(agent_name, {})
        stats["restart_count"] = stats.get("restart_count", 0) + 1
        stats["last_restart"] = time.time()
        stats["completed"] = 0
        stats["failed"] = 0
        self._agents[agent_name] = stats

        comp = self._components.get(agent_name)
        if comp:
            comp.health_score = 80.0
            comp.error_message = ""

        await event_bus.emit(
            EventType.AGENT_COMPLETED,
            {"agent": agent_name, "action": "restarted"},
            source="monitoring_system",
        )
        return True

    async def _repair_data_quality(self, comp: ComponentHealth) -> bool:
        logger.info("Running data quality repair...")
        data_dir = BASE_DIR / "data"
        repaired = 0

        if not data_dir.exists():
            return True

        for json_file in data_dir.rglob("*.json"):
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    json.load(f)
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.warning(f"Corrupted JSON: {json_file} - {e}")
                if await self._repair_json_file(json_file):
                    repaired += 1
            except Exception as e:
                logger.warning(f"Unreadable file: {json_file} - {e}")

        comp.metrics["repaired_files"] = repaired
        if repaired > 0:
            comp.health_score = min(100.0, comp.health_score + repaired * 5)
        return True

    async def _repair_json_file(self, file_path: Path) -> bool:
        try:
            raw = file_path.read_text(encoding="utf-8", errors="ignore")
            raw = raw.strip()
            if not raw:
                file_path.write_text("{}", encoding="utf-8")
                return True

            try:
                json.loads(raw)
                return True
            except json.JSONDecodeError:
                pass

            for end_char in ["}", "]"]:
                for cut in range(len(raw) - 1, max(0, len(raw) - 500), -1):
                    candidate = raw[:cut] + end_char
                    try:
                        data = json.loads(candidate)
                        file_path.write_text(
                            json.dumps(data, indent=2, ensure_ascii=False),
                            encoding="utf-8",
                        )
                        logger.info(f"Repaired JSON: {file_path}")
                        return True
                    except json.JSONDecodeError:
                        continue

            backup = file_path.with_suffix(".json.bak")
            file_path.rename(backup)
            file_path.write_text("{}", encoding="utf-8")
            logger.info(f"Backed up and reset: {file_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to repair {file_path}: {e}")
            return False

    async def _handle_api_failure(self, comp: ComponentHealth) -> bool:
        stats = self._api_endpoints.get(comp.name, {})
        backoff = stats.get("backoff_s", 1)
        stats["backoff_s"] = min(backoff * 2, 300)
        stats["is_up"] = False
        stats["last_failure"] = time.time()
        self._api_endpoints[comp.name] = stats

        logger.info(
            f"API {comp.name}: backing off for {stats['backoff_s']}s"
        )
        return True

    async def _handle_storage_issue(self, comp: ComponentHealth) -> bool:
        logger.info("Running storage cleanup...")
        cleaned = 0
        logs_dir = BASE_DIR / "logs"
        if logs_dir.exists():
            for old_log in sorted(logs_dir.glob("*.log"), key=lambda f: f.stat().st_mtime)[:-5]:
                try:
                    old_log.unlink()
                    cleaned += 1
                except Exception:
                    pass

        temp_dir = Path(os.environ.get("TEMP", "/tmp")) / "commodity_os"
        if temp_dir.exists():
            for tmp_file in temp_dir.glob("*"):
                try:
                    tmp_file.unlink()
                    cleaned += 1
                except Exception:
                    pass

        comp.metrics["cleaned_files"] = cleaned
        return True

    async def _refresh_dashboard(self, comp: ComponentHealth) -> bool:
        comp.metrics["last_update_time"] = time.time()
        comp.health_score = 100.0
        return True

    async def recover_partial_download(self, file_path: str, expected_size: int) -> bool:
        path = Path(file_path)
        if not path.exists():
            return False

        actual_size = path.stat().st_size
        if actual_size >= expected_size:
            return True

        logger.warning(
            f"Partial download: {file_path} ({actual_size}/{expected_size} bytes)"
        )
        incomplete = path.with_suffix(path.suffix + ".incomplete")
        path.rename(incomplete)

        self._log_event(
            AlertLevel.WARNING,
            "download",
            f"Partial download detected: {file_path}",
            {"expected": expected_size, "actual": actual_size},
        )
        return False

    async def handle_rate_limit(self, endpoint: str, retry_after: int = 60):
        stats = self._api_endpoints.get(endpoint, {})
        stats["rate_limited"] = True
        stats["retry_after"] = retry_after
        stats["rate_limit_time"] = time.time()
        self._api_endpoints[endpoint] = stats

        self._log_event(
            AlertLevel.WARNING,
            endpoint,
            f"Rate limited. Retrying after {retry_after}s",
        )

        await asyncio.sleep(retry_after)

        stats["rate_limited"] = False
        self._api_endpoints[endpoint] = stats

    async def handle_network_failure(self, operation, max_retries: int = 5):
        delay = 1.0
        for attempt in range(max_retries):
            try:
                result = await operation()
                return result
            except Exception as e:
                if attempt == max_retries - 1:
                    self._log_event(
                        AlertLevel.CRITICAL,
                        "network",
                        f"Network failure after {max_retries} retries: {e}",
                    )
                    raise
                logger.warning(
                    f"Network failure (attempt {attempt + 1}): {e}. "
                    f"Retrying in {delay:.1f}s..."
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)

    async def handle_session_disconnect(self, session_id: str):
        self._log_event(
            AlertLevel.WARNING,
            "session",
            f"Session disconnected: {session_id}",
        )
        await event_bus.emit(
            EventType.SESSION_DISCONNECTED,
            {"session_id": session_id},
            source="monitoring_system",
        )

    def save_checkpoint(self, state: Dict[str, Any]):
        self._checkpoint.update(state)
        self._checkpoint["saved_at"] = time.time()
        self._checkpoint["pipeline_stage_index"] = self._current_stage_index

        try:
            CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
                json.dump(self._checkpoint, f, indent=2, ensure_ascii=False)
            logger.debug(f"Checkpoint saved: {CHECKPOINT_FILE}")
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")

    def load_checkpoint(self) -> Dict[str, Any]:
        return self._load_checkpoint()

    def _load_checkpoint(self) -> Dict[str, Any]:
        if CHECKPOINT_FILE.exists():
            try:
                with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                    self._checkpoint = json.load(f)
                self._current_stage_index = self._checkpoint.get(
                    "pipeline_stage_index", 0
                )
                logger.info(
                    f"Checkpoint loaded. Last stage index: {self._current_stage_index}"
                )
                return self._checkpoint
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Failed to load checkpoint: {e}")

        self._checkpoint = {
            "start_time": time.time(),
            "pipeline_stage_index": 0,
            "cycle_count": 0,
            "last_successful_stage": "",
        }
        return self._checkpoint

    def get_last_successful_stage(self) -> str:
        return self._checkpoint.get("last_successful_stage", "")

    def mark_stage_complete(self, stage_name: str):
        self._checkpoint["last_successful_stage"] = stage_name
        if stage_name in self._pipeline_stages:
            idx = self._pipeline_stages.index(stage_name)
            self._current_stage_index = idx + 1
        self.save_checkpoint(self._checkpoint)

    def get_resume_stage(self) -> Optional[str]:
        if self._current_stage_index < len(self._pipeline_stages):
            return self._pipeline_stages[self._current_stage_index]
        return None

    def get_health_dashboard(self) -> Dict[str, Any]:
        components_summary = {}
        for name, comp in self._components.items():
            components_summary[name] = {
                "score": comp.health_score,
                "alert": comp.alert_level.value,
                "healthy": comp.is_healthy,
            }

        alert_counts = {level.value: 0 for level in AlertLevel}
        for comp in self._components.values():
            alert_counts[comp.alert_level.value] += 1

        overall = (
            sum(c.health_score for c in self._components.values())
            / len(self._components)
            if self._components
            else 0.0
        )

        return {
            "overall_health_score": round(overall, 2),
            "component_count": len(self._components),
            "healthy_components": sum(
                1 for c in self._components.values() if c.is_healthy
            ),
            "alert_counts": alert_counts,
            "components": components_summary,
            "recent_events": [e.to_dict() for e in self._event_log[-50:]],
            "checkpoint": {
                "last_saved": self._checkpoint.get("saved_at", 0),
                "resume_stage": self.get_resume_stage(),
                "last_successful": self.get_last_successful_stage(),
            },
            "performance_history_size": len(self._performance_history),
            "timestamp": time.time(),
        }

    def _log_event(
        self,
        level: AlertLevel,
        component: str,
        message: str,
        details: Optional[Dict[str, Any]] = None,
    ):
        event = MonitorEvent(
            timestamp=time.time(),
            level=level,
            component=component,
            message=message,
            details=details or {},
        )
        self._event_log.append(event)
        if len(self._event_log) > self._max_log_size:
            self._event_log = self._event_log[-self._max_log_size:]

        if level == AlertLevel.CRITICAL:
            logger.critical(f"[{component}] {message}")
        elif level == AlertLevel.WARNING:
            logger.warning(f"[{component}] {message}")
        else:
            logger.info(f"[{component}] {message}")

    async def _on_crawler_completed(self, event: Event):
        crawler = event.payload.get("crawler", event.source)
        stats = self._crawlers.setdefault(
            crawler, {"total": 0, "success": 0, "errors": 0, "avg_response_time": 0}
        )
        stats["total"] += 1
        stats["success"] += 1
        rt = event.payload.get("response_time_ms", 0)
        if rt > 0:
            n = stats["success"]
            stats["avg_response_time"] = (
                stats["avg_response_time"] * (n - 1) + rt
            ) / n

    async def _on_crawler_failed(self, event: Event):
        crawler = event.payload.get("crawler", event.source)
        stats = self._crawlers.setdefault(
            crawler, {"total": 0, "success": 0, "errors": 0, "avg_response_time": 0}
        )
        stats["total"] += 1
        stats["errors"] += 1

    async def _on_agent_completed(self, event: Event):
        agent = event.payload.get("agent", event.source)
        stats = self._agents.setdefault(
            agent, {"completed": 0, "failed": 0, "avg_duration": 0}
        )
        stats["completed"] += 1
        dur = event.payload.get("duration_s", 0)
        if dur > 0:
            n = stats["completed"]
            stats["avg_duration"] = (
                stats["avg_duration"] * (n - 1) + dur
            ) / n

    async def _on_agent_failed(self, event: Event):
        agent = event.payload.get("agent", event.source)
        stats = self._agents.setdefault(
            agent, {"completed": 0, "failed": 0, "avg_duration": 0}
        )
        stats["failed"] += 1

    async def _on_data_validated(self, event: Event):
        comp = self._components.get("data_quality")
        if comp:
            completeness = event.payload.get("completeness", 100)
            accuracy = event.payload.get("accuracy", 100)
            comp.metrics["completeness_score"] = completeness
            comp.metrics["accuracy_score"] = accuracy

    async def _on_api_health_check(self, event: Event):
        endpoint = event.payload.get("endpoint", event.source)
        stats = self._api_endpoints.setdefault(
            endpoint, {"is_up": True, "latency_ms": 0, "rate_limited": False, "last_200_time": time.time()}
        )
        stats["is_up"] = event.payload.get("is_up", True)
        stats["latency_ms"] = event.payload.get("latency_ms", 0)
        if stats["is_up"]:
            stats["last_200_time"] = time.time()

    async def _on_resource_check(self, event: Event):
        snapshot = event.payload.get("snapshot", {})
        self._performance_history.append(
            {
                "timestamp": time.time(),
                "throughput": event.payload.get("throughput", 0),
                "latency_ms": snapshot.get("cpu_percent", 0),
            }
        )
        if len(self._performance_history) > self._max_history:
            self._performance_history = self._performance_history[
                -self._max_history:
            ]

    async def _on_dashboard_published(self, event: Event):
        comp = self._components.get("dashboard")
        if comp:
            comp.metrics["last_update_time"] = time.time()

    async def _on_session_disconnect(self, event: Event):
        self._log_event(
            AlertLevel.WARNING,
            "session",
            f"Session disconnect detected: {event.payload}",
        )

    async def _on_recovery_completed(self, event: Event):
        component = event.payload.get("component", "unknown")
        self._log_event(
            AlertLevel.INFO,
            component,
            f"Recovery completed: {event.payload}",
        )

    async def run_monitoring_loop(self):
        self._running = True
        logger.info(
            f"Monitoring loop started (interval={self._monitor_interval}s)"
        )

        while self._running:
            try:
                report = await self.check_all_health()

                if report.overall_score < 50:
                    logger.critical(
                        f"System health critical: {report.overall_score:.1f}/100"
                    )
                    await self._auto_heal(report)
                elif report.overall_score < 70:
                    logger.warning(
                        f"System health degraded: {report.overall_score:.1f}/100"
                    )

                self.save_checkpoint(
                    {
                        "last_health_score": report.overall_score,
                        "last_check": report.timestamp,
                    }
                )

            except Exception as e:
                logger.error(f"Monitoring loop error: {e}")

            await asyncio.sleep(self._monitor_interval)

    async def _auto_heal(self, report: HealthReport):
        critical_components = [
            name
            for name, comp in report.components.items()
            if comp.alert_level == AlertLevel.CRITICAL
        ]

        for name in critical_components:
            logger.info(f"Auto-healing critical component: {name}")
            await self.heal_component(name)

    async def shutdown(self):
        self._running = False
        self.save_checkpoint(
            {
                "shutdown_time": time.time(),
                "last_health_score": self.get_health_dashboard()[
                    "overall_health_score"
                ],
            }
        )
        await event_bus.emit(
            EventType.MONITORING_UPDATE,
            {"status": "shutdown"},
            source="monitoring_system",
        )
        logger.info("MonitoringSystem shut down")
