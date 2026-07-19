"""Resource-aware orchestrator - monitors system resources and schedules work."""
import asyncio
import logging
import os
import time
import psutil
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from enum import Enum

from commodity_os.core.events import Event, EventType, event_bus

logger = logging.getLogger(__name__)


class TaskPriority(Enum):
    CRITICAL = 0
    HIGH = 1
    MEDIUM = 2
    LOW = 3
    DEFERRED = 4


class TaskStatus(Enum):
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    DEFERRED = "deferred"
    CANCELLED = "cancelled"


@dataclass
class ResourceSnapshot:
    timestamp: float = 0.0
    cpu_percent: float = 0.0
    ram_total_gb: float = 0.0
    ram_used_gb: float = 0.0
    ram_percent: float = 0.0
    disk_total_gb: float = 0.0
    disk_used_gb: float = 0.0
    disk_percent: float = 0.0
    gpu_available: bool = False
    gpu_name: str = "N/A"
    gpu_memory_gb: float = 0.0
    network_available: bool = True
    load_average: float = 0.0
    runtime_remaining_minutes: float = 999.0
    queue_length: int = 0
    api_budget_remaining: float = 1.0
    estimated_workload_cost: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "cpu_percent": self.cpu_percent,
            "ram_total_gb": self.ram_total_gb,
            "ram_used_gb": self.ram_used_gb,
            "ram_percent": self.ram_percent,
            "disk_total_gb": self.disk_total_gb,
            "disk_used_gb": self.disk_used_gb,
            "disk_percent": self.disk_percent,
            "gpu_available": self.gpu_available,
            "gpu_name": self.gpu_name,
            "gpu_memory_gb": self.gpu_memory_gb,
            "network_available": self.network_available,
            "load_average": self.load_average,
            "runtime_remaining_minutes": self.runtime_remaining_minutes,
            "queue_length": self.queue_length,
            "api_budget_remaining": self.api_budget_remaining,
        }


@dataclass
class ScheduledTask:
    task_id: str
    name: str
    priority: TaskPriority
    status: TaskStatus = TaskStatus.PENDING
    assigned_to: str = ""
    estimated_cost: float = 0.0
    estimated_duration: float = 0.0
    actual_duration: float = 0.0
    created_at: float = 0.0
    started_at: float = 0.0
    completed_at: float = 0.0
    error: str = ""
    retry_count: int = 0
    max_retries: int = 3
    payload: Dict[str, Any] = field(default_factory=dict)


class ResourceMonitor:
    def __init__(self):
        self._history: List[ResourceSnapshot] = []
        self._max_history = 1000

    async def snapshot(self) -> ResourceSnapshot:
        snap = ResourceSnapshot(timestamp=time.time())

        try:
            snap.cpu_percent = psutil.cpu_percent(interval=0.1)
            snap.load_average = os.getloadavg()[0] if hasattr(os, 'getloadavg') else snap.cpu_percent / 100.0
        except Exception:
            snap.cpu_percent = 50.0

        try:
            vm = psutil.virtual_memory()
            snap.ram_total_gb = vm.total / (1024**3)
            snap.ram_used_gb = vm.used / (1024**3)
            snap.ram_percent = vm.percent
        except Exception:
            snap.ram_percent = 50.0

        try:
            du = psutil.disk_usage("/")
            snap.disk_total_gb = du.total / (1024**3)
            snap.disk_used_gb = du.used / (1024**3)
            snap.disk_percent = du.percent
        except Exception:
            try:
                du = psutil.disk_usage("C:\\")
                snap.disk_total_gb = du.total / (1024**3)
                snap.disk_used_gb = du.used / (1024**3)
                snap.disk_percent = du.percent
            except Exception:
                snap.disk_percent = 50.0

        try:
            import torch
            snap.gpu_available = torch.cuda.is_available()
            if snap.gpu_available:
                snap.gpu_name = torch.cuda.get_device_name(0)
                mem = torch.cuda.get_device_properties(0)
                snap.gpu_memory_gb = mem.total_mem / (1024**3)
        except Exception:
            snap.gpu_available = False

        try:
            import requests
            resp = requests.get("https://httpbin.org/get", timeout=3)
            snap.network_available = resp.status_code == 200
        except Exception:
            snap.network_available = False

        self._history.append(snap)
        if len(self._history) > self._max_history:
            self._history = self._history[-self._max_history:]

        return snap

    def get_health_score(self, snap: ResourceSnapshot) -> float:
        score = 100.0
        if snap.cpu_percent > 90:
            score -= 30
        elif snap.cpu_percent > 70:
            score -= 15
        if snap.ram_percent > 90:
            score -= 30
        elif snap.ram_percent > 75:
            score -= 10
        if snap.disk_percent > 95:
            score -= 40
        elif snap.disk_percent > 85:
            score -= 15
        if not snap.network_available:
            score -= 50
        if snap.api_budget_remaining < 0.1:
            score -= 20
        return max(0.0, score)

    def get_schedule_recommendation(self, snap: ResourceSnapshot) -> Dict[str, Any]:
        health = self.get_health_score(snap)
        if health >= 80:
            return {"mode": "full", "parallel_tasks": 8, "batch_size": 100}
        elif health >= 60:
            return {"mode": "reduced", "parallel_tasks": 4, "batch_size": 50}
        elif health >= 40:
            return {"mode": "minimal", "parallel_tasks": 2, "batch_size": 20}
        else:
            return {"mode": "deferred", "parallel_tasks": 1, "batch_size": 5}


class ResourceAwareOrchestrator:
    def __init__(self):
        self.resource_monitor = ResourceMonitor()
        self.task_queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self.active_tasks: Dict[str, ScheduledTask] = {}
        self.completed_tasks: List[ScheduledTask] = []
        self._running = False
        self._cycle_count = 0

    async def initialize(self):
        logger.info("Resource-Aware Orchestrator initializing...")
        event_bus.subscribe(EventType.SYSTEM_START, self._on_system_start)
        event_bus.subscribe(EventType.RESOURCE_CHECK, self._on_resource_check)
        self._running = True
        await event_bus.emit(EventType.RESOURCE_CHECK, source="orchestrator")

    async def _on_system_start(self, event: Event):
        snap = await self.resource_monitor.snapshot()
        logger.info(f"System health: {self.resource_monitor.get_health_score(snap):.1f}/100")

    async def _on_resource_check(self, event: Event):
        snap = await self.resource_monitor.snapshot()
        recommendation = self.resource_monitor.get_schedule_recommendation(snap)
        logger.info(f"Resource check: CPU={snap.cpu_percent:.1f}% RAM={snap.ram_percent:.1f}% "
                     f"Disk={snap.disk_percent:.1f}% Health={self.resource_monitor.get_health_score(snap):.1f}")
        if self.resource_monitor.get_health_score(snap) < 40:
            await event_bus.emit(EventType.RESOURCE_LOW, {"snapshot": snap.to_dict()}, source="orchestrator")

    async def submit_task(self, task: ScheduledTask) -> str:
        snap = await self.resource_monitor.snapshot()
        rec = self.resource_monitor.get_schedule_recommendation(snap)

        if rec["mode"] == "deferred" and task.priority not in (TaskPriority.CRITICAL, TaskPriority.HIGH):
            task.status = TaskStatus.DEFERRED
            self.completed_tasks.append(task)
            logger.info(f"Task {task.name} deferred (low resources)")
            return task.task_id

        task.status = TaskStatus.QUEUED
        task.created_at = time.time()
        await self.task_queue.put((task.priority.value, task.task_id, task))
        logger.info(f"Task {task.name} queued (priority={task.priority.name})")
        return task.task_id

    async def run_cycle(self):
        self._cycle_count += 1
        snap = await self.resource_monitor.snapshot()
        rec = self.resource_monitor.get_schedule_recommendation(snap)
        max_parallel = rec["parallel_tasks"]

        await event_bus.emit(EventType.CYCLE_START, {
            "cycle": self._cycle_count,
            "resources": snap.to_dict(),
            "recommendation": rec
        }, source="orchestrator")

        running = []
        while not self.task_queue.empty() and len(running) < max_parallel:
            try:
                _, _, task = self.task_queue.get_nowait()
                task.status = TaskStatus.RUNNING
                task.started_at = time.time()
                self.active_tasks[task.task_id] = task
                running.append(self._execute_task(task))
            except asyncio.QueueEmpty:
                break

        if running:
            await asyncio.gather(*running, return_exceptions=True)

        await event_bus.emit(EventType.CYCLE_COMPLETE, {
            "cycle": self._cycle_count,
            "completed": len(self.completed_tasks),
        }, source="orchestrator")

    async def _execute_task(self, task: ScheduledTask):
        try:
            await event_bus.emit(EventType.AGENT_COMPLETED, {
                "task_id": task.task_id,
                "task_name": task.name,
            }, source=task.assigned_to)
            task.status = TaskStatus.COMPLETED
            task.completed_at = time.time()
            task.actual_duration = task.completed_at - task.started_at
        except Exception as e:
            task.status = TaskStatus.FAILED
            task.error = str(e)
            task.completed_at = time.time()
            if task.retry_count < task.max_retries:
                task.retry_count += 1
                task.status = TaskStatus.PENDING
                await self.task_queue.put((task.priority.value, task.task_id, task))
            else:
                await event_bus.emit(EventType.AGENT_FAILED, {
                    "task_id": task.task_id,
                    "error": str(e)
                }, source=task.assigned_to)
        finally:
            self.active_tasks.pop(task.task_id, None)
            self.completed_tasks.append(task)

    async def shutdown(self):
        self._running = False
        await event_bus.emit(EventType.SYSTEM_STOP, source="orchestrator")
        logger.info("Orchestrator shut down")

    def get_status(self) -> Dict[str, Any]:
        return {
            "cycle_count": self._cycle_count,
            "queue_size": self.task_queue.qsize(),
            "active_tasks": len(self.active_tasks),
            "completed_tasks": len(self.completed_tasks),
            "failed_tasks": len([t for t in self.completed_tasks if t.status == TaskStatus.FAILED]),
        }
