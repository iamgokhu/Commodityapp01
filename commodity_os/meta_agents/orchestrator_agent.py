"""Meta Agent 1: System Orchestrator - plans execution, assigns tasks, balances workloads."""
import asyncio
import logging
import time
from typing import Any, Dict, List, Optional

from commodity_os.core.events import Event, EventType, event_bus
from commodity_os.core.orchestrator import ResourceAwareOrchestrator, ScheduledTask, TaskPriority, TaskStatus

logger = logging.getLogger(__name__)


class SystemOrchestratorAgent:
    """Meta Agent 1: Plans execution, assigns tasks, balances workloads, monitors resources."""

    def __init__(self, orchestrator: ResourceAwareOrchestrator):
        self.orchestrator = orchestrator
        self.execution_plan: List[Dict[str, Any]] = []
        self.task_assignments: Dict[str, str] = {}
        self._running = False
        self._cycle_count = 0

    async def initialize(self):
        self._running = True
        event_bus.subscribe(EventType.CYCLE_START, self._on_cycle_start)
        event_bus.subscribe(EventType.RESOURCE_LOW, self._on_resource_low)
        event_bus.subscribe(EventType.CRAWLER_FAILED, self._on_crawler_failed)
        event_bus.subscribe(EventType.AGENT_FAILED, self._on_agent_failed)
        logger.info("System Orchestrator Agent initialized")

    async def _on_cycle_start(self, event: Event):
        self._cycle_count += 1
        cycle = event.payload.get("cycle", self._cycle_count)
        resources = event.payload.get("resources", {})
        recommendation = event.payload.get("recommendation", {})

        logger.info(f"Cycle {cycle}: Planning execution with mode={recommendation.get('mode', 'full')}")

        self.execution_plan = await self._create_execution_plan(resources, recommendation)

        for task_plan in self.execution_plan:
            task = ScheduledTask(
                task_id=task_plan["task_id"],
                name=task_plan["name"],
                priority=TaskPriority[task_plan["priority"]],
                assigned_to=task_plan.get("assigned_to", ""),
                estimated_cost=task_plan.get("estimated_cost", 0.0),
                payload=task_plan.get("payload", {}),
            )
            await self.orchestrator.submit_task(task)

    async def _create_execution_plan(self, resources: Dict, recommendation: Dict) -> List[Dict[str, Any]]:
        plan = []
        mode = recommendation.get("mode", "full")
        batch_size = recommendation.get("batch_size", 100)

        base_tasks = [
            {"name": "api_health_check", "priority": "CRITICAL", "estimated_cost": 0.01},
            {"name": "resource_detection", "priority": "CRITICAL", "estimated_cost": 0.01},
            {"name": "crawl_indiamart", "priority": "HIGH", "estimated_cost": 0.05},
            {"name": "crawl_tradeindia", "priority": "HIGH", "estimated_cost": 0.05},
            {"name": "crawl_agmarknet", "priority": "HIGH", "estimated_cost": 0.03},
            {"name": "crawl_apmc", "priority": "MEDIUM", "estimated_cost": 0.03},
            {"name": "crawl_export_dir", "priority": "MEDIUM", "estimated_cost": 0.04},
            {"name": "crawl_amazon_business", "priority": "HIGH", "estimated_cost": 0.05},
            {"name": "crawl_flipkart_wholesale", "priority": "HIGH", "estimated_cost": 0.05},
            {"name": "crawl_gov_api", "priority": "CRITICAL", "estimated_cost": 0.03},
            {"name": "crawl_linkedin", "priority": "MEDIUM", "estimated_cost": 0.06},
            {"name": "crawl_news", "priority": "MEDIUM", "estimated_cost": 0.04},
            {"name": "data_validation", "priority": "HIGH", "estimated_cost": 0.02},
            {"name": "data_dedup", "priority": "HIGH", "estimated_cost": 0.02},
            {"name": "data_cleaning", "priority": "HIGH", "estimated_cost": 0.02},
            {"name": "data_normalization", "priority": "MEDIUM", "estimated_cost": 0.02},
            {"name": "entity_recognition", "priority": "MEDIUM", "estimated_cost": 0.03},
            {"name": "commodity_classification", "priority": "MEDIUM", "estimated_cost": 0.02},
            {"name": "sector_classification", "priority": "MEDIUM", "estimated_cost": 0.02},
            {"name": "knowledge_graph_update", "priority": "MEDIUM", "estimated_cost": 0.03},
            {"name": "trend_analysis", "priority": "LOW", "estimated_cost": 0.04},
            {"name": "risk_analysis", "priority": "LOW", "estimated_cost": 0.04},
            {"name": "forecast_generation", "priority": "LOW", "estimated_cost": 0.05},
            {"name": "executive_summary", "priority": "MEDIUM", "estimated_cost": 0.02},
            {"name": "dashboard_generation", "priority": "HIGH", "estimated_cost": 0.03},
            {"name": "report_generation", "priority": "MEDIUM", "estimated_cost": 0.02},
            {"name": "github_publish", "priority": "LOW", "estimated_cost": 0.01},
        ]

        for i, task in enumerate(base_tasks):
            if mode == "deferred" and task["priority"] in ("LOW", "MEDIUM"):
                continue
            if mode == "minimal" and task["priority"] == "LOW":
                continue

            task["task_id"] = f"cycle_{self._cycle_count}_task_{i}"
            plan.append(task)

        return plan

    async def _on_resource_low(self, event: Event):
        logger.warning("Resource low detected - adjusting execution plan")
        snapshot = event.payload.get("snapshot", {})

        for task_id, task in list(self.orchestrator.active_tasks.items()):
            if task.priority in (TaskPriority.LOW, TaskPriority.DEFERRED):
                task.status = TaskStatus.DEFERRED
                logger.info(f"Deferred task {task.name} due to low resources")

    async def _on_crawler_failed(self, event: Event):
        crawler_name = event.payload.get("crawler", "unknown")
        error = event.payload.get("error", "unknown")
        logger.warning(f"Crawler {crawler_name} failed: {error}")

        replacement_task = ScheduledTask(
            task_id=f"replace_{crawler_name}_{int(time.time())}",
            name=f"replace_{crawler_name}",
            priority=TaskPriority.HIGH,
            payload={"crawler": crawler_name, "action": "replace"},
        )
        await self.orchestrator.submit_task(replacement_task)

    async def _on_agent_failed(self, event: Event):
        agent_id = event.payload.get("agent_id", "unknown")
        error = event.payload.get("error", "unknown")
        logger.warning(f"Agent {agent_id} failed: {error}")

    async def shutdown(self):
        self._running = False
        logger.info("System Orchestrator Agent shut down")

    def get_status(self) -> Dict[str, Any]:
        return {
            "cycle_count": self._cycle_count,
            "plan_size": len(self.execution_plan),
            "running": self._running,
        }
