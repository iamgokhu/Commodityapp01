"""CommodityOS - Automated Market Intelligence Operating System.

Main entrypoint that orchestrates the full pipeline:
  Event Bus → Orchestrator → Crawlers → Data Pipeline → Knowledge Graph
  → Dashboard → Reports → GitHub → Monitoring → Loop
"""
import asyncio
import json
import logging
import sys
import time
from pathlib import Path

from commodity_os.core.events import Event, EventType, event_bus
from commodity_os.core.orchestrator import ResourceAwareOrchestrator, ScheduledTask, TaskPriority
from commodity_os.monitoring.monitor import MonitoringSystem
from commodity_os.meta_agents.orchestrator_agent import SystemOrchestratorAgent
from commodity_os.meta_agents.quality_agent import QualityAgent
from commodity_os.meta_agents.executive_agent import ExecutiveIntelligenceAgent
from commodity_os.github_integration.github import GitHubIntegration

CONFIG_PATH = Path(__file__).parent / "config.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("commodity_os.log"),
    ],
)
logger = logging.getLogger("commodity_os")


def load_config() -> dict:
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH) as f:
            return json.load(f)
    return {}


class CommodityOS:
    """Main orchestrator for the full automated pipeline."""

    def __init__(self, config: dict = None):
        file_config = load_config()
        self.config = {**file_config, **(config or {})}
        self.orchestrator = ResourceAwareOrchestrator()
        self.monitoring = MonitoringSystem()
        self.orchestrator_agent = SystemOrchestratorAgent(self.orchestrator)
        self.quality_agent = QualityAgent()
        self.executive_agent = ExecutiveIntelligenceAgent()
        self.github = GitHubIntegration(
            repo_path=self.config.get("repo_path", "."),
            remote=self.config.get("remote", "origin"),
            branch=self.config.get("branch", "main"),
        )
        self._running = False
        self._cycle_count = 0
        self._start_time = None

    def _default_config(self) -> dict:
        return {
            "repo_path": ".",
            "remote": "origin",
            "branch": "main",
            "max_cycles": 0,  # 0 = infinite
            "cycle_interval": 3600,  # 1 hour
            "auto_publish": True,
        }

    async def initialize(self):
        """Initialize all components."""
        logger.info("=== Initializing CommodityOS ===")

        await self.orchestrator.initialize()
        await self.monitoring.initialize()
        await self.orchestrator_agent.initialize()
        await self.quality_agent.initialize()
        await self.executive_agent.initialize()
        await self.github.initialize()

        event_bus.subscribe(EventType.CRAWLER_COMPLETED, self._on_crawler_completed)
        event_bus.subscribe(EventType.KNOWLEDGE_GRAPH_UPDATED, self._on_kg_updated)

        self._running = True
        self._start_time = time.time()
        logger.info("=== CommodityOS initialized ===")

    async def run(self):
        """Main execution loop."""
        await self.initialize()

        logger.info("Starting automated collection pipeline...")
        max_cycles = self.config.get("max_cycles", 0)
        cycle_interval = self.config.get("cycle_interval", 3600)

        try:
            while self._running:
                self._cycle_count += 1
                logger.info(f"\n{'='*60}")
                logger.info(f"CYCLE {self._cycle_count} START")
                logger.info(f"{'='*60}")

                dash = self.monitoring.get_health_dashboard()
                resources = {"health_score": dash.get("overall_health_score", 0)}
                await event_bus.emit(EventType.CYCLE_START, {
                    "cycle": self._cycle_count,
                    "resources": resources,
                    "recommendation": {"mode": "full", "batch_size": 100},
                }, source="main")

                await self.orchestrator.run_cycle()

                await event_bus.emit(EventType.CYCLE_COMPLETE, {
                    "cycle": self._cycle_count,
                    "completed": len([
                        t for t in self.orchestrator.active_tasks.values()
                        if hasattr(t, 'status') and t.status.name == "COMPLETED"
                    ]),
                }, source="main")

                if self.config.get("auto_publish", True):
                    await self.github.full_publish_cycle()

                dash = self.monitoring.get_health_dashboard()
                health = dash.get("overall_health_score", 0)
                quality = self.quality_agent.get_overall_quality_score()
                logger.info(f"Cycle {self._cycle_count} complete - Health: {health:.0f}/100, Quality: {quality:.2%}")

                if max_cycles > 0 and self._cycle_count >= max_cycles:
                    logger.info(f"Reached max cycles ({max_cycles}), stopping")
                    break

                logger.info(f"Next cycle in {cycle_interval}s...")
                await asyncio.sleep(cycle_interval)

        except KeyboardInterrupt:
            logger.info("Shutdown requested (Ctrl+C)")
        finally:
            await self.shutdown()

    async def _on_crawler_completed(self, event: Event):
        logger.info(f"Crawler completed: {event.payload.get('crawler', 'unknown')}")

    async def _on_kg_updated(self, event: Event):
        stats = event.payload.get("stats", {})
        logger.info(f"Knowledge Graph updated: {stats.get('total_nodes', 0)} nodes, {stats.get('total_edges', 0)} edges")

    async def shutdown(self):
        """Gracefully shutdown all components."""
        logger.info("=== Shutting down CommodityOS ===")
        self._running = False

        uptime = time.time() - self._start_time if self._start_time else 0
        logger.info(f"Total uptime: {uptime:.0f}s, Cycles completed: {self._cycle_count}")

        await self.github.auto_commit(f"Session end: {self._cycle_count} cycles completed")
        await self.executive_agent.shutdown()
        await self.quality_agent.shutdown()
        await self.orchestrator_agent.shutdown()
        await self.monitoring.shutdown()
        await self.orchestrator.shutdown()

        logger.info("=== CommodityOS shut down ===")

    def get_status(self) -> dict:
        return {
            "running": self._running,
            "cycle_count": self._cycle_count,
            "uptime": time.time() - self._start_time if self._start_time else 0,
            "orchestrator": self.orchestrator_agent.get_status(),
            "quality": self.quality_agent.get_quality_report(),
            "executive": self.executive_agent.get_executive_summary(),
            "github": self.github.get_status(),
            "monitoring": self.monitoring.get_health_dashboard(),
        }


async def main():
    os = CommodityOS()
    await os.run()


if __name__ == "__main__":
    asyncio.run(main())
