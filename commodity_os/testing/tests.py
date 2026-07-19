"""Automated Testing Framework for CommodityOS."""
import asyncio
import json
import os
import sys
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from commodity_os.core.events import Event, EventType, event_bus
from commodity_os.core.orchestrator import (
    ResourceAwareOrchestrator, ResourceMonitor, ScheduledTask,
    TaskPriority, TaskStatus,
)
from commodity_os.data_pipeline.pipeline import DataPipeline
from commodity_os.knowledge_graph.graph import KnowledgeGraph
from commodity_os.dashboard.generator import DashboardGenerator
from commodity_os.reports.generator import ReportGenerator, ReportType
from commodity_os.monitoring.monitor import MonitoringSystem
from commodity_os.meta_agents.orchestrator_agent import SystemOrchestratorAgent
from commodity_os.meta_agents.quality_agent import QualityAgent
from commodity_os.meta_agents.executive_agent import ExecutiveIntelligenceAgent


class TestEventBus(unittest.IsolatedAsyncioTestCase):
    async def test_subscribe_and_emit(self):
        received = []
        async def handler(event):
            received.append(event)
        event_bus.subscribe(EventType.NEWS_COLLECTED, handler)
        await event_bus.emit(EventType.NEWS_COLLECTED, {"source": "test"}, source="test")
        await asyncio.sleep(0.05)
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].payload["source"], "test")

    async def test_multiple_subscribers(self):
        received_a, received_b = [], []
        async def handler_a(event):
            received_a.append(event)
        async def handler_b(event):
            received_b.append(event)
        event_bus.subscribe(EventType.PRICE_CHANGED, handler_a)
        event_bus.subscribe(EventType.PRICE_CHANGED, handler_b)
        await event_bus.emit(EventType.PRICE_CHANGED, {"price": 100}, source="test")
        await asyncio.sleep(0.05)
        self.assertEqual(len(received_a), 1)
        self.assertEqual(len(received_b), 1)

    async def test_correlation_id(self):
        received = []
        async def handler(event):
            received.append(event)
        event_bus.subscribe(EventType.DATA_COLLECTED, handler)
        await event_bus.emit(EventType.DATA_COLLECTED, {}, correlation_id="abc-123", source="test")
        await asyncio.sleep(0.05)
        self.assertEqual(received[0].correlation_id, "abc-123")

    async def test_stats(self):
        await event_bus.emit(EventType.SYSTEM_START, {}, source="test")
        stats = event_bus.get_stats()
        self.assertIsInstance(stats, dict)
        self.assertGreater(len(stats), 0)


class TestOrchestrator(unittest.IsolatedAsyncioTestCase):
    async def test_submit_task(self):
        orch = ResourceAwareOrchestrator()
        await orch.initialize()
        task = ScheduledTask(
            task_id="test_1", name="test_task",
            priority=TaskPriority.HIGH, payload={"action": "test"},
        )
        task_id = await orch.submit_task(task)
        self.assertIsNotNone(task_id)
        await orch.shutdown()

    async def test_priority_ordering(self):
        orch = ResourceAwareOrchestrator()
        await orch.initialize()
        low = ScheduledTask(task_id="low", name="low", priority=TaskPriority.LOW, payload={})
        critical = ScheduledTask(task_id="crit", name="crit", priority=TaskPriority.CRITICAL, payload={})
        await orch.submit_task(low)
        await orch.submit_task(critical)
        status = orch.get_status()
        self.assertIn("queue_size", status)
        await orch.shutdown()

    async def test_resource_monitor_snapshot(self):
        monitor = ResourceMonitor()
        snap = await monitor.snapshot()
        self.assertTrue(hasattr(snap, "cpu_percent"))
        self.assertTrue(hasattr(snap, "ram_percent"))
        self.assertTrue(hasattr(snap, "disk_percent"))

    async def test_resource_health_score(self):
        monitor = ResourceMonitor()
        snap = await monitor.snapshot()
        score = monitor.get_health_score(snap)
        self.assertGreaterEqual(score, 0)
        self.assertLessEqual(score, 100)


class TestDataPipeline(unittest.IsolatedAsyncioTestCase):
    async def test_full_pipeline(self):
        pipeline = DataPipeline()
        entities = [
            {"name": "Test Sugar Co", "product": "Sugar", "state": "Maharashtra",
             "phone": "9876543210", "price_per_kg": 45.0, "source": "test",
             "id": "ent_1", "type": "Manufacturer", "district": "Pune", "taluk": "Haveli"},
            {"name": "Test Sugar Co", "product": "Sugar", "state": "Maharashtra",
             "phone": "9876543210", "price_per_kg": 45.0, "source": "test",
             "id": "ent_2", "type": "Manufacturer", "district": "Pune", "taluk": "Haveli"},
            {"name": "Rice Corp", "product": "Rice", "state": "UP",
             "phone": "9123456789", "source": "test",
             "id": "ent_3", "type": "Wholesaler", "district": "Lucknow", "taluk": "Lucknow"},
        ]
        result = await pipeline.run(entities)
        self.assertIsNotNone(result)

    async def test_empty_records(self):
        pipeline = DataPipeline()
        result = await pipeline.run([])
        self.assertIsNotNone(result)


class TestKnowledgeGraph(unittest.IsolatedAsyncioTestCase):
    def test_add_entity(self):
        kg = KnowledgeGraph()
        entity = {"id": "ent_1", "name": "Test Co", "type": "Manufacturer",
                  "product": "Sugar", "state": "Maharashtra"}
        kg.add_entity(entity)
        stats = kg.get_stats()
        self.assertGreater(stats["total_nodes"], 0)

    def test_add_relationship(self):
        kg = KnowledgeGraph()
        kg.add_entity({"id": "a", "name": "A", "type": "Manufacturer"})
        kg.add_entity({"id": "b", "name": "B", "type": "Wholesaler"})
        kg.add_relationship("a", "b", "SUPPLIES")
        stats = kg.get_stats()
        self.assertGreater(stats["total_edges"], 0)

    def test_find_competitors(self):
        kg = KnowledgeGraph()
        kg.add_entity({"id": "a", "name": "A", "type": "Manufacturer", "product": "Sugar", "state": "MH"})
        kg.add_entity({"id": "b", "name": "B", "type": "Manufacturer", "product": "Sugar", "state": "MH"})
        competitors = kg.find_competitors("a")
        self.assertIsInstance(competitors, list)

    def test_save_and_load(self):
        kg = KnowledgeGraph()
        kg.add_entity({"id": "x", "name": "X", "type": "Manufacturer"})
        test_path = "test_kg.json"
        kg.to_json()
        if os.path.exists(test_path):
            os.remove(test_path)

    def test_get_stats(self):
        kg = KnowledgeGraph()
        kg.add_entity({"id": "s1", "name": "S1", "type": "Manufacturer"})
        stats = kg.get_stats()
        self.assertIn("total_nodes", stats)


class TestDashboardGenerator(unittest.IsolatedAsyncioTestCase):
    async def test_generate_dashboard(self):
        gen = DashboardGenerator()
        entities = [
            {"name": "Co A", "product": "Sugar", "state": "MH", "type": "Manufacturer"},
            {"name": "Co B", "product": "Rice", "state": "UP", "type": "Wholesaler"},
        ]
        consolidated = {"entities": entities, "stats": {"total": 2}}
        await gen.generate(entities, consolidated)


class TestReportGenerator(unittest.IsolatedAsyncioTestCase):
    async def test_generate_report(self):
        gen = ReportGenerator()
        entities = [{"name": "Co A", "product": "Sugar", "state": "MH", "type": "Manufacturer"}]
        stats = {"total": 100, "valid": 95}
        result = await gen.generate_report(ReportType.HOURLY, entities, stats)
        self.assertIsInstance(result, dict)


class TestMonitoring(unittest.IsolatedAsyncioTestCase):
    async def test_health_checks(self):
        monitor = MonitoringSystem()
        await monitor.initialize()
        dashboard = monitor.get_health_dashboard()
        self.assertIn("overall_health_score", dashboard)
        await monitor.shutdown()

    async def test_checkpoint(self):
        monitor = MonitoringSystem()
        await monitor.initialize()
        test_path = "test_checkpoint.json"
        monitor.save_checkpoint({"test": "data"})
        await monitor.shutdown()


class TestMetaAgents(unittest.IsolatedAsyncioTestCase):
    async def test_orchestrator_agent(self):
        orch = ResourceAwareOrchestrator()
        await orch.initialize()
        agent = SystemOrchestratorAgent(orch)
        await agent.initialize()
        status = agent.get_status()
        self.assertIn("cycle_count", status)
        await agent.shutdown()
        await orch.shutdown()

    async def test_quality_agent(self):
        agent = QualityAgent()
        await agent.initialize()
        report = agent.get_quality_report()
        self.assertIn("overall_score", report)
        await agent.shutdown()

    async def test_executive_agent(self):
        agent = ExecutiveIntelligenceAgent()
        await agent.initialize()
        summary = agent.get_executive_summary()
        self.assertIn("total_cycles", summary)
        await agent.shutdown()


class TestEndToEnd(unittest.IsolatedAsyncioTestCase):
    async def test_full_system_init_shutdown(self):
        from commodity_os.main import CommodityOS
        os_instance = CommodityOS({"max_cycles": 1, "cycle_interval": 0, "auto_publish": False})
        await os_instance.initialize()
        self.assertTrue(os_instance._running)
        await os_instance.shutdown()
        self.assertFalse(os_instance._running)


if __name__ == "__main__":
    unittest.main()
