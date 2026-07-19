"""Integration layer: Crawlers → Data Pipeline → Knowledge Graph → Dashboard → GitHub.

Wires all 28 crawlers to the data pipeline, processes results through the
knowledge graph, generates dashboard, and publishes to GitHub Pages.
"""
import asyncio
import json
import logging
import random
import time
from pathlib import Path
from typing import Any, Dict, List

from commodity_os.core.events import event_bus, EventType
from commodity_os.core.orchestrator import ResourceAwareOrchestrator
from commodity_os.crawlers.base import CrawlerManager
from commodity_os.data_pipeline.pipeline import DataPipeline
from commodity_os.knowledge_graph.graph import KnowledgeGraph
from commodity_os.dashboard.generator import DashboardGenerator

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output"
DOCS_DIR = Path(__file__).resolve().parent.parent / "docs"


# ---------------------------------------------------------------------------
# Schema mapping: crawler output → data pipeline expected fields
# ---------------------------------------------------------------------------

def _map_crawler_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Transform a crawler-generated record into the schema the data pipeline expects."""
    contact = record.get("contact", {})
    pricing = record.get("pricing", {})
    geo = record.get("geography", {})

    return {
        "company_name": record.get("name", ""),
        "product": record.get("commodity", record.get("commodity_group", "")),
        "category": record.get("commodity_group", ""),
        "entity_type": record.get("entity_type", ""),
        "contact_phone": contact.get("phone", ""),
        "address": record.get("office_address", ""),
        "district": geo.get("district", record.get("district", "")),
        "state": geo.get("state", record.get("state", "")),
        "taluk": geo.get("taluk", record.get("taluk", "")),
        "email": contact.get("email", ""),
        "website": contact.get("website", ""),
        "gst_number": record.get("gst_number", ""),
        "year_of_establishment": record.get("year_of_establishment", ""),
        "market_price": pricing.get("market_price", 0),
        "purchase_price": pricing.get("purchase_price", 0),
        "selling_price": pricing.get("selling_price", 0),
        "price_unit": pricing.get("unit", "KG"),
        "currency": pricing.get("currency", "INR"),
        "payment_terms": record.get("payment_terms", ""),
        "delivery_available": record.get("delivery_available", False),
        "source": record.get("source", ""),
        "collected_at": record.get("collected_at", ""),
        "_raw": record,
    }


# ---------------------------------------------------------------------------
# Integration orchestrator
# ---------------------------------------------------------------------------

class IntegrationOrchestrator:
    """Wires the full pipeline: Crawl → Process → Graph → Dashboard."""

    def __init__(self, config: dict = None):
        self.config = config or {}
        self.crawler_manager = CrawlerManager()
        self.pipeline = DataPipeline()
        self.graph = KnowledgeGraph()
        self.orchestrator = ResourceAwareOrchestrator()
        self._register_all_crawlers()

    def _register_all_crawlers(self):
        """Register all 28 crawlers in the CrawlerManager."""
        from commodity_os.crawlers.base import (
            IndiaMARTCrawler, TradeIndiaCrawler, AgMarkNetCrawler, APMCCrawler,
            ExportDirectoryCrawler, AmazonBusinessCrawler, FlipkartWholesaleCrawler,
            GovernmentAPICrawler, LinkedInCrawler, NewsCrawler,
            JioMartCrawler, DMartCrawler, BigBasketCrawler, BlinkitCrawler,
            ZeptoCrawler, SwiggyInstamartCrawler, RelianceFreshCrawler,
            MoreRetailCrawler, SpencersCrawler, WalmartGlobalCrawler,
            CostcoGlobalCrawler, CarrefourGlobalCrawler, TescoGlobalCrawler,
            AlibabaCrawler, AmazonGlobalCrawler, SeafoodExporterCrawler,
            CorporateFarmCrawler, MarineHarvestCrawler,
        )
        crawlers = [
            IndiaMARTCrawler(),
            TradeIndiaCrawler(),
            AgMarkNetCrawler(),
            APMCCrawler(),
            ExportDirectoryCrawler(),
            AmazonBusinessCrawler(),
            FlipkartWholesaleCrawler(),
            GovernmentAPICrawler(),
            LinkedInCrawler(),
            NewsCrawler(),
            JioMartCrawler(),
            DMartCrawler(),
            BigBasketCrawler(),
            BlinkitCrawler(),
            ZeptoCrawler(),
            SwiggyInstamartCrawler(),
            RelianceFreshCrawler(),
            MoreRetailCrawler(),
            SpencersCrawler(),
            WalmartGlobalCrawler(),
            CostcoGlobalCrawler(),
            CarrefourGlobalCrawler(),
            TescoGlobalCrawler(),
            AlibabaCrawler(),
            AmazonGlobalCrawler(),
            SeafoodExporterCrawler(),
            CorporateFarmCrawler(),
            MarineHarvestCrawler(),
        ]
        for crawler in crawlers:
            self.crawler_manager.register(crawler)
        logger.info(f"Registered {len(crawlers)} crawlers")

    async def run_collection_cycle(self) -> Dict[str, Any]:
        """Run a full collection cycle across all crawlers."""
        cycle_start = time.time()
        logger.info("=== Starting collection cycle ===")

        # 1. Run orchestrator cycle to check resources
        await self.orchestrator.initialize()
        snap = await self.orchestrator.resource_monitor.snapshot()
        rec = self.orchestrator.resource_monitor.get_schedule_recommendation(snap)
        logger.info(f"Resources: CPU={snap.cpu_percent:.1f}% RAM={snap.ram_percent:.1f}% "
                     f"Health={self.orchestrator.resource_monitor.get_health_score(snap):.1f}")

        # 2. Collect raw data from all crawlers
        all_raw_entities = []
        crawler_stats = {}

        for crawler_info in self.crawler_manager.list_crawlers():
            crawler_id = crawler_info["crawler_id"]
            crawler = self.crawler_manager.get_crawler(crawler_id)
            if not crawler:
                continue

            try:
                task = {"commodity": "all", "region": "all"}
                entities = await crawler.fetch(task)
                all_raw_entities.extend(entities)
                crawler_stats[crawler_id] = {"entities_collected": len(entities), "status": "success"}
                logger.info(f"Crawler {crawler_id}: {len(entities)} entities collected")
            except Exception as e:
                crawler_stats[crawler_id] = {"entities_collected": 0, "status": "error", "error": str(e)}
                logger.error(f"Crawler {crawler_id} failed: {e}")

        logger.info(f"Total raw entities collected: {len(all_raw_entities)}")

        # 3. Map crawler output to data pipeline schema
        mapped_records = [_map_crawler_record(e) for e in all_raw_entities]
        logger.info(f"Mapped {len(mapped_records)} records to pipeline schema")

        # 4. Run through data pipeline
        pipeline_result = await self.pipeline.run(mapped_records)
        logger.info(f"Pipeline result: {pipeline_result.records_in} → {pipeline_result.records_out} records")
        for stage in pipeline_result.stages:
            logger.info(f"  Stage {stage.stage_name}: {stage.input_count} → {stage.output_count} "
                        f"({stage.processing_time_ms:.1f}ms, {stage.errors} errors)")

        # 5. Add surviving records to knowledge graph
        graph_entities = 0
        graph_commodities = set()
        graph_geographies = set()

        for record in mapped_records:
            # Add entity
            name = record.get("company_name", "")
            if name:
                self.graph.add_entity({
                    "name": name,
                    "entity_type": record.get("entity_type", ""),
                    "source": record.get("source", ""),
                    "state": record.get("state", ""),
                    "district": record.get("district", ""),
                    "commodity": record.get("product", ""),
                })
                graph_entities += 1

                # Add commodity node and SUPPLIES relationship
                commodity = record.get("product", "")
                if commodity:
                    self.graph.add_commodity(commodity)
                    commodity_id = f"commodity:{commodity.lower().strip()}"
                    entity_id = f"entity:{name.lower().strip()}"
                    self.graph.add_relationship(entity_id, commodity_id, "SUPPLIES")
                    graph_commodities.add(commodity)

                # Add geography nodes and LOCATED_IN relationship
                state = record.get("state", "")
                district = record.get("district", "")
                if state:
                    self.graph.add_geography(state, "state")
                    state_id = f"geography:{state.lower().strip()}"
                    entity_id = f"entity:{name.lower().strip()}"
                    self.graph.add_relationship(entity_id, state_id, "LOCATED_IN")
                    graph_geographies.add(state)
                if district:
                    self.graph.add_geography(district, "district")

        logger.info(f"Knowledge graph: {graph_entities} entities, {len(graph_commodities)} commodities, "
                     f"{len(graph_geographies)} geographies")

        # 6. Save results
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        all_entities_file = OUTPUT_DIR / "all_entities.json"
        with open(all_entities_file, "w", encoding="utf-8") as f:
            json.dump(all_raw_entities, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved {len(all_raw_entities)} raw entities to {all_entities_file}")

        stats_file = OUTPUT_DIR / "pipeline_stats.json"
        with open(stats_file, "w", encoding="utf-8") as f:
            json.dump({
                "cycle": self._cycle_count if hasattr(self, '_cycle_count') else 0,
                "raw_entities": len(all_raw_entities),
                "pipeline_output": pipeline_result.records_out,
                "pipeline_stages": [s.__dict__ for s in pipeline_result.stages],
                "graph_stats": self.graph.get_stats(),
                "crawler_stats": crawler_stats,
                "resources": snap.to_dict(),
            }, f, indent=2, default=str)

        # 7. Discover relationships
        discovered = self.graph.discover_relationships()
        logger.info(f"Discovered relationships: {discovered}")

        # 8. Generate dashboard
        dash_gen = DashboardGenerator(str(OUTPUT_DIR))
        dashboard_entities = []
        for record in mapped_records:
            dashboard_entities.append({
                "name": record.get("company_name", ""),
                "type": record.get("entity_type", ""),
                "product": record.get("product", ""),
                "category": record.get("category", record.get("commodity_group", "")),
                "state": record.get("state", ""),
                "district": record.get("district", ""),
                "taluk": record.get("taluk", ""),
                "phone": record.get("contact_phone", ""),
                "email": record.get("email", ""),
                "website": record.get("website", ""),
                "market_price": record.get("market_price", 0),
                "purchase_price": record.get("purchase_price", 0),
                "selling_price": record.get("selling_price", 0),
                "unit": record.get("price_unit", "KG"),
                "source": record.get("source", ""),
                "year": record.get("year_of_establishment", ""),
                "gst": record.get("gst_number", ""),
                "confidence": 0.8,
                "entity_type": record.get("entity_type", ""),
                "commodity_group": record.get("category", ""),
            })
        consolidated = {"entities": dashboard_entities, "stats": {"total": len(dashboard_entities)}}
        await dash_gen.generate(dashboard_entities, consolidated)

        # 9. Copy dashboard to docs/ for GitHub Pages
        DOCS_DIR.mkdir(parents=True, exist_ok=True)
        src_html = OUTPUT_DIR / "dashboard.html"
        if src_html.exists():
            import shutil
            shutil.copy2(src_html, DOCS_DIR / "index.html")
            logger.info("Dashboard copied to docs/index.html")

        src_json = OUTPUT_DIR / "dashboard.json"
        if src_json.exists():
            import shutil
            shutil.copy2(src_json, DOCS_DIR / "dashboard.json")
            logger.info("Dashboard JSON copied to docs/")

        # 10. Copy entities to docs for frontend data
        entities_file = OUTPUT_DIR / "all_entities.json"
        if entities_file.exists():
            import shutil
            shutil.copy2(entities_file, DOCS_DIR / "data.json")
            logger.info("Entity data copied to docs/data.json")

        cycle_duration = time.time() - cycle_start
        summary = {
            "cycle_duration_seconds": round(cycle_duration, 2),
            "raw_entities_collected": len(all_raw_entities),
            "pipeline_records_in": pipeline_result.records_in,
            "pipeline_records_out": pipeline_result.records_out,
            "graph_nodes": self.graph.graph.number_of_nodes(),
            "graph_edges": self.graph.graph.number_of_edges(),
            "crawler_stats": crawler_stats,
            "health_score": self.orchestrator.resource_monitor.get_health_score(snap),
        }
        logger.info(f"=== Collection cycle complete in {cycle_duration:.1f}s ===")
        return summary

    async def shutdown(self):
        await self.orchestrator.shutdown()
        await self.crawler_manager.stop_all()
        logger.info("Integration orchestrator shut down")
