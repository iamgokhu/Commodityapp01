import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from commodity_agents.config.config import load_config
from commodity_agents.coordinator.coordinator import AgentCoordinator
from commodity_agents.storage.storage import SQLiteStorage, JSONStorage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/commodity_agents.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


async def main():
    config = load_config("commodity_agents/config/config.yaml")
    
    Path("data").mkdir(exist_ok=True)
    Path("output").mkdir(exist_ok=True)
    Path("logs").mkdir(exist_ok=True)
    
    sqlite_storage = SQLiteStorage(config["storage"]["path"])
    json_storage = JSONStorage(config["consolidation"]["output_path"].split("/")[0])
    
    coordinator = AgentCoordinator(num_agents=config["project"]["agents_count"])
    coordinator.create_agents()
    
    # Limit to 3 states for testing
    coordinator.indian_states = ["Maharashtra", "Uttar Pradesh", "Punjab"]
    
    tasks = coordinator.generate_tasks()
    coordinator.assign_tasks(tasks)
    
    entities = await coordinator.run_collection()
    
    coordinator.deduplicate_entities()
    consolidated = coordinator.consolidate_data()
    
    sqlite_storage.save_entities(entities)
    for task in coordinator.all_tasks:
        sqlite_storage.save_task(task)
    
    json_storage.save_consolidated(consolidated, "commodity_data_consolidated.json")
    json_storage.save_entities_json(entities, "all_entities.json")
    
    stats = coordinator.get_stats()
    logger.info("=== COLLECTION COMPLETE ===")
    logger.info(f"Total Entities: {stats['total_entities']}")
    logger.info(f"By Entity Type: {stats['by_entity_type']}")
    logger.info(f"By Product Category: {stats['by_product_category']}")
    logger.info(f"By State (top 10): {dict(list(stats['by_state'].items())[:10])}")
    logger.info(f"By Source: {stats['by_source']}")
    logger.info(f"Consolidated Groups: {len(consolidated)}")
    
    await coordinator.shutdown()
    
    return coordinator


if __name__ == "__main__":
    asyncio.run(main())