import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from uuid import uuid4

from commodity_agents.agents.base_agent import BaseAgent
from commodity_agents.agents.specialized_agents import (
    RegionalAgent, ProductSpecificAgent, EntityTypeAgent, create_agent
)
from commodity_agents.models.models import (
    AgentConfig, CollectionTask, Entity, Geography, EntityType,
    ProductCategory, ConsolidatedProductData
)

logger = logging.getLogger(__name__)


class AgentCoordinator:
    def __init__(self, num_agents: int = 32):
        self.num_agents = num_agents
        self.agents: List[BaseAgent] = []
        self.all_tasks: List[CollectionTask] = []
        self.all_entities: List[Entity] = []
        self.consolidated_data: Dict[str, ConsolidatedProductData] = {}

        self.indian_states = [
            "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chhattisgarh",
            "Goa", "Gujarat", "Haryana", "Himachal Pradesh", "Jharkhand",
            "Karnataka", "Kerala", "Madhya Pradesh", "Maharashtra", "Manipur",
            "Meghalaya", "Mizoram", "Nagaland", "Odisha", "Punjab",
            "Rajasthan", "Sikkim", "Tamil Nadu", "Telangana", "Tripura",
            "Uttar Pradesh", "Uttarakhand", "West Bengal"
        ]

        self.product_categories = list(ProductCategory)
        self.entity_types = list(EntityType)

    def create_agents(self):
        agent_configs = self._generate_agent_configs()
        
        for i, config in enumerate(agent_configs):
            if i < 16:
                agent_type = "regional"
            elif i < 24:
                agent_type = "product"
            else:
                agent_type = "entity_type"
            
            agent = create_agent(agent_type, config)
            self.agents.append(agent)
            logger.info(f"Created {agent_type} agent: {config.agent_id}")

        logger.info(f"Created {len(self.agents)} agents total")

    def _generate_agent_configs(self) -> List[AgentConfig]:
        configs = []
        
        for i in range(16):
            state = self.indian_states[i]
            config = AgentConfig(
                agent_id=f"regional_agent_{i:02d}_{state.replace(' ', '_').lower()}",
                agent_type="regional",
                assigned_regions=[Geography(state=state)],
                assigned_products=self.product_categories,
                assigned_entity_types=self.entity_types,
                data_sources=["IndiaMART", "TradeIndia", "ExportDirectory", "APMC"],
                rate_limit_per_minute=15,
                max_retries=3
            )
            configs.append(config)

        for i, product in enumerate(self.product_categories):
            config = AgentConfig(
                agent_id=f"product_agent_{i:02d}_{product.value.lower().replace(' ', '_')}",
                agent_type="product",
                assigned_regions=[Geography(state=s) for s in self.indian_states],
                assigned_products=[product],
                assigned_entity_types=self.entity_types,
                data_sources=["IndiaMART", "TradeIndia", "APMC"],
                rate_limit_per_minute=12,
                max_retries=3
            )
            configs.append(config)

        for i, entity_type in enumerate(self.entity_types):
            config = AgentConfig(
                agent_id=f"entity_agent_{i:02d}_{entity_type.value.lower()}",
                agent_type="entity_type",
                assigned_regions=[Geography(state=s) for s in self.indian_states],
                assigned_products=self.product_categories,
                assigned_entity_types=[entity_type],
                data_sources=["IndiaMART", "TradeIndia", "ExportDirectory", "APMC"],
                rate_limit_per_minute=10,
                max_retries=3
            )
            configs.append(config)

        return configs

    def generate_tasks(self) -> List[CollectionTask]:
        tasks = []
        
        for state in self.indian_states:
            for product in self.product_categories:
                for entity_type in self.entity_types:
                    task = CollectionTask(
                        agent_id="",
                        product_category=product,
                        geography=Geography(state=state),
                        entity_type=entity_type,
                        data_sources=[]
                    )
                    tasks.append(task)

        logger.info(f"Generated {len(tasks)} collection tasks")
        return tasks

    def assign_tasks(self, tasks: List[CollectionTask]):
        regional_agents = [a for a in self.agents if a.config.agent_type == "regional"]
        product_agents = [a for a in self.agents if a.config.agent_type == "product"]
        entity_agents = [a for a in self.agents if a.config.agent_type == "entity_type"]

        for task in tasks:
            state = task.geography.state
            product = task.product_category
            entity_type = task.entity_type

            regional_agent = next(
                (a for a in regional_agents if any(r.state == state for r in a.config.assigned_regions)),
                regional_agents[0]
            )
            product_agent = next(
                (a for a in product_agents if product in a.config.assigned_products),
                product_agents[0]
            )
            entity_agent = next(
                (a for a in entity_agents if entity_type in a.config.assigned_entity_types),
                entity_agents[0]
            )

            assigned_agent = [regional_agent, product_agent, entity_agent][
                hash(f"{state}{product}{entity_type}") % 3
            ]
            
            task.agent_id = assigned_agent.config.agent_id
            assigned_agent.add_task(task)
            self.all_tasks.append(task)

    async def run_collection(self) -> List[Entity]:
        logger.info("Starting data collection with all agents...")
        
        agent_tasks = [asyncio.create_task(agent.run()) for agent in self.agents]
        
        # Wait for all task queues to be empty
        for agent in self.agents:
            await agent.task_queue.join()
        
        # Stop all agents
        for agent in self.agents:
            agent.stop()
        
        # Wait for agent tasks to finish
        await asyncio.gather(*agent_tasks, return_exceptions=True)
        
        for agent in self.agents:
            self.all_entities.extend(agent.get_results())
            agent.clear_results()

        logger.info(f"Collection complete. Total entities collected: {len(self.all_entities)}")
        return self.all_entities

    def consolidate_data(self) -> Dict[str, ConsolidatedProductData]:
        self.consolidated_data = {}
        
        for entity in self.all_entities:
            for product in entity.product_categories:
                key = f"{product.value}_{entity.geography.state}"
                if entity.geography.district:
                    key += f"_{entity.geography.district}"
                
                if key not in self.consolidated_data:
                    self.consolidated_data[key] = ConsolidatedProductData(
                        product_category=product,
                        state=entity.geography.state,
                        district=entity.geography.district,
                        taluk=entity.geography.taluk
                    )
                
                self.consolidated_data[key].entities.append(entity)
                self.consolidated_data[key].total_entities = len(self.consolidated_data[key].entities)
                
                for source in entity.data_sources:
                    if source not in self.consolidated_data[key].sources_used:
                        self.consolidated_data[key].sources_used.append(source)

        logger.info(f"Consolidated into {len(self.consolidated_data)} product-region groups")
        return self.consolidated_data

    def deduplicate_entities(self) -> List[Entity]:
        seen = {}
        deduplicated = []
        
        for entity in self.all_entities:
            key = (
                entity.name.lower().strip(),
                entity.geography.state,
                entity.geography.district or "",
                entity.entity_type.value
            )
            
            if key not in seen:
                seen[key] = entity
                deduplicated.append(entity)
            else:
                existing = seen[key]
                if entity.confidence_score > existing.confidence_score:
                    seen[key] = entity
                    deduplicated[deduplicated.index(existing)] = entity
                elif entity.confidence_score == existing.confidence_score:
                    if len(entity.data_sources) > len(existing.data_sources):
                        seen[key] = entity
                        deduplicated[deduplicated.index(existing)] = entity

        logger.info(f"Deduplicated {len(self.all_entities)} -> {len(deduplicated)} entities")
        self.all_entities = deduplicated
        return deduplicated

    def get_stats(self) -> Dict[str, Any]:
        stats = {
            "total_entities": len(self.all_entities),
            "by_entity_type": {},
            "by_product_category": {},
            "by_state": {},
            "by_source": {},
            "tasks_completed": len([t for t in self.all_tasks if t.status == "completed"]),
            "tasks_failed": len([t for t in self.all_tasks if t.status == "failed"]),
        }

        for entity in self.all_entities:
            stats["by_entity_type"][entity.entity_type.value] = \
                stats["by_entity_type"].get(entity.entity_type.value, 0) + 1
            
            for product in entity.product_categories:
                stats["by_product_category"][product.value] = \
                    stats["by_product_category"].get(product.value, 0) + 1
            
            stats["by_state"][entity.geography.state] = \
                stats["by_state"].get(entity.geography.state, 0) + 1
            
            for source in entity.data_sources:
                stats["by_source"][source] = stats["by_source"].get(source, 0) + 1

        return stats

    async def shutdown(self):
        for agent in self.agents:
            agent.stop()
        logger.info("All agents stopped")


async def main():
    logging.basicConfig(level=logging.INFO)
    
    coordinator = AgentCoordinator(num_agents=32)
    coordinator.create_agents()
    
    tasks = coordinator.generate_tasks()
    coordinator.assign_tasks(tasks)
    
    entities = await coordinator.run_collection()
    
    coordinator.deduplicate_entities()
    consolidated = coordinator.consolidate_data()
    
    stats = coordinator.get_stats()
    print(f"\n=== Collection Statistics ===")
    print(f"Total Entities: {stats['total_entities']}")
    print(f"Tasks Completed: {stats['tasks_completed']}")
    print(f"Tasks Failed: {stats['tasks_failed']}")
    print(f"\nBy Entity Type: {stats['by_entity_type']}")
    print(f"By Product: {stats['by_product_category']}")
    print(f"By State (top 10): {dict(list(stats['by_state'].items())[:10])}")
    print(f"By Source: {stats['by_source']}")
    print(f"\nConsolidated Groups: {len(consolidated)}")
    
    await coordinator.shutdown()
    
    return coordinator


if __name__ == "__main__":
    asyncio.run(main())