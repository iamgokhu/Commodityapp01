import asyncio
import logging
import random
from datetime import datetime
from typing import List, Dict, Any
from pathlib import Path

import yaml

from commodity_agents.models.models import (
    AgentConfig, CollectionTask, Entity, Geography, EntityType,
    ProductCategory, GeographyLevel
)
from commodity_agents.agents.base_agent import BaseAgent
from commodity_agents.agents.india_agents import (
    IndiaMARTSource, TradeIndiaSource, ExportDirectorySource, APMCMarketSource
)
from commodity_agents.storage.storage import SQLiteStorage, JSONStorage
from commodity_agents.consolidation.consolidator import DataConsolidator

logger = logging.getLogger(__name__)


class AgentFactory:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.data_sources = {
            "indiamart": IndiaMARTSource(),
            "tradeindia": TradeIndiaSource(),
            "export_directory": ExportDirectorySource(),
            "apmc": APMCMarketSource()
        }

    def create_regional_agents(self, states: List[str]) -> List[BaseAgent]:
        agents = []
        for i, state in enumerate(states[:16]):
            agent_id = f"regional_agent_{i+1}_{state.lower().replace(' ', '_')}"
            
            geography = Geography(state=state)
            
            config = AgentConfig(
                agent_id=agent_id,
                agent_type="regional",
                assigned_regions=[geography],
                assigned_products=list(ProductCategory),
                assigned_entity_types=list(EntityType),
                data_sources=list(self.data_sources.keys()),
                rate_limit_per_minute=self.config["rate_limiting"]["requests_per_minute"],
                max_retries=self.config["rate_limiting"]["retry_attempts"]
            )
            
            agent = RegionalAgent(config, self.data_sources)
            agents.append(agent)
        
        return agents

    def create_product_agents(self) -> List[BaseAgent]:
        agents = []
        products = list(ProductCategory)
        
        for i, product in enumerate(products[:8]):
            agent_id = f"product_agent_{i+1}_{product.value.lower().replace(' ', '_')}"
            
            config = AgentConfig(
                agent_id=agent_id,
                agent_type="product_specialist",
                assigned_regions=[],
                assigned_products=[product],
                assigned_entity_types=list(EntityType),
                data_sources=list(self.data_sources.keys()),
                rate_limit_per_minute=self.config["rate_limiting"]["requests_per_minute"],
                max_retries=self.config["rate_limiting"]["retry_attempts"]
            )
            
            agent = ProductAgent(config, self.data_sources)
            agents.append(agent)
        
        return agents

    def create_entity_agents(self) -> List[BaseAgent]:
        agents = []
        entity_types = list(EntityType)
        
        for i, entity_type in enumerate(entity_types[:8]):
            agent_id = f"entity_agent_{i+1}_{entity_type.value.lower()}"
            
            config = AgentConfig(
                agent_id=agent_id,
                agent_type="entity_specialist",
                assigned_regions=[],
                assigned_products=list(ProductCategory),
                assigned_entity_types=[entity_type],
                data_sources=list(self.data_sources.keys()),
                rate_limit_per_minute=self.config["rate_limiting"]["requests_per_minute"],
                max_retries=self.config["rate_limiting"]["retry_attempts"]
            )
            
            agent = EntityTypeAgent(config, self.data_sources)
            agents.append(agent)
        
        return agents


class RegionalAgent(BaseAgent):
    def __init__(self, config: AgentConfig, data_sources: Dict[str, Any]):
        super().__init__(config)
        self.data_sources = data_sources

    async def get_data_sources(self, task: CollectionTask) -> List[str]:
        return self.config.data_sources

    async def collect_data(self, task: CollectionTask) -> List[Entity]:
        entities = []
        
        for source_name in self.config.data_sources:
            source = self.data_sources.get(source_name)
            if not source:
                continue
            
            try:
                filters = {
                    "state": task.geography.state,
                    "district": task.geography.district,
                    "taluk": task.geography.taluk,
                    "product": task.product_category.value
                }
                
                raw_results = await source.search(
                    f"{task.product_category.value} {task.entity_type.value} in {task.geography.state}",
                    filters
                )
                
                for raw_data in raw_results:
                    entity = await source.extract_entity(raw_data)
                    if entity:
                        entity.entity_type = task.entity_type
                        entity.product_categories = [task.product_category]
                        entities.append(entity)
                        
            except Exception as e:
                logger.error(f"Error collecting from {source_name}: {e}")
        
        return entities


class ProductAgent(BaseAgent):
    def __init__(self, config: AgentConfig, data_sources: Dict[str, Any]):
        super().__init__(config)
        self.data_sources = data_sources

    async def get_data_sources(self, task: CollectionTask) -> List[str]:
        return self.config.data_sources

    async def collect_data(self, task: CollectionTask) -> List[Entity]:
        entities = []
        states = ["Maharashtra", "Uttar Pradesh", "Punjab", "Haryana", "Madhya Pradesh", 
                  "Rajasthan", "Gujarat", "Karnataka", "Tamil Nadu", "Andhra Pradesh",
                  "Telangana", "West Bengal", "Bihar", "Odisha", "Chhattisgarh", "Jharkhand"]
        
        for source_name in self.config.data_sources:
            source = self.data_sources.get(source_name)
            if not source:
                continue
            
            for state in states[:8]:
                try:
                    filters = {
                        "state": state,
                        "product": task.product_category.value
                    }
                    
                    raw_results = await source.search(
                        f"{task.product_category.value} suppliers in {state}",
                        filters
                    )
                    
                    for raw_data in raw_results:
                        entity = await source.extract_entity(raw_data)
                        if entity:
                            entity.product_categories = [task.product_category]
                            entities.append(entity)
                            
                except Exception as e:
                    logger.error(f"Error in product agent {source_name} for {state}: {e}")
        
        return entities


class EntityTypeAgent(BaseAgent):
    def __init__(self, config: AgentConfig, data_sources: Dict[str, Any]):
        super().__init__(config)
        self.data_sources = data_sources

    async def get_data_sources(self, task: CollectionTask) -> List[str]:
        return self.config.data_sources

    async def collect_data(self, task: CollectionTask) -> List[Entity]:
        entities = []
        states = ["Maharashtra", "Uttar Pradesh", "Punjab", "Haryana", "Madhya Pradesh", 
                  "Rajasthan", "Gujarat", "Karnataka"]
        
        for source_name in self.config.data_sources:
            source = self.data_sources.get(source_name)
            if not source:
                continue
            
            for state in states:
                try:
                    filters = {
                        "state": state,
                        "entity_type": task.entity_type.value
                    }
                    
                    raw_results = await source.search(
                        f"{task.entity_type.value} in {state}",
                        filters
                    )
                    
                    for raw_data in raw_results:
                        entity = await source.extract_entity(raw_data)
                        if entity:
                            entity.entity_type = task.entity_type
                            entities.append(entity)
                            
                except Exception as e:
                    logger.error(f"Error in entity agent {source_name} for {state}: {e}")
        
        return entities


class Orchestrator:
    def __init__(self, config_path: str = "config/config.yaml"):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
        
        self.factory = AgentFactory(self.config)
        self.agents: List[BaseAgent] = []
        self.storage = SQLiteStorage(self.config["storage"]["path"])
        self.json_storage = JSONStorage("output")
        self.consolidator = DataConsolidator()
        self.tasks: List[CollectionTask] = []
        self.all_entities: List[Entity] = []

    def create_agents(self):
        indian_states = [
            "Maharashtra", "Uttar Pradesh", "Punjab", "Haryana", "Madhya Pradesh",
            "Rajasthan", "Gujarat", "Karnataka", "Tamil Nadu", "Andhra Pradesh",
            "Telangana", "West Bengal", "Bihar", "Odisha", "Chhattisgarh", "Jharkhand"
        ]
        
        self.agents.extend(self.factory.create_regional_agents(indian_states))
        self.agents.extend(self.factory.create_product_agents())
        self.agents.extend(self.factory.create_entity_agents())
        
        logger.info(f"Created {len(self.agents)} agents")

    def create_tasks(self):
        indian_states = [
            "Maharashtra", "Uttar Pradesh", "Punjab", "Haryana", "Madhya Pradesh",
            "Rajasthan", "Gujarat", "Karnataka", "Tamil Nadu", "Andhra Pradesh",
            "Telangana", "West Bengal", "Bihar", "Odisha", "Chhattisgarh", "Jharkhand"
        ]
        
        task_id = 0
        for agent in self.agents:
            if agent.config.agent_type == "regional":
                for product in agent.config.assigned_products:
                    for entity_type in agent.config.assigned_entity_types:
                        for region in agent.config.assigned_regions:
                            task = CollectionTask(
                                agent_id=agent.config.agent_id,
                                product_category=product,
                                geography=region,
                                entity_type=entity_type
                            )
                            self.tasks.append(task)
                            task_id += 1
            
            elif agent.config.agent_type == "product_specialist":
                product = agent.config.assigned_products[0]
                for entity_type in agent.config.assigned_entity_types:
                    for state in indian_states[:8]:
                        geography = Geography(state=state)
                        task = CollectionTask(
                            agent_id=agent.config.agent_id,
                            product_category=product,
                            geography=geography,
                            entity_type=entity_type
                        )
                        self.tasks.append(task)
                        task_id += 1
            
            elif agent.config.agent_type == "entity_specialist":
                entity_type = agent.config.assigned_entity_types[0]
                for product in agent.config.assigned_products:
                    for state in indian_states[:8]:
                        geography = Geography(state=state)
                        task = CollectionTask(
                            agent_id=agent.config.agent_id,
                            product_category=product,
                            geography=geography,
                            entity_type=entity_type
                        )
                        self.tasks.append(task)
                        task_id += 1
        
        logger.info(f"Created {len(self.tasks)} collection tasks")

    def assign_tasks_to_agents(self):
        agent_tasks: Dict[str, List[CollectionTask]] = {}
        
        for task in self.tasks:
            if task.agent_id not in agent_tasks:
                agent_tasks[task.agent_id] = []
            agent_tasks[task.agent_id].append(task)
        
        for agent in self.agents:
            if agent.config.agent_id in agent_tasks:
                for task in agent_tasks[agent.config.agent_id]:
                    agent.add_task(task)

    async def run_collection(self):
        logger.info("Starting data collection with 32 agents...")
        
        agent_tasks = [agent.run() for agent in self.agents]
        
        await asyncio.gather(*agent_tasks, return_exceptions=True)
        
        for agent in self.agents:
            self.all_entities.extend(agent.get_results())
        
        logger.info(f"Collection complete. Total entities: {len(self.all_entities)}")

    def save_results(self):
        logger.info("Saving results to database...")
        self.storage.save_entities(self.all_entities)
        
        for agent in self.agents:
            for task in agent.tasks:
                self.storage.save_task(task)
        
        self.json_storage.save_entities_json(self.all_entities)
        
        consolidated = self.consolidator.consolidate(self.all_entities)
        self.json_storage.save_consolidated(consolidated)
        
        stats = self.storage.get_stats()
        logger.info(f"Storage stats: {stats}")

    async def run(self):
        self.create_agents()
        self.create_tasks()
        self.assign_tasks_to_agents()
        await self.run_collection()
        self.save_results()
        logger.info("Orchestration complete!")