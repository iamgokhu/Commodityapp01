import asyncio
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Optional, Dict, Any
from uuid import uuid4

from commodity_agents.models.models import (
    AgentConfig, CollectionTask, Entity, Geography, EntityType,
    ProductCategory, ContactDetails, PriceInfo, PaymentTerms,
    DeliveryAvailability, SupportService
)

logger = logging.getLogger(__name__)


class BaseAgent(ABC):
    def __init__(self, config: AgentConfig):
        self.config = config
        self.task_queue: asyncio.Queue = asyncio.Queue()
        self.results: List[Entity] = []
        self.is_running = False
        self.rate_limiter = asyncio.Semaphore(config.rate_limit_per_minute)
        self.last_request_time = 0

    @abstractmethod
    async def collect_data(self, task: CollectionTask) -> List[Entity]:
        pass

    @abstractmethod
    async def get_data_sources(self, task: CollectionTask) -> List[str]:
        pass

    async def rate_limit(self):
        async with self.rate_limiter:
            now = asyncio.get_event_loop().time()
            time_since_last = now - self.last_request_time
            min_interval = 60 / self.config.rate_limit_per_minute
            if time_since_last < min_interval:
                await asyncio.sleep(min_interval - time_since_last)
            self.last_request_time = asyncio.get_event_loop().time()

    async def execute_task(self, task: CollectionTask) -> List[Entity]:
        task.status = "in_progress"
        logger.info(f"Agent {self.config.agent_id} starting task {task.id}")

        try:
            sources = await self.get_data_sources(task)
            task.data_sources = sources

            entities = await self.collect_data(task)
            
            for entity in entities:
                entity.id = str(uuid4())
                entity.collected_at = datetime.utcnow()
                entity.data_sources = sources

            task.entities_collected = len(entities)
            task.status = "completed"
            task.completed_at = datetime.utcnow()

            logger.info(f"Agent {self.config.agent_id} completed task {task.id} with {len(entities)} entities")
            return entities

        except Exception as e:
            task.status = "failed"
            task.error = str(e)
            task.completed_at = datetime.utcnow()
            logger.error(f"Agent {self.config.agent_id} failed task {task.id}: {e}")
            return []

    async def run(self):
        self.is_running = True
        logger.info(f"Agent {self.config.agent_id} started")

        while self.is_running:
            try:
                task = await asyncio.wait_for(self.task_queue.get(), timeout=1.0)
                entities = await self.execute_task(task)
                self.results.extend(entities)
                self.task_queue.task_done()
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Agent {self.config.agent_id} error: {e}")

    def stop(self):
        self.is_running = False

    def add_task(self, task: CollectionTask):
        self.task_queue.put_nowait(task)

    def get_results(self) -> List[Entity]:
        return self.results.copy()

    def clear_results(self):
        self.results.clear()


class DataSource(ABC):
    @abstractmethod
    async def search(self, query: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    async def extract_entity(self, raw_data: Dict[str, Any]) -> Optional[Entity]:
        pass


class MockDataSource(DataSource):
    async def search(self, query: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        await asyncio.sleep(0.1)
        return []

    async def extract_entity(self, raw_data: Dict[str, Any]) -> Optional[Entity]:
        return None