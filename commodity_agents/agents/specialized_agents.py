import asyncio
import logging
import random
from datetime import datetime
from typing import List, Dict, Any, Optional
from uuid import uuid4

from commodity_agents.agents.base_agent import BaseAgent, DataSource, MockDataSource
from commodity_agents.models.models import (
    AgentConfig, CollectionTask, Entity, EntityType, ProductCategory,
    Geography, ContactDetails, PriceInfo, PaymentTerms, DeliveryAvailability, SupportService
)

logger = logging.getLogger(__name__)


class IndiaMARTSource(DataSource):
    async def search(self, query: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        await asyncio.sleep(0.2)
        return [
            {
                "name": f"{filters.get('product', 'Commodity')} Supplier",
                "phone": f"+91-{random.randint(7000000000, 9999999999)}",
                "email": f"sales@supplier{random.randint(100, 999)}.com",
                "address": f"Industrial Area, {filters.get('district', 'District')}, {filters.get('state', 'State')}",
                "gst": f"{random.randint(10, 99)}AAAAA{random.randint(1000, 9999)}Z{random.randint(1, 9)}",
                "established": random.randint(1990, 2020),
                "website": f"https://supplier{random.randint(100, 999)}.com"
            }
            for _ in range(random.randint(3, 8))
        ]

    async def extract_entity(self, raw_data: Dict[str, Any]) -> Optional[Entity]:
        return Entity(
            entity_type=EntityType.MANUFACTURER,
            name=raw_data.get("name", "Unknown Supplier"),
            geography=Geography(
                district=raw_data.get("district", ""),
                state=raw_data.get("state", "")
            ),
            contact_details=ContactDetails(
                phone=raw_data.get("phone"),
                email=raw_data.get("email"),
                website=raw_data.get("website")
            ),
            year_established=raw_data.get("established"),
            gst_number=raw_data.get("gst"),
            office_address=raw_data.get("address"),
            prices=[
                PriceInfo(
                    sku=raw_data.get("product", "Commodity"),
                    market_price_today=round(random.uniform(20, 80), 2),
                    purchase_price=round(random.uniform(18, 75), 2),
                    market_selling_price=round(random.uniform(22, 85), 2)
                )
            ],
            payment_terms=random.choice(list(PaymentTerms)),
            support_services=random.sample(list(SupportService), k=random.randint(1, 3)),
            delivery_available=random.choice(list(DeliveryAvailability)),
            source_urls=[f"https://indiamart.com/supplier/{uuid4().hex[:8]}"]
        )


class TradeIndiaSource(DataSource):
    async def search(self, query: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        await asyncio.sleep(0.15)
        return [
            {
                "company_name": f"{filters.get('product', 'Agri')} Traders",
                "contact_person": f"Mr. {random.choice(['Sharma', 'Patel', 'Singh', 'Kumar', 'Reddy'])}",
                "mobile": f"+91-{random.randint(7000000000, 9999999999)}",
                "email": f"info@trader{random.randint(100, 999)}.in",
                "location": f"{filters.get('taluk', 'Taluk')}, {filters.get('district', 'District')}",
                "state": filters.get("state", "State"),
                "gstin": f"{random.randint(10, 99)}BBBBB{random.randint(1000, 9999)}Z{random.randint(1, 9)}",
                "year": random.randint(1995, 2022)
            }
            for _ in range(random.randint(2, 6))
        ]

    async def extract_entity(self, raw_data: Dict[str, Any]) -> Optional[Entity]:
        return Entity(
            entity_type=EntityType.WHOLESALER,
            name=raw_data.get("company_name", "Unknown Trader"),
            geography=Geography(
                taluk=raw_data.get("location", "").split(",")[0].strip(),
                district=raw_data.get("location", "").split(",")[1].strip() if "," in raw_data.get("location", "") else "",
                state=raw_data.get("state", "")
            ),
            contact_details=ContactDetails(
                name=raw_data.get("contact_person"),
                mobile=raw_data.get("mobile"),
                email=raw_data.get("email")
            ),
            year_established=raw_data.get("year"),
            gst_number=raw_data.get("gstin"),
            prices=[
                PriceInfo(
                    sku="Generic",
                    market_price_today=round(random.uniform(15, 60), 2),
                    purchase_price=round(random.uniform(13, 55), 2),
                    market_selling_price=round(random.uniform(18, 65), 2)
                )
            ],
            payment_terms=random.choice(list(PaymentTerms)),
            support_services=random.sample(list(SupportService), k=random.randint(1, 2)),
            delivery_available=random.choice(list(DeliveryAvailability)),
            source_urls=[f"https://tradeindia.com/company/{uuid4().hex[:8]}"]
        )


class ExportDirectorySource(DataSource):
    async def search(self, query: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        await asyncio.sleep(0.25)
        return [
            {
                "exporter_name": f"Global {filters.get('product', 'Food')} Exports",
                "director": f"Mrs. {random.choice(['Gupta', 'Agarwal', 'Jain', 'Shah', 'Mehta'])}",
                "phone": f"+91-{random.randint(7000000000, 9999999999)}",
                "email": f"exports@global{random.randint(100, 999)}.com",
                "regd_office": f"Export House, {filters.get('district', 'District')}, {filters.get('state', 'State')}",
                "iec_code": f"{random.randint(10, 99)}{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}{random.randint(1000, 9999)}",
                "established": random.randint(1985, 2015),
                "markets": ["UAE", "USA", "EU", "SE Asia"],
                "certifications": ["ISO 22000", "HACCP", "FSSAI"]
            }
            for _ in range(random.randint(1, 4))
        ]

    async def extract_entity(self, raw_data: Dict[str, Any]) -> Optional[Entity]:
        return Entity(
            entity_type=EntityType.EXPORTER,
            name=raw_data.get("exporter_name", "Unknown Exporter"),
            geography=Geography(
                district=raw_data.get("regd_office", "").split(",")[1].strip() if "," in raw_data.get("regd_office", "") else "",
                state=raw_data.get("regd_office", "").split(",")[-1].strip() if "," in raw_data.get("regd_office", "") else ""
            ),
            contact_details=ContactDetails(
                name=raw_data.get("director"),
                phone=raw_data.get("phone"),
                email=raw_data.get("email")
            ),
            year_established=raw_data.get("established"),
            office_address=raw_data.get("regd_office"),
            prices=[
                PriceInfo(
                    sku="Export Grade",
                    market_price_today=round(random.uniform(30, 120), 2),
                    purchase_price=round(random.uniform(25, 100), 2),
                    market_selling_price=round(random.uniform(35, 140), 2)
                )
            ],
            payment_terms=PaymentTerms.LC,
            support_services=[SupportService.QUALITY_ASSURANCE, SupportService.LOGISTICS],
            delivery_available=DeliveryAvailability.YES,
            source_urls=[f"https://exportdirectory.gov.in/exporter/{uuid4().hex[:8]}"]
        )


class APMCMarketSource(DataSource):
    async def search(self, query: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        await asyncio.sleep(0.1)
        return [
            {
                "trader_name": f"APMC {filters.get('product', 'Grain')} Trader {i}",
                "license_no": f"APMC{random.randint(10000, 99999)}",
                "shop_no": f"Shop-{random.randint(1, 200)}",
                "market": f"{filters.get('district', 'District')} APMC Yard",
                "phone": f"+91-{random.randint(7000000000, 9999999999)}",
                "commodity": filters.get('product', 'Wheat'),
                "price_per_quintal": round(random.uniform(1500, 4000), 2),
                "arrival_date": datetime.now().strftime("%Y-%m-%d")
            }
            for i in range(random.randint(5, 15))
        ]

    async def extract_entity(self, raw_data: Dict[str, Any]) -> Optional[Entity]:
        return Entity(
            entity_type=EntityType.WHOLESALER,
            name=raw_data.get("trader_name", "APMC Trader"),
            geography=Geography(
                district=raw_data.get("market", "").replace(" APMC Yard", ""),
                state=""
            ),
            contact_details=ContactDetails(
                phone=raw_data.get("phone")
            ),
            office_address=f"{raw_data.get('shop_no', '')}, {raw_data.get('market', '')}",
            prices=[
                PriceInfo(
                    sku=raw_data.get("commodity", "Commodity"),
                    market_price_today=raw_data.get("price_per_quintal", 0) / 100,
                    purchase_price=raw_data.get("price_per_quintal", 0) / 100 * 0.95,
                    market_selling_price=raw_data.get("price_per_quintal", 0) / 100 * 1.05,
                    unit="KG"
                )
            ],
            payment_terms=PaymentTerms.CASH,
            delivery_available=DeliveryAvailability.CONDITIONAL,
            source_urls=[f"https://apmc.gov.in/market/{uuid4().hex[:8]}"]
        )


class RegionalAgent(BaseAgent):
    def __init__(self, config: AgentConfig):
        super().__init__(config)
        self.data_sources = [
            IndiaMARTSource(),
            TradeIndiaSource(),
            ExportDirectorySource(),
            APMCMarketSource(),
            MockDataSource()
        ]

    async def get_data_sources(self, task: CollectionTask) -> List[str]:
        return [source.__class__.__name__ for source in self.data_sources]

    async def collect_data(self, task: CollectionTask) -> List[Entity]:
        await self.rate_limit()
        
        all_entities = []
        filters = {
            "product": task.product_category.value,
            "state": task.geography.state,
            "district": task.geography.district,
            "taluk": task.geography.taluk
        }

        for source in self.data_sources:
            try:
                raw_results = await source.search(
                    f"{task.product_category.value} {task.entity_type.value} {task.geography.state}",
                    filters
                )
                
                for raw in raw_results:
                    entity = await source.extract_entity(raw)
                    if entity:
                        entity.entity_type = task.entity_type
                        entity.geography = task.geography
                        entity.product_categories = [task.product_category]
                        all_entities.append(entity)
                        
            except Exception as e:
                logger.error(f"Source {source.__class__.__name__} failed: {e}")

        return all_entities


class ProductSpecificAgent(BaseAgent):
    def __init__(self, config: AgentConfig):
        super().__init__(config)
        self.data_sources = [
            IndiaMARTSource(),
            TradeIndiaSource(),
            APMCMarketSource(),
            MockDataSource()
        ]

    async def get_data_sources(self, task: CollectionTask) -> List[str]:
        return [source.__class__.__name__ for source in self.data_sources]

    async def collect_data(self, task: CollectionTask) -> List[Entity]:
        await self.rate_limit()
        
        all_entities = []
        filters = {
            "product": task.product_category.value,
            "state": task.geography.state,
            "district": task.geography.district,
            "taluk": task.geography.taluk
        }

        for source in self.data_sources:
            try:
                raw_results = await source.search(
                    f"{task.product_category.value} suppliers manufacturers exporters",
                    filters
                )
                
                for raw in raw_results:
                    entity = await source.extract_entity(raw)
                    if entity:
                        entity.entity_type = task.entity_type
                        entity.geography = task.geography
                        entity.product_categories = [task.product_category]
                        all_entities.append(entity)
                        
            except Exception as e:
                logger.error(f"Product agent source {source.__class__.__name__} failed: {e}")

        return all_entities


class EntityTypeAgent(BaseAgent):
    def __init__(self, config: AgentConfig):
        super().__init__(config)
        if config.assigned_entity_types[0] == EntityType.MANUFACTURER:
            self.data_sources = [IndiaMARTSource(), MockDataSource()]
        elif config.assigned_entity_types[0] == EntityType.WHOLESALER:
            self.data_sources = [TradeIndiaSource(), APMCMarketSource(), MockDataSource()]
        else:
            self.data_sources = [ExportDirectorySource(), MockDataSource()]

    async def get_data_sources(self, task: CollectionTask) -> List[str]:
        return [source.__class__.__name__ for source in self.data_sources]

    async def collect_data(self, task: CollectionTask) -> List[Entity]:
        await self.rate_limit()
        
        all_entities = []
        filters = {
            "product": task.product_category.value,
            "state": task.geography.state,
            "district": task.geography.district,
            "taluk": task.geography.taluk
        }

        for source in self.data_sources:
            try:
                raw_results = await source.search(
                    f"{task.entity_type.value} {task.product_category.value} {task.geography.state}",
                    filters
                )
                
                for raw in raw_results:
                    entity = await source.extract_entity(raw)
                    if entity:
                        entity.entity_type = task.entity_type
                        entity.geography = task.geography
                        entity.product_categories = [task.product_category]
                        all_entities.append(entity)
                        
            except Exception as e:
                logger.error(f"Entity type agent source {source.__class__.__name__} failed: {e}")

        return all_entities


def create_agent(agent_type: str, config: AgentConfig) -> BaseAgent:
    if agent_type == "regional":
        return RegionalAgent(config)
    elif agent_type == "product":
        return ProductSpecificAgent(config)
    elif agent_type == "entity_type":
        return EntityTypeAgent(config)
    else:
        raise ValueError(f"Unknown agent type: {agent_type}")