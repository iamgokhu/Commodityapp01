from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum
import uuid


class ProductCategory(str, Enum):
    SUGAR = "Sugar"
    RICE = "Rice"
    GRAINS = "Grains"
    PULSES = "Pulses"
    WHEAT = "Wheat"
    DALS = "Dals"
    BASMATHI_RICE = "Basmathi Rice"


class EntityType(str, Enum):
    MANUFACTURER = "Manufacturer"
    WHOLESALER = "Wholesaler"
    EXPORTER = "Exporter"


class GeographyLevel(str, Enum):
    TALUK = "Taluk"
    DISTRICT = "District"
    STATE = "State"


class PaymentTerms(str, Enum):
    CASH = "Cash"
    CREDIT_30 = "Credit 30 Days"
    CREDIT_60 = "Credit 60 Days"
    CREDIT_90 = "Credit 90 Days"
    ADVANCE = "Advance Payment"
    LC = "Letter of Credit"


class DeliveryAvailability(str, Enum):
    YES = "Yes"
    NO = "No"
    CONDITIONAL = "Conditional"


class SupportService(str, Enum):
    TECHNICAL = "Technical Support"
    LOGISTICS = "Logistics Support"
    QUALITY_ASSURANCE = "Quality Assurance"
    FINANCING = "Financing Options"
    MARKETING = "Marketing Support"


class Geography(BaseModel):
    taluk: Optional[str] = None
    district: Optional[str] = None
    state: str


class ContactDetails(BaseModel):
    name: Optional[str] = None
    phone: Optional[str] = None
    mobile: Optional[str] = None
    email: Optional[str] = None
    website: Optional[str] = None


class PriceInfo(BaseModel):
    sku: str
    market_price_today: Optional[float] = None
    purchase_price: Optional[float] = None
    market_selling_price: Optional[float] = None
    unit: str = "KG"
    currency: str = "INR"
    last_updated: datetime = Field(default_factory=datetime.utcnow)


class Entity(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    entity_type: EntityType
    name: str
    geography: Geography
    contact_details: ContactDetails
    year_established: Optional[int] = None
    gst_number: Optional[str] = None
    office_address: Optional[str] = None
    product_categories: List[ProductCategory] = []
    prices: List[PriceInfo] = []
    payment_terms: Optional[PaymentTerms] = None
    support_services: List[SupportService] = []
    delivery_available: DeliveryAvailability = DeliveryAvailability.NO
    data_sources: List[str] = []
    collected_at: datetime = Field(default_factory=datetime.utcnow)
    source_urls: List[str] = []
    confidence_score: float = 0.0


class ConsolidatedProductData(BaseModel):
    product_category: ProductCategory
    state: str
    district: Optional[str] = None
    taluk: Optional[str] = None
    entities: List[Entity] = []
    collected_at: datetime = Field(default_factory=datetime.utcnow)
    total_entities: int = 0
    sources_used: List[str] = []


class CollectionTask(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    agent_id: str
    product_category: ProductCategory
    geography: Geography
    entity_type: EntityType
    data_sources: List[str] = []
    status: str = "pending"
    created_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    error: Optional[str] = None
    entities_collected: int = 0


class AgentConfig(BaseModel):
    agent_id: str
    agent_type: str
    assigned_regions: List[Geography] = []
    assigned_products: List[ProductCategory] = []
    assigned_entity_types: List[EntityType] = []
    data_sources: List[str] = []
    rate_limit_per_minute: int = 10
    max_retries: int = 3