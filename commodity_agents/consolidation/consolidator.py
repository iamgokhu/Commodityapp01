import logging
from collections import defaultdict
from datetime import datetime
from typing import List, Dict, Any
from uuid import uuid4

from commodity_agents.models.models import (
    Entity, ConsolidatedProductData, ProductCategory, EntityType
)

logger = logging.getLogger(__name__)


class DataConsolidator:
    def __init__(self):
        self.dedup_key_fields = ["gst_number", "name", "office_address", "phone", "mobile"]

    def consolidate(self, entities: List[Entity]) -> Dict[str, ConsolidatedProductData]:
        logger.info(f"Consolidating {len(entities)} entities...")
        
        deduped = self.deduplicate(entities)
        logger.info(f"After deduplication: {len(deduped)} unique entities")
        
        grouped = self.group_by_product_and_geography(deduped)
        
        consolidated = {}
        for key, entity_list in grouped.items():
            product_category, state, district, taluk = key
            
            consolidated[key] = ConsolidatedProductData(
                product_category=product_category,
                state=state,
                district=district,
                taluk=taluk,
                entities=entity_list,
                collected_at=datetime.utcnow(),
                total_entities=len(entity_list),
                sources_used=self._collect_sources(entity_list)
            )
        
        logger.info(f"Created {len(consolidated)} consolidated segments")
        return consolidated

    def deduplicate(self, entities: List[Entity]) -> List[Entity]:
        seen = {}
        deduped = []
        
        for entity in entities:
            dedup_key = self._generate_dedup_key(entity)
            
            if dedup_key in seen:
                existing = seen[dedup_key]
                merged = self._merge_entities(existing, entity)
                seen[dedup_key] = merged
            else:
                seen[dedup_key] = entity
        
        return list(seen.values())

    def _generate_dedup_key(self, entity: Entity) -> str:
        parts = []
        
        if entity.gst_number:
            parts.append(f"gst:{entity.gst_number.lower().strip()}")
        
        if entity.name:
            parts.append(f"name:{entity.name.lower().strip()}")
        
        if entity.office_address:
            parts.append(f"addr:{entity.office_address.lower().strip()}")
        
        if entity.contact_details.mobile:
            parts.append(f"mobile:{entity.contact_details.mobile.strip()}")
        elif entity.contact_details.phone:
            parts.append(f"phone:{entity.contact_details.phone.strip()}")
        
        if entity.geography.state:
            parts.append(f"state:{entity.geography.state.lower().strip()}")
        if entity.geography.district:
            parts.append(f"district:{entity.geography.district.lower().strip()}")
        
        if not parts:
            parts.append(f"id:{entity.id}")
        
        return "|".join(parts)

    def _merge_entities(self, existing: Entity, new: Entity) -> Entity:
        merged = existing
        
        for field in ["year_established", "gst_number", "office_address", "contact_details"]:
            new_val = getattr(new, field)
            if new_val and not getattr(merged, field):
                setattr(merged, field, new_val)
        
        if new.contact_details:
            if new.contact_details.email and not merged.contact_details.email:
                merged.contact_details.email = new.contact_details.email
            if new.contact_details.website and not merged.contact_details.website:
                merged.contact_details.website = new.contact_details.website
        
        existing_skus = {p.sku for p in merged.prices}
        for price in new.prices:
            if price.sku not in existing_skus:
                merged.prices.append(price)
        
        merged.support_services = list(set(merged.support_services + new.support_services))
        merged.data_sources = list(set(merged.data_sources + new.data_sources))
        merged.source_urls = list(set(merged.source_urls + new.source_urls))
        
        merged.confidence_score = max(merged.confidence_score, new.confidence_score)
        
        return merged

    def group_by_product_and_geography(self, entities: List[Entity]) -> Dict[tuple, List[Entity]]:
        grouped = defaultdict(list)
        
        for entity in entities:
            for product in entity.product_categories:
                key = (
                    product,
                    entity.geography.state or "Unknown",
                    entity.geography.district or "Unknown",
                    entity.geography.taluk or "Unknown"
                )
                grouped[key].append(entity)
        
        return grouped

    def _collect_sources(self, entities: List[Entity]) -> List[str]:
        sources = set()
        for entity in entities:
            sources.update(entity.data_sources)
        return list(sources)

    def get_summary_stats(self, consolidated: Dict[str, ConsolidatedProductData]) -> Dict[str, Any]:
        stats = {
            "total_segments": len(consolidated),
            "total_entities": sum(c.total_entities for c in consolidated.values()),
            "by_product_category": defaultdict(int),
            "by_state": defaultdict(int),
            "by_entity_type": defaultdict(int),
            "by_source": defaultdict(int),
            "price_coverage": 0,
            "gst_coverage": 0,
            "contact_coverage": 0
        }
        
        entities_with_price = 0
        entities_with_gst = 0
        entities_with_contact = 0
        
        for product_data in consolidated.values():
            stats["by_product_category"][product_data.product_category.value] += product_data.total_entities
            stats["by_state"][product_data.state] += product_data.total_entities
            
            for entity in product_data.entities:
                stats["by_entity_type"][entity.entity_type.value] += 1
                stats["by_source"].update({s: 1 for s in entity.data_sources})
                
                if entity.prices:
                    entities_with_price += 1
                if entity.gst_number:
                    entities_with_gst += 1
                if entity.contact_details.phone or entity.contact_details.mobile or entity.contact_details.email:
                    entities_with_contact += 1
        
        total = stats["total_entities"]
        stats["price_coverage"] = round(entities_with_price / total * 100, 1) if total > 0 else 0
        stats["gst_coverage"] = round(entities_with_gst / total * 100, 1) if total > 0 else 0
        stats["contact_coverage"] = round(entities_with_contact / total * 100, 1) if total > 0 else 0
        
        return stats