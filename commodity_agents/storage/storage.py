import sqlite3
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path

from commodity_agents.models.models import Entity, ConsolidatedProductData, CollectionTask

logger = logging.getLogger(__name__)


class SQLiteStorage:
    def __init__(self, db_path: str = "data/commodity_data.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS entities (
                    id TEXT PRIMARY KEY,
                    entity_type TEXT NOT NULL,
                    name TEXT NOT NULL,
                    state TEXT NOT NULL,
                    district TEXT,
                    taluk TEXT,
                    contact_name TEXT,
                    phone TEXT,
                    mobile TEXT,
                    email TEXT,
                    website TEXT,
                    year_established INTEGER,
                    gst_number TEXT,
                    office_address TEXT,
                    product_categories TEXT,
                    prices TEXT,
                    payment_terms TEXT,
                    support_services TEXT,
                    delivery_available TEXT,
                    data_sources TEXT,
                    collected_at TEXT,
                    source_urls TEXT,
                    confidence_score REAL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    agent_id TEXT,
                    product_category TEXT,
                    state TEXT,
                    district TEXT,
                    taluk TEXT,
                    entity_type TEXT,
                    data_sources TEXT,
                    status TEXT,
                    created_at TEXT,
                    completed_at TEXT,
                    error TEXT,
                    entities_collected INTEGER
                )
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_entities_state_product_type 
                ON entities(state, product_categories, entity_type)
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_entities_name_location 
                ON entities(name, state, district, entity_type)
            """)
            
            conn.commit()

    def save_entity(self, entity: Entity) -> bool:
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO entities (
                        id, entity_type, name, state, district, taluk,
                        contact_name, phone, mobile, email, website,
                        year_established, gst_number, office_address,
                        product_categories, prices, payment_terms,
                        support_services, delivery_available, data_sources,
                        collected_at, source_urls, confidence_score
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    entity.id,
                    entity.entity_type.value,
                    entity.name,
                    entity.geography.state,
                    entity.geography.district,
                    entity.geography.taluk,
                    entity.contact_details.name,
                    entity.contact_details.phone,
                    entity.contact_details.mobile,
                    entity.contact_details.email,
                    entity.contact_details.website,
                    entity.year_established,
                    entity.gst_number,
                    entity.office_address,
                    json.dumps([p.value for p in entity.product_categories]),
                    json.dumps([{
                        "sku": p.sku,
                        "market_price_today": p.market_price_today,
                        "purchase_price": p.purchase_price,
                        "market_selling_price": p.market_selling_price,
                        "unit": p.unit,
                        "currency": p.currency,
                        "last_updated": p.last_updated.isoformat() if p.last_updated else None
                    } for p in entity.prices]),
                    entity.payment_terms.value if entity.payment_terms else None,
                    json.dumps([s.value for s in entity.support_services]),
                    entity.delivery_available.value,
                    json.dumps(entity.data_sources),
                    entity.collected_at.isoformat(),
                    json.dumps(entity.source_urls),
                    entity.confidence_score
                ))
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to save entity {entity.id}: {e}")
            return False

    def save_entities(self, entities: List[Entity]) -> int:
        saved = 0
        for entity in entities:
            if self.save_entity(entity):
                saved += 1
        logger.info(f"Saved {saved}/{len(entities)} entities to database")
        return saved

    def save_task(self, task: CollectionTask) -> bool:
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO tasks (
                        id, agent_id, product_category, state, district, taluk,
                        entity_type, data_sources, status, created_at, completed_at,
                        error, entities_collected
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    task.id,
                    task.agent_id,
                    task.product_category.value,
                    task.geography.state,
                    task.geography.district,
                    task.geography.taluk,
                    task.entity_type.value,
                    json.dumps(task.data_sources),
                    task.status,
                    task.created_at.isoformat(),
                    task.completed_at.isoformat() if task.completed_at else None,
                    task.error,
                    task.entities_collected
                ))
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to save task {task.id}: {e}")
            return False

    def get_entities(self, filters: Dict[str, Any] = None) -> List[Entity]:
        query = "SELECT * FROM entities WHERE 1=1"
        params = []
        
        if filters:
            if "state" in filters:
                query += " AND state = ?"
                params.append(filters["state"])
            if "product_category" in filters:
                query += " AND product_categories LIKE ?"
                params.append(f"%{filters['product_category']}%")
            if "entity_type" in filters:
                query += " AND entity_type = ?"
                params.append(filters["entity_type"])
        
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(query, params).fetchall()
        
        entities = []
        for row in rows:
            entity = self._row_to_entity(row)
            entities.append(entity)
        
        return entities

    def _row_to_entity(self, row: sqlite3.Row) -> Entity:
        prices = []
        try:
            prices_data = json.loads(row["prices"]) if row["prices"] else []
            for p in prices_data:
                prices.append(type('PriceInfo', (), {
                    'sku': p.get('sku', ''),
                    'market_price_today': p.get('market_price_today'),
                    'purchase_price': p.get('purchase_price'),
                    'market_selling_price': p.get('market_selling_price'),
                    'unit': p.get('unit', 'KG'),
                    'currency': p.get('currency', 'INR'),
                    'last_updated': datetime.fromisoformat(p['last_updated']) if p.get('last_updated') else datetime.utcnow()
                })())
        except:
            pass

        return Entity(
            id=row["id"],
            entity_type=type('EntityType', (), {'value': row["entity_type"]})(),
            name=row["name"],
            geography=type('Geography', (), {
                'state': row["state"],
                'district': row["district"],
                'taluk': row["taluk"]
            })(),
            contact_details=type('ContactDetails', (), {
                'name': row["contact_name"],
                'phone': row["phone"],
                'mobile': row["mobile"],
                'email': row["email"],
                'website': row["website"]
            })(),
            year_established=row["year_established"],
            gst_number=row["gst_number"],
            office_address=row["office_address"],
            product_categories=[type('ProductCategory', (), {'value': p})() 
                              for p in json.loads(row["product_categories"]) if row["product_categories"]],
            prices=prices,
            payment_terms=type('PaymentTerms', (), {'value': row["payment_terms"]})() if row["payment_terms"] else None,
            support_services=[type('SupportService', (), {'value': s})() 
                            for s in json.loads(row["support_services"]) if row["support_services"]],
            delivery_available=type('DeliveryAvailability', (), {'value': row["delivery_available"]})(),
            data_sources=json.loads(row["data_sources"]) if row["data_sources"] else [],
            collected_at=datetime.fromisoformat(row["collected_at"]) if row["collected_at"] else datetime.utcnow(),
            source_urls=json.loads(row["source_urls"]) if row["source_urls"] else [],
            confidence_score=row["confidence_score"] or 0.0
        )

    def get_stats(self) -> Dict[str, Any]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            total = conn.execute("SELECT COUNT(*) as c FROM entities").fetchone()["c"]
            
            by_type = dict(conn.execute("""
                SELECT entity_type, COUNT(*) as c FROM entities GROUP BY entity_type
            """).fetchall())
            
            by_state = dict(conn.execute("""
                SELECT state, COUNT(*) as c FROM entities GROUP BY state ORDER BY c DESC
            """).fetchall())
            
            tasks_completed = conn.execute("""
                SELECT COUNT(*) as c FROM tasks WHERE status = 'completed'
            """).fetchone()["c"]
            
            tasks_failed = conn.execute("""
                SELECT COUNT(*) as c FROM tasks WHERE status = 'failed'
            """).fetchone()["c"]

        return {
            "total_entities": total,
            "by_entity_type": by_type,
            "by_state": by_state,
            "tasks_completed": tasks_completed,
            "tasks_failed": tasks_failed
        }


class JSONStorage:
    def __init__(self, output_dir: str = "output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def save_consolidated(self, consolidated: Dict[str, ConsolidatedProductData], 
                         filename: str = "commodity_data_consolidated.json") -> str:
        output_path = self.output_dir / filename
        
        data = {}
        for key, product_data in consolidated.items():
            data[key] = {
                "product_category": product_data.product_category.value,
                "state": product_data.state,
                "district": product_data.district,
                "taluk": product_data.taluk,
                "collected_at": product_data.collected_at.isoformat(),
                "total_entities": product_data.total_entities,
                "sources_used": product_data.sources_used,
                "entities": [
                    {
                        "id": e.id,
                        "entity_type": e.entity_type.value,
                        "name": e.name,
                        "geography": {
                            "state": e.geography.state,
                            "district": e.geography.district,
                            "taluk": e.geography.taluk
                        },
                        "contact_details": {
                            "name": e.contact_details.name,
                            "phone": e.contact_details.phone,
                            "mobile": e.contact_details.mobile,
                            "email": e.contact_details.email,
                            "website": e.contact_details.website
                        },
                        "year_established": e.year_established,
                        "gst_number": e.gst_number,
                        "office_address": e.office_address,
                        "product_categories": [p.value for p in e.product_categories],
                        "prices": [
                            {
                                "sku": p.sku,
                                "market_price_today": p.market_price_today,
                                "purchase_price": p.purchase_price,
                                "market_selling_price": p.market_selling_price,
                                "unit": p.unit,
                                "currency": p.currency
                            } for p in e.prices
                        ],
                        "payment_terms": e.payment_terms.value if e.payment_terms else None,
                        "support_services": [s.value for s in e.support_services],
                        "delivery_available": e.delivery_available.value,
                        "data_sources": e.data_sources,
                        "collected_at": e.collected_at.isoformat(),
                        "source_urls": e.source_urls,
                        "confidence_score": e.confidence_score
                    } for e in product_data.entities
                ]
            }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved consolidated data to {output_path}")
        return str(output_path)

    def save_entities_json(self, entities: List[Entity], filename: str = "all_entities.json") -> str:
        output_path = self.output_dir / filename
        
        data = [
            {
                "id": e.id,
                "entity_type": e.entity_type.value,
                "name": e.name,
                "geography": {
                    "state": e.geography.state,
                    "district": e.geography.district,
                    "taluk": e.geography.taluk
                },
                "contact_details": {
                    "name": e.contact_details.name,
                    "phone": e.contact_details.phone,
                    "mobile": e.contact_details.mobile,
                    "email": e.contact_details.email,
                    "website": e.contact_details.website
                },
                "year_established": e.year_established,
                "gst_number": e.gst_number,
                "office_address": e.office_address,
                "product_categories": [p.value for p in e.product_categories],
                "prices": [
                    {
                        "sku": p.sku,
                        "market_price_today": p.market_price_today,
                        "purchase_price": p.purchase_price,
                        "market_selling_price": p.market_selling_price,
                        "unit": p.unit,
                        "currency": p.currency
                    } for p in e.prices
                ],
                "payment_terms": e.payment_terms.value if e.payment_terms else None,
                "support_services": [s.value for s in e.support_services],
                "delivery_available": e.delivery_available.value,
                "data_sources": e.data_sources,
                "collected_at": e.collected_at.isoformat(),
                "source_urls": e.source_urls,
                "confidence_score": e.confidence_score
            } for e in entities
        ]
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved {len(entities)} entities to {output_path}")
        return str(output_path)