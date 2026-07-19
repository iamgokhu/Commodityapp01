"""Knowledge graph engine for commodity market intelligence."""
import json
import logging
import os
from typing import Any, Dict, List, Optional, Set, Tuple
from pathlib import Path

import networkx as nx

from commodity_os.core.events import event_bus, EventType

logger = logging.getLogger(__name__)

GRAPH_PERSIST_PATH = Path(__file__).resolve().parents[2] / "data" / "knowledge_graph.json"

ENTITY_PREFIX = "entity:"
COMMODITY_PREFIX = "commodity:"
GEOGRAPHY_PREFIX = "geography:"

RELATIONSHIP_TYPES = {
    "SUPPLIES",
    "LOCATED_IN",
    "COMPETES_WITH",
    "PRICE_CORRELATED",
    "SUPPLY_CHAIN_TO",
    "EXPORTS_TO",
}


def _node_id(name: str, prefix: str) -> str:
    return f"{prefix}{name.lower().strip()}"


def _strip_prefix(node_id: str) -> str:
    for prefix in (ENTITY_PREFIX, COMMODITY_PREFIX, GEOGRAPHY_PREFIX):
        if node_id.startswith(prefix):
            return node_id[len(prefix):]
    return node_id


class KnowledgeGraph:
    """Graph-based knowledge store for commodity market entities and relationships."""

    def __init__(self, persist_path: Optional[str] = None):
        self.graph = nx.DiGraph()
        self._persist_path = Path(persist_path) if persist_path else GRAPH_PERSIST_PATH
        self._load()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _load(self):
        if self._persist_path.exists():
            try:
                data = json.loads(self._persist_path.read_text(encoding="utf-8"))
                self.from_json(data)
                logger.info("Loaded knowledge graph from %s", self._persist_path)
            except Exception as exc:
                logger.error("Failed to load knowledge graph: %s", exc)

    def _save(self):
        try:
            self._persist_path.parent.mkdir(parents=True, exist_ok=True)
            self._persist_path.write_text(
                json.dumps(self.to_json(), indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
        except Exception as exc:
            logger.error("Failed to persist knowledge graph: %s", exc)

    # ------------------------------------------------------------------
    # Node helpers
    # ------------------------------------------------------------------

    def _add_node(self, node_id: str, **attrs):
        if self.graph.has_node(node_id):
            self.graph.nodes[node_id].update(attrs)
        else:
            self.graph.add_node(node_id, **attrs)

    def _emit_update(self, operation: str, detail: Optional[Dict[str, Any]] = None):
        payload = {
            "operation": operation,
            "nodes": self.graph.number_of_nodes(),
            "edges": self.graph.number_of_edges(),
        }
        if detail:
            payload.update(detail)
        try:
            loop = __import__("asyncio").get_event_loop()
            if loop.is_running():
                loop.create_task(
                    event_bus.emit(EventType.KNOWLEDGE_GRAPH_UPDATED, payload, source="knowledge_graph")
                )
            else:
                loop.run_until_complete(
                    event_bus.emit(EventType.KNOWLEDGE_GRAPH_UPDATED, payload, source="knowledge_graph")
                )
        except RuntimeError:
            import asyncio
            asyncio.run(
                event_bus.emit(EventType.KNOWLEDGE_GRAPH_UPDATED, payload, source="knowledge_graph")
            )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add_entity(self, entity_dict: Dict[str, Any]):
        name = entity_dict.get("name", "").strip()
        if not name:
            logger.warning("add_entity called without a name")
            return
        node_id = _node_id(name, ENTITY_PREFIX)
        attrs = {k: v for k, v in entity_dict.items() if k != "name"}
        attrs["node_type"] = "entity"
        attrs["name"] = name
        self._add_node(node_id, **attrs)
        logger.info("Added entity: %s", name)
        self._emit_update("add_entity", {"name": name})
        self._save()

    def add_commodity(self, name: str):
        name = name.strip()
        if not name:
            return
        node_id = _node_id(name, COMMODITY_PREFIX)
        self._add_node(node_id, node_type="commodity", name=name)
        logger.info("Added commodity: %s", name)
        self._emit_update("add_commodity", {"name": name})
        self._save()

    def add_geography(self, name: str, level: str):
        name = name.strip()
        if not name:
            return
        node_id = _node_id(name, GEOGRAPHY_PREFIX)
        self._add_node(node_id, node_type="geography", name=name, level=level)
        logger.info("Added geography: %s (level=%s)", name, level)
        self._emit_update("add_geography", {"name": name, "level": level})
        self._save()

    def add_relationship(
        self,
        source: str,
        target: str,
        rel_type: str,
        attrs: Optional[Dict[str, Any]] = None,
    ):
        rel_type = rel_type.upper().strip()
        if rel_type not in RELATIONSHIP_TYPES:
            logger.warning("Unknown relationship type: %s", rel_type)
            return
        self.graph.add_edge(source, target, relationship=rel_type, **(attrs or {}))
        logger.info("Added relationship: %s --%s--> %s", source, rel_type, target)
        self._emit_update("add_relationship", {"source": source, "target": target, "rel_type": rel_type})
        self._save()

    # ------------------------------------------------------------------
    # Query methods
    # ------------------------------------------------------------------

    def find_supply_chain(self, commodity: str) -> List[Dict[str, Any]]:
        commodity_id = _node_id(commodity, COMMODITY_PREFIX)
        if not self.graph.has_node(commodity_id):
            return []
        results = []
        for pred in self.graph.predecessors(commodity_id):
            edge = self.graph.edges[pred, commodity_id]
            if edge.get("relationship") == "SUPPLIES":
                node_data = dict(self.graph.nodes[pred])
                node_data["node_id"] = pred
                node_data["commodity"] = commodity
                results.append(node_data)
        return results

    def find_competitors(self, entity: str) -> List[Dict[str, Any]]:
        entity_id = _node_id(entity, ENTITY_PREFIX)
        if not self.graph.has_node(entity_id):
            return []
        competitors: List[Dict[str, Any]] = []
        for _, target, data in self.graph.out_edges(entity_id, data=True):
            if data.get("relationship") == "COMPETES_WITH":
                node_data = dict(self.graph.nodes[target])
                node_data["node_id"] = target
                competitors.append(node_data)
        for source, _, data in self.graph.in_edges(entity_id, data=True):
            if data.get("relationship") == "COMPETES_WITH":
                node_data = dict(self.graph.nodes[source])
                node_data["node_id"] = source
                competitors.append(node_data)
        return competitors

    def get_price_correlations(self, commodity: str) -> List[Dict[str, Any]]:
        commodity_id = _node_id(commodity, COMMODITY_PREFIX)
        if not self.graph.has_node(commodity_id):
            return []
        correlated: List[Dict[str, Any]] = []
        for _, target, data in self.graph.out_edges(commodity_id, data=True):
            if data.get("relationship") == "PRICE_CORRELATED":
                node_data = dict(self.graph.nodes[target])
                node_data["node_id"] = target
                node_data["correlation_attrs"] = data
                correlated.append(node_data)
        for source, _, data in self.graph.in_edges(commodity_id, data=True):
            if data.get("relationship") == "PRICE_CORRELATED":
                node_data = dict(self.graph.nodes[source])
                node_data["node_id"] = source
                node_data["correlation_attrs"] = data
                correlated.append(node_data)
        return correlated

    def get_region_summary(self, state: str) -> Dict[str, Any]:
        state_id = _node_id(state, GEOGRAPHY_PREFIX)
        entities_in_region: List[str] = []
        commodities_in_region: Set[str] = set()

        for source, target, data in self.graph.edges(data=True):
            if data.get("relationship") == "LOCATED_IN" and target == state_id:
                entities_in_region.append(source)
                for _, comm_target, comm_data in self.graph.out_edges(source, data=True):
                    if comm_data.get("relationship") == "SUPPLIES" and comm_target.startswith(COMMODITY_PREFIX):
                        commodities_in_region.add(_strip_prefix(comm_target))

        return {
            "state": state,
            "entity_count": len(entities_in_region),
            "entities": entities_in_region,
            "commodity_count": len(commodities_in_region),
            "commodities": sorted(commodities_in_region),
        }

    def discover_relationships(self):
        """Auto-discover and emit summary of existing relationships."""
        discovered: Dict[str, int] = {}
        for _, _, data in self.graph.edges(data=True):
            rel = data.get("relationship", "unknown")
            discovered[rel] = discovered.get(rel, 0) + 1
        logger.info("Discovered relationships: %s", discovered)
        self._emit_update("discover_relationships", {"discovered": discovered})
        return discovered

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_json(self) -> Dict[str, Any]:
        nodes = []
        for nid, attrs in self.graph.nodes(data=True):
            node_entry = {"id": nid, **attrs}
            nodes.append(node_entry)

        edges = []
        for src, tgt, attrs in self.graph.edges(data=True):
            edge_entry = {"source": src, "target": tgt, **attrs}
            edges.append(edge_entry)

        return {"nodes": nodes, "edges": edges}

    def from_json(self, data: Dict[str, Any]):
        self.graph.clear()
        for node in data.get("nodes", []):
            nid = node.pop("id")
            self.graph.add_node(nid, **node)
        for edge in data.get("edges", []):
            src = edge.pop("source")
            tgt = edge.pop("target")
            self.graph.add_edge(src, tgt, **edge)

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def get_stats(self) -> Dict[str, Any]:
        entity_count = sum(1 for _, a in self.graph.nodes(data=True) if a.get("node_type") == "entity")
        commodity_count = sum(1 for _, a in self.graph.nodes(data=True) if a.get("node_type") == "commodity")
        geography_count = sum(1 for _, a in self.graph.nodes(data=True) if a.get("node_type") == "geography")

        rel_counts: Dict[str, int] = {}
        for _, _, data in self.graph.edges(data=True):
            rel = data.get("relationship", "unknown")
            rel_counts[rel] = rel_counts.get(rel, 0) + 1

        return {
            "total_nodes": self.graph.number_of_nodes(),
            "total_edges": self.graph.number_of_edges(),
            "entity_count": entity_count,
            "commodity_count": commodity_count,
            "geography_count": geography_count,
            "relationship_counts": rel_counts,
            "is_dag": nx.is_directed_acyclic_graph(self.graph) if self.graph.number_of_nodes() > 0 else False,
            "num_connected_components": nx.number_weakly_connected_components(self.graph),
        }
