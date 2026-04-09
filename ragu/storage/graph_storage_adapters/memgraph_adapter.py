"""Memgraph graph storage adapter for RAGU. Implements BaseGraphStorage."""

from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Optional

from ragu.graph.types import Entity, Relation
from ragu.storage.base_storage import BaseGraphStorage, EdgeSpec

try:
    from neo4j import AsyncGraphDatabase
except ImportError:  # pragma: no cover - handled by runtime error when used
    AsyncGraphDatabase = None

NODE_LABEL = "RaguEntity"
EDGE_TYPE = "RaguRelation"


def _esc(s: str) -> str:
    if s is None:
        return ""
    return str(s).replace("\\", "\\\\").replace("'", "\\'")


def _parse_json_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


def _entity_to_attrs(e: Entity) -> Dict[str, Any]:
    return {
        "id": str(e.id),
        "entity_name": e.entity_name,
        "entity_type": e.entity_type,
        "description": e.description or "",
        "source_chunk_id": list(e.source_chunk_id),
        "documents_id": list(e.documents_id),
        "clusters": list(e.clusters),
    }


def _entity_from_row(row: Dict[str, Any]) -> Entity:
    return Entity(
        id=str(row.get("id", "")),
        entity_name=row.get("entity_name", ""),
        entity_type=row.get("entity_type", ""),
        description=row.get("description", ""),
        source_chunk_id=_parse_json_list(row.get("source_chunk_id")),
        documents_id=_parse_json_list(row.get("documents_id")),
        clusters=_parse_json_list(row.get("clusters")),
    )


def _relation_from_row(row: Dict[str, Any], subject_id: str, object_id: str) -> Relation:
    return Relation(
        id=str(row.get("id", "")),
        subject_id=str(subject_id),
        object_id=str(object_id),
        subject_name=row.get("subject_name", str(subject_id)),
        object_name=row.get("object_name", str(object_id)),
        relation_type=row.get("relation_type", "UNKNOWN"),
        description=row.get("description", ""),
        relation_strength=float(row.get("relation_strength", 1.0)),
        source_chunk_id=_parse_json_list(row.get("source_chunk_id")),
    )


class MemgraphStorage(BaseGraphStorage):
    """Memgraph implementation of BaseGraphStorage for RAGU."""

    def __init__(
        self,
        uri: str = "bolt://localhost:7687",
        username: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        **kwargs: Any,
    ):
        self._uri = str(kwargs.get("uri", uri))
        self._username = kwargs.get("username", username)
        self._password = kwargs.get("password", password)
        self._database = kwargs.get("database", database)
        self._driver = None

    async def _get_driver(self):
        if self._driver is None:
            if AsyncGraphDatabase is None:
                raise RuntimeError(
                    "Memgraph adapter requires the `neo4j` package. Install it with `pip install neo4j`."
                )
            auth = None
            if self._username:
                auth = (self._username, self._password or "")
            self._driver = AsyncGraphDatabase.driver(self._uri, auth=auth)
        return self._driver

    async def _run(
        self, query: str, parameters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        driver = await self._get_driver()
        session_kwargs: Dict[str, Any] = {}
        if self._database:
            session_kwargs["database"] = self._database
        async with driver.session(**session_kwargs) as session:
            result = await session.run(query, parameters or {})
            return await result.data()

    async def index_start_callback(self) -> None:
        await self._ensure_schema()

    async def _ensure_schema(self) -> None:
        try:
            await self._run(f"CREATE INDEX ON :{NODE_LABEL}(id)")
        except Exception:
            # Index may already exist; Memgraph returns an error in that case.
            pass

    async def index_done_callback(self) -> None:
        pass

    async def query_done_callback(self) -> None:
        pass

    async def edges_degrees(self, edge_specs: List[EdgeSpec]) -> List[int]:
        result: List[int] = []
        for subject_id, object_id, _ in edge_specs:
            rows = await self._run(
                (
                    f"MATCH ()-[r:{EDGE_TYPE}]-() "
                    "WHERE startNode(r).id IN $node_ids OR endNode(r).id IN $node_ids "
                    "RETURN count(r) AS count"
                ),
                {"node_ids": [subject_id, object_id]},
            )
            result.append(int(rows[0].get("count", 0)) if rows else 0)
        return result

    async def get_nodes(self, node_ids: List[str]) -> List[Optional[Entity]]:
        result: List[Optional[Entity]] = []
        for node_id in node_ids:
            rows = await self._run(
                f"MATCH (n:{NODE_LABEL} {{id: $id}}) RETURN n LIMIT 1",
                {"id": node_id},
            )
            node = rows[0].get("n") if rows else None
            result.append(_entity_from_row(dict(node)) if node else None)
        return result

    async def upsert_nodes(self, nodes: Iterable[Entity]) -> None:
        for node in nodes:
            attrs = _entity_to_attrs(node)
            await self._run(
                f"MERGE (n:{NODE_LABEL} {{id: $id}}) SET n += $attrs",
                {"id": str(node.id), "attrs": attrs},
            )

    async def delete_nodes(self, node_ids: List[str]) -> None:
        for node_id in node_ids:
            await self._run(
                f"MATCH (n:{NODE_LABEL} {{id: $id}}) DETACH DELETE n",
                {"id": node_id},
            )

    async def get_edges(self, edge_specs: List[EdgeSpec]) -> List[Optional[Relation]]:
        result: List[Optional[Relation]] = []
        for subject_id, object_id, relation_id in edge_specs:
            query = (
                f"MATCH (s:{NODE_LABEL} {{id: $subject_id}})-[r:{EDGE_TYPE}]->"
                f"(o:{NODE_LABEL} {{id: $object_id}}) "
            )
            params: Dict[str, Any] = {"subject_id": subject_id, "object_id": object_id}
            if relation_id:
                query += "WHERE r.id = $relation_id "
                params["relation_id"] = relation_id
            query += "RETURN r, s.id AS subject_id, o.id AS object_id LIMIT 1"
            rows = await self._run(query, params)
            if not rows:
                result.append(None)
                continue
            rel = rows[0].get("r")
            sid = rows[0].get("subject_id", subject_id)
            oid = rows[0].get("object_id", object_id)
            result.append(_relation_from_row(dict(rel), str(sid), str(oid)) if rel else None)
        return result

    async def upsert_edges(self, edges: List[Relation]) -> None:
        for edge in edges:
            await self._run(
                (
                    f"MATCH (s:{NODE_LABEL} {{id: $subject_id}})-[r:{EDGE_TYPE}]->"
                    f"(o:{NODE_LABEL} {{id: $object_id}}) "
                    "WHERE r.id = $id OR (r.relation_type = $relation_type AND r.id <> $id) "
                    "DELETE r"
                ),
                {
                    "subject_id": edge.subject_id,
                    "object_id": edge.object_id,
                    "id": str(edge.id),
                    "relation_type": edge.relation_type,
                },
            )
            attrs = {
                "id": str(edge.id),
                "subject_name": edge.subject_name,
                "object_name": edge.object_name,
                "relation_type": edge.relation_type,
                "description": edge.description or "",
                "relation_strength": float(edge.relation_strength),
                "source_chunk_id": list(edge.source_chunk_id),
            }
            await self._run(
                (
                    f"MATCH (s:{NODE_LABEL} {{id: $subject_id}}), "
                    f"(o:{NODE_LABEL} {{id: $object_id}}) "
                    f"MERGE (s)-[r:{EDGE_TYPE} {{id: $id}}]->(o) "
                    "SET r += $attrs"
                ),
                {
                    "subject_id": edge.subject_id,
                    "object_id": edge.object_id,
                    "id": str(edge.id),
                    "attrs": attrs,
                },
            )

    async def delete_edges(self, edge_specs: List[EdgeSpec]) -> None:
        for subject_id, object_id, relation_id in edge_specs:
            query = (
                f"MATCH (s:{NODE_LABEL} {{id: $subject_id}})-[r:{EDGE_TYPE}]->"
                f"(o:{NODE_LABEL} {{id: $object_id}}) "
            )
            params: Dict[str, Any] = {"subject_id": subject_id, "object_id": object_id}
            if relation_id:
                query += "WHERE r.id = $relation_id "
                params["relation_id"] = relation_id
            query += "DELETE r"
            await self._run(query, params)

    async def get_all_edges_for_nodes(self, node_ids: List[str]) -> List[List[Relation]]:
        result: List[List[Relation]] = []
        for node_id in node_ids:
            rows = await self._run(
                (
                    f"MATCH (n:{NODE_LABEL} {{id: $id}})-[r:{EDGE_TYPE}]-(m:{NODE_LABEL}) "
                    "RETURN r, startNode(r).id AS subject_id, endNode(r).id AS object_id"
                ),
                {"id": node_id},
            )
            rels: List[Relation] = []
            for row in rows:
                rel = row.get("r")
                if not rel:
                    continue
                rels.append(
                    _relation_from_row(
                        dict(rel),
                        str(row.get("subject_id", node_id)),
                        str(row.get("object_id", node_id)),
                    )
                )
            result.append(rels)
        return result

    async def get_all_nodes(self) -> List[Entity]:
        rows = await self._run(f"MATCH (n:{NODE_LABEL}) RETURN n")
        entities: List[Entity] = []
        for row in rows:
            node = row.get("n")
            if node:
                entities.append(_entity_from_row(dict(node)))
        return entities

    async def get_all_edges(self) -> List[Relation]:
        rows = await self._run(
            f"MATCH (s:{NODE_LABEL})-[r:{EDGE_TYPE}]->(o:{NODE_LABEL}) "
            "RETURN r, s.id AS subject_id, o.id AS object_id"
        )
        relations: List[Relation] = []
        for row in rows:
            rel = row.get("r")
            if not rel:
                continue
            relations.append(
                _relation_from_row(
                    dict(rel),
                    str(row.get("subject_id", "")),
                    str(row.get("object_id", "")),
                )
            )
        return relations

    async def close(self) -> None:
        if self._driver is not None:
            await self._driver.close()
            self._driver = None
