"""ArcadeDB graph storage adapter for RAGU. Implements BaseGraphStorage."""

from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Optional

import httpx

from ragu.graph.types import Entity, Relation
from ragu.storage.base_storage import BaseGraphStorage, EdgeSpec

VERTEX_TYPE = "RaguEntity"
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
        "source_chunk_id": json.dumps(list(e.source_chunk_id)),
        "documents_id": json.dumps(list(e.documents_id)),
        "clusters": json.dumps(list(e.clusters)),
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


def _relation_from_row(row: Dict[str, Any]) -> Relation:
    return Relation(
        id=str(row.get("id", "")),
        subject_id=str(row.get("subject_id", "")),
        object_id=str(row.get("object_id", "")),
        subject_name=row.get("subject_name", ""),
        object_name=row.get("object_name", ""),
        relation_type=row.get("relation_type", "UNKNOWN"),
        description=row.get("description", ""),
        relation_strength=float(row.get("relation_strength", 1.0)),
        source_chunk_id=_parse_json_list(row.get("source_chunk_id")),
    )


class ArcadeDBStorage(BaseGraphStorage):
    """ArcadeDB implementation of BaseGraphStorage for RAGU."""

    def __init__(
            self,
            url: str = "http://localhost:2480",
            database: str = "tododb",
            auth: Optional[tuple[str, str]] = ("root", "playwithdata"),
            **kwargs: Any,
    ):
        self._base_url = str(kwargs.get("url", url)).rstrip("/")
        self._database = str(kwargs.get("database", database))
        self._auth = kwargs.get("auth", auth)
        self._client: Optional[httpx.AsyncClient] = None

    def _endpoint(self) -> str:
        return f"{self._base_url}/api/v1/command/{self._database}"

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                auth=httpx.BasicAuth(*self._auth) if self._auth else None,
                timeout=30.0,
            )
        return self._client

    async def _command(self, sql: str) -> List[Dict[str, Any]]:
        client = await self._get_client()
        r = await client.post(
            self._endpoint(),
            json={"language": "sql", "command": sql},
        )
        r.raise_for_status()
        data = r.json()
        return data.get("result", []) if isinstance(data, dict) else []

    async def index_start_callback(self) -> None:
        await self._ensure_schema()

    async def _ensure_schema(self) -> None:
        try:
            await self._command(f"CREATE VERTEX TYPE {VERTEX_TYPE} IF NOT EXISTS")
            await self._command(f"CREATE EDGE TYPE {EDGE_TYPE} IF NOT EXISTS")

            v_props = ["id", "entity_name", "entity_type", "description", "source_chunk_id", "documents_id", "clusters"]
            for p in v_props:
                await self._command(f"CREATE PROPERTY {VERTEX_TYPE}.{p} IF NOT EXISTS STRING")

            await self._command(f"CREATE INDEX IF NOT EXISTS ON {VERTEX_TYPE} (id) UNIQUE")

            # Denormalizing subject_id and object_id onto the edge for robust querying
            e_str_props = ["id", "subject_id", "object_id", "subject_name", "object_name", "relation_type", "description", "source_chunk_id"]
            for p in e_str_props:
                await self._command(f"CREATE PROPERTY {EDGE_TYPE}.{p} IF NOT EXISTS STRING")

            await self._command(f"CREATE PROPERTY {EDGE_TYPE}.relation_strength IF NOT EXISTS DOUBLE")
        except Exception:
            pass

    async def index_done_callback(self) -> None:
        pass

    async def query_done_callback(self) -> None:
        pass

    async def edges_degrees(self, edge_specs: List[EdgeSpec]) -> List[int]:
        result: List[int] = []
        for subject_id, object_id, _ in edge_specs:
            sql = (
                f"SELECT count(*) as count "
                f"FROM {EDGE_TYPE} "
                f"WHERE subject_id IN ['{_esc(subject_id)}', '{_esc(object_id)}'] "
                f"OR object_id IN ['{_esc(subject_id)}', '{_esc(object_id)}']"
            )
            rows = await self._command(sql)
            result.append(int(rows[0].get("count", 0)) if rows else 0)
        return result

    async def get_nodes(self, node_ids: List[str]) -> List[Optional[Entity]]:
        result: List[Optional[Entity]] = []
        for nid in node_ids:
            sql = f"SELECT FROM {VERTEX_TYPE} WHERE id = '{_esc(nid)}'"
            rows = await self._command(sql)
            result.append(_entity_from_row(rows[0]) if rows else None)
        return result

    async def upsert_nodes(self, nodes: Iterable[Entity]) -> None:
        for node in nodes:
            attrs = _entity_to_attrs(node)
            assignments = ", ".join(f"{k} = '{_esc(v)}'" for k, v in attrs.items())
            sql = f"UPDATE {VERTEX_TYPE} SET {assignments} UPSERT WHERE id = '{_esc(node.id)}'"
            await self._command(sql)

    async def delete_nodes(self, node_ids: List[str]) -> None:
        for nid in node_ids:
            await self._command(f"DELETE FROM {VERTEX_TYPE} WHERE id = '{_esc(nid)}'")

    async def get_edges(self, edge_specs: List[EdgeSpec]) -> List[Optional[Relation]]:
        result: List[Optional[Relation]] = []
        for subject_id, object_id, relation_id in edge_specs:
            sql = f"SELECT FROM {EDGE_TYPE} WHERE subject_id = '{_esc(subject_id)}' AND object_id = '{_esc(object_id)}'"
            if relation_id:
                sql += f" AND id = '{_esc(relation_id)}'"
            rows = await self._command(sql)
            result.append(_relation_from_row(rows[0]) if rows else None)
        return result

    async def upsert_edges(self, edges: List[Relation]) -> None:
        for edge in edges:
            # 1. Cleanly delete the old edge using string properties (Avoids graph traversal 500 errors)
            del_sql = (
                f"DELETE FROM {EDGE_TYPE} "
                f"WHERE subject_id = '{_esc(edge.subject_id)}' "
                f"AND object_id = '{_esc(edge.object_id)}' "
                f"AND relation_type = '{_esc(edge.relation_type)}'"
            )
            await self._command(del_sql)

            # 2. Create the new edge and populate all properties including endpoints
            content = {
                "id": str(edge.id),
                "subject_id": str(edge.subject_id),
                "object_id": str(edge.object_id),
                "subject_name": edge.subject_name,
                "object_name": edge.object_name,
                "relation_type": edge.relation_type,
                "description": edge.description or "",
                "relation_strength": float(edge.relation_strength),
                "source_chunk_id": json.dumps(list(edge.source_chunk_id)),
            }

            set_parts = []
            for k, v in content.items():
                val = f"'{_esc(v)}'" if isinstance(v, str) else str(v)
                set_parts.append(f"{k} = {val}")

            create_sql = (
                f"CREATE EDGE {EDGE_TYPE} "
                f"FROM (SELECT FROM {VERTEX_TYPE} WHERE id = '{_esc(edge.subject_id)}') "
                f"TO (SELECT FROM {VERTEX_TYPE} WHERE id = '{_esc(edge.object_id)}') "
                f"SET {', '.join(set_parts)}"
            )
            await self._command(create_sql)

    async def delete_edges(self, edge_specs: List[EdgeSpec]) -> None:
        for subject_id, object_id, relation_id in edge_specs:
            sql = f"DELETE FROM {EDGE_TYPE} WHERE subject_id = '{_esc(subject_id)}' AND object_id = '{_esc(object_id)}'"
            if relation_id:
                sql += f" AND id = '{_esc(relation_id)}'"
            await self._command(sql)

    async def get_all_edges_for_nodes(self, node_ids: List[str]) -> List[List[Relation]]:
        result: List[List[Relation]] = []
        for nid in node_ids:
            sql = f"SELECT FROM {EDGE_TYPE} WHERE subject_id = '{_esc(nid)}' OR object_id = '{_esc(nid)}'"
            rows = await self._command(sql)
            rels = [_relation_from_row(r) for r in rows]
            result.append(rels)
        return result

    async def get_all_nodes(self) -> List[Entity]:
        rows = await self._command(f"SELECT FROM {VERTEX_TYPE}")
        return [_entity_from_row(r) for r in rows]

    async def get_all_edges(self) -> List[Relation]:
        rows = await self._command(f"SELECT FROM {EDGE_TYPE}")
        return [_relation_from_row(r) for r in rows]

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
