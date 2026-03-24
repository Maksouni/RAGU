"""ArcadeDB graph storage adapter for RAGU. Implements BaseGraphStorage."""

from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Optional

import httpx

from ragu.graph.types import Entity, Relation
from ragu.storage.base_storage import BaseGraphStorage, EdgeSpec

VERTEX_TYPE = "RaguEntity"
EDGE_TYPE = "RaguRelation"


def _entity_to_attrs(e: Entity) -> Dict[str, Any]:
    return {
        "id": e.id,
        "entity_name": e.entity_name,
        "entity_type": e.entity_type,
        "description": e.description or "",
        "source_chunk_id": json.dumps(list(e.source_chunk_id)),
        "documents_id": json.dumps(list(e.documents_id)),
        "clusters": json.dumps(e.clusters),
    }


def _entity_from_row(row: Dict[str, Any]) -> Entity:
    def _parse_list(s: str) -> list:
        return json.loads(s) if isinstance(s, str) else (s or [])

    return Entity(
        id=str(row.get("id", "")),
        entity_name=row.get("entity_name", ""),
        entity_type=row.get("entity_type", ""),
        description=row.get("description", ""),
        source_chunk_id=_parse_list(row.get("source_chunk_id", "[]")),
        documents_id=_parse_list(row.get("documents_id", "[]")),
        clusters=_parse_list(row.get("clusters", "[]")),
    )


def _relation_from_row(row: Dict[str, Any], subject_id: str, object_id: str) -> Relation:
    def _parse_list(s: str) -> list:
        return json.loads(s) if isinstance(s, str) else (s or [])

    return Relation(
        subject_id=subject_id,
        object_id=object_id,
        subject_name=row.get("subject_name", subject_id),
        object_name=row.get("object_name", object_id),
        relation_type=row.get("relation_type", "UNKNOWN"),
        description=row.get("description", ""),
        relation_strength=float(row.get("relation_strength", 1.0)),
        source_chunk_id=_parse_list(row.get("source_chunk_id", "[]")),
        id=str(row.get("id", "")),
    )


class ArcadeDBStorage(BaseGraphStorage):
    """ArcadeDB implementation of BaseGraphStorage for RAGU."""

    def __init__(
        self,
        url: str = "http://localhost:2480",
        database: str = "tododb",
        auth: Optional[tuple[str, str]] = None,
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
        except Exception:
            pass
        try:
            await self._command(f"CREATE EDGE TYPE {EDGE_TYPE} IF NOT EXISTS")
        except Exception:
            pass

    async def index_done_callback(self) -> None:
        pass

    async def query_done_callback(self) -> None:
        pass

    async def edges_degrees(self, edge_specs: List[EdgeSpec]) -> List[int]:
        result: List[int] = []
        for subject_id, object_id, _ in edge_specs:
            sql = f"""
            SELECT (OUT().size() + IN().size()) AS d
            FROM {VERTEX_TYPE}
            WHERE id = '{_esc(subject_id)}' OR id = '{_esc(object_id)}'
            """
            rows = await self._command(sql)
            deg = sum(int(r.get("d", 0)) for r in rows) if rows else 0
            result.append(deg)
        return result

    async def get_nodes(self, node_ids: List[str]) -> List[Optional[Entity]]:
        result: List[Optional[Entity]] = []
        for nid in node_ids:
            sql = f"SELECT FROM {VERTEX_TYPE} WHERE id = '{_esc(nid)}'"
            rows = await self._command(sql)
            if rows:
                result.append(_entity_from_row(rows[0]))
            else:
                result.append(None)
        return result

    async def upsert_nodes(self, nodes: Iterable[Entity]) -> None:
        for node in nodes:
            attrs = _entity_to_attrs(node)
            existing = await self._command(
                f"SELECT FROM {VERTEX_TYPE} WHERE id = '{_esc(node.id)}'"
            )
            if existing:
                await self._command(
                    f"UPDATE {VERTEX_TYPE} SET entity_name = '{_esc(attrs['entity_name'])}', "
                    f"entity_type = '{_esc(attrs['entity_type'])}', description = '{_esc(attrs['description'])}', "
                    f"source_chunk_id = '{_esc(attrs['source_chunk_id'])}', "
                    f"documents_id = '{_esc(attrs['documents_id'])}', clusters = '{_esc(attrs['clusters'])}' "
                    f"WHERE id = '{_esc(node.id)}'"
                )
            else:
                await self._command(
                    f"INSERT INTO {VERTEX_TYPE} CONTENT {json.dumps(attrs)}"
                )

    async def delete_nodes(self, node_ids: List[str]) -> None:
        for nid in node_ids:
            await self._command(f"DELETE FROM {VERTEX_TYPE} WHERE id = '{_esc(nid)}'")

    async def get_edges(self, edge_specs: List[EdgeSpec]) -> List[Optional[Relation]]:
        result: List[Optional[Relation]] = []
        for subject_id, object_id, relation_id in edge_specs:
            sql = f"""
            SELECT FROM {EDGE_TYPE}
            WHERE out.id = '{_esc(subject_id)}' AND in.id = '{_esc(object_id)}'
            """
            if relation_id:
                sql += f" AND id = '{_esc(relation_id)}'"
            rows = await self._command(sql)
            if rows:
                r = rows[0]
                r["subject_id"] = subject_id
                r["object_id"] = object_id
                result.append(_relation_from_row(r, subject_id, object_id))
            else:
                result.append(None)
        return result

    async def upsert_edges(self, edges: List[Relation]) -> None:
        for edge in edges:
            content = {
                "id": edge.id,
                "subject_name": edge.subject_name,
                "object_name": edge.object_name,
                "relation_type": edge.relation_type,
                "description": edge.description,
                "relation_strength": edge.relation_strength,
                "source_chunk_id": json.dumps(list(edge.source_chunk_id)),
            }
            sql = f"""
            CREATE EDGE {EDGE_TYPE}
            FROM (SELECT FROM {VERTEX_TYPE} WHERE id = '{_esc(edge.subject_id)}')
            TO (SELECT FROM {VERTEX_TYPE} WHERE id = '{_esc(edge.object_id)}')
            CONTENT {json.dumps(content)}
            """
            await self._command(sql)

    async def delete_edges(self, edge_specs: List[EdgeSpec]) -> None:
        for subject_id, object_id, relation_id in edge_specs:
            sql = f"""
            DELETE FROM {EDGE_TYPE}
            WHERE out.id = '{_esc(subject_id)}' AND in.id = '{_esc(object_id)}'
            """
            if relation_id:
                sql += f" AND id = '{_esc(relation_id)}'"
            await self._command(sql)

    async def get_all_edges_for_nodes(self, node_ids: List[str]) -> List[List[Relation]]:
        result: List[List[Relation]] = []
        for nid in node_ids:
            sql = f"""
            SELECT FROM {EDGE_TYPE}
            WHERE out.id = '{_esc(nid)}' OR in.id = '{_esc(nid)}'
            """
            rows = await self._command(sql)
            rels: List[Relation] = []
            for r in rows:
                s_id = r.get("out_id", r.get("out", nid))
                o_id = r.get("in_id", r.get("in", nid))
                if isinstance(s_id, dict):
                    s_id = s_id.get("id", nid)
                if isinstance(o_id, dict):
                    o_id = o_id.get("id", nid)
                rels.append(_relation_from_row(r, str(s_id), str(o_id)))
            result.append(rels)
        return result

    async def get_all_nodes(self) -> List[Entity]:
        rows = await self._command(f"SELECT FROM {VERTEX_TYPE}")
        return [_entity_from_row(r) for r in rows]

    async def get_all_edges(self) -> List[Relation]:
        rows = await self._command(f"SELECT FROM {EDGE_TYPE}")
        result: List[Relation] = []
        for r in rows:
            s_id = r.get("out_id", r.get("out", ""))
            o_id = r.get("in_id", r.get("in", ""))
            if isinstance(s_id, dict):
                s_id = s_id.get("id", "")
            if isinstance(o_id, dict):
                o_id = o_id.get("id", "")
            result.append(_relation_from_row(r, str(s_id), str(o_id)))
        return result


def _esc(s: str) -> str:
    return s.replace("\\", "\\\\").replace("'", "\\'")
