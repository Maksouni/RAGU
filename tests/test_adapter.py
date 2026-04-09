import os
import uuid
from typing import Any, Dict, List, Optional

import pytest
import pytest_asyncio

from ragu.graph.types import Entity, Relation
from ragu.storage.graph_storage_adapters.memgraph_adapter import (
    EDGE_TYPE,
    NODE_LABEL,
    MemgraphStorage,
    _entity_from_row,
    _entity_to_attrs,
    _esc,
    _parse_json_list,
    _relation_from_row,
)


class RecordingMemgraphStorage(MemgraphStorage):
    """Fake storage that records Cypher instead of executing it."""

    def __init__(self, responses: Optional[List[List[Dict[str, Any]]]] = None):
        super().__init__(uri="bolt://fake:7687")
        self.responses = list(responses or [])
        self.commands: List[str] = []
        self.params: List[Dict[str, Any]] = []

    async def _run(
        self, query: str, parameters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        self.commands.append(query)
        self.params.append(parameters or {})
        if self.responses:
            return self.responses.pop(0)
        return []


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, ""),
        ("abc", "abc"),
        ("a'b", "a\\'b"),
        ("a\\b", "a\\\\b"),
    ],
)
def test_esc(value, expected):
    assert _esc(value) == expected


def test_parse_json_list():
    assert _parse_json_list(None) == []
    assert _parse_json_list(["a"]) == ["a"]
    assert _parse_json_list('["a","b"]') == ["a", "b"]
    assert _parse_json_list("bad") == []


def test_entity_roundtrip():
    entity = Entity(
        id="1",
        entity_name="VLC",
        entity_type="Software",
        description="Player",
        source_chunk_id=["c1"],
        documents_id=["d1"],
        clusters=[],
    )

    restored = _entity_from_row(_entity_to_attrs(entity))
    assert restored.id == entity.id
    assert restored.entity_name == entity.entity_name
    assert restored.source_chunk_id == entity.source_chunk_id


def test_relation_defaults():
    rel = _relation_from_row({"id": "r1"}, "s", "o")
    assert rel.subject_id == "s"
    assert rel.object_id == "o"
    assert rel.relation_type == "UNKNOWN"


@pytest.mark.asyncio
async def test_upsert_node_cypher():
    storage = RecordingMemgraphStorage()
    node = Entity(
        id="1",
        entity_name="VLC",
        entity_type="Software",
        description="desc",
        source_chunk_id=["c1"],
        documents_id=["d1"],
        clusters=[],
    )
    await storage.upsert_nodes([node])

    assert len(storage.commands) == 1
    assert f"MERGE (n:{NODE_LABEL} {{id: $id}})" in storage.commands[0]
    assert storage.params[0]["id"] == "1"
    assert storage.params[0]["attrs"]["entity_name"] == "VLC"


@pytest.mark.asyncio
async def test_upsert_edge_cypher():
    storage = RecordingMemgraphStorage()
    edge = Relation(
        id="r1",
        subject_id="a",
        object_id="b",
        subject_name="A",
        object_name="B",
        relation_type="REL",
        description="desc",
        relation_strength=1.0,
        source_chunk_id=["c1"],
    )
    await storage.upsert_edges([edge])

    assert "DELETE r" in storage.commands[0]
    assert f"MERGE (s)-[r:{EDGE_TYPE} {{id: $id}}]->(o)" in storage.commands[1]
    assert storage.params[1]["attrs"]["relation_type"] == "REL"


@pytest.mark.asyncio
async def test_get_nodes_and_edges_mapping():
    storage = RecordingMemgraphStorage(
        responses=[
            [{"n": {"id": "a", "entity_name": "Alice", "entity_type": "Person"}}],
            [],
            [{"r": {"id": "r1", "relation_type": "KNOWS"}, "subject_id": "a", "object_id": "b"}],
            [],
        ]
    )

    nodes = await storage.get_nodes(["a", "missing"])
    assert nodes[0] is not None
    assert nodes[0].entity_name == "Alice"
    assert nodes[1] is None

    edges = await storage.get_edges([("a", "b", "r1"), ("x", "y", None)])
    assert edges[0] is not None
    assert edges[0].id == "r1"
    assert edges[0].subject_id == "a"
    assert edges[1] is None


@pytest.mark.asyncio
async def test_edges_degrees_reads_count():
    storage = RecordingMemgraphStorage(responses=[[{"count": 3}], []])
    degrees = await storage.edges_degrees(
        [("ent-1", "ent-2", "rel-1"), ("ent-404", "ent-405", None)]
    )
    assert degrees == [3, 0]


@pytest.mark.asyncio
async def test_delete_edges_with_and_without_relation_id():
    storage = RecordingMemgraphStorage()
    await storage.delete_edges([("a", "b", None), ("a", "b", "r1")])

    assert "DELETE r" in storage.commands[0]
    assert "WHERE r.id = $relation_id" in storage.commands[1]
    assert storage.params[1]["relation_id"] == "r1"


@pytest.mark.asyncio
async def test_fetch_all_entities_and_relations():
    storage = RecordingMemgraphStorage(
        responses=[
            [{"n": {"id": "a", "entity_name": "Alice", "entity_type": "Person"}}],
            [{"r": {"id": "r1", "relation_type": "KNOWS"}, "subject_id": "a", "object_id": "b"}],
            [
                {
                    "r": {"id": "r2", "relation_type": "WORKS_WITH"},
                    "subject_id": "a",
                    "object_id": "c",
                }
            ],
        ]
    )

    nodes = await storage.get_all_nodes()
    edges = await storage.get_all_edges()
    node_edges = await storage.get_all_edges_for_nodes(["a"])

    assert len(nodes) == 1
    assert len(edges) == 1
    assert len(node_edges) == 1
    assert node_edges[0][0].id == "r2"


@pytest.fixture
def integration_enabled():
    if os.getenv("RUN_MEMGRAPH_INTEGRATION") != "1":
        pytest.skip("integration disabled")


@pytest_asyncio.fixture
async def storage(integration_enabled):
    s = MemgraphStorage(uri=os.getenv("MEMGRAPH_URI", "bolt://localhost:7687"))
    await s.index_start_callback()
    try:
        yield s
    finally:
        await s.close()


@pytest.mark.asyncio
async def test_full_cycle(storage):
    uid = uuid.uuid4().hex[:6]
    node_a_id = f"a_{uid}"
    node_b_id = f"b_{uid}"
    rel_id = f"r_{uid}"

    node_a = Entity(node_a_id, "A", "T", "", [], [], [])
    node_b = Entity(node_b_id, "B", "T", "", [], [], [])
    await storage.upsert_nodes([node_a, node_b])

    nodes = await storage.get_nodes([node_a_id, node_b_id])
    assert nodes[0] is not None
    assert nodes[1] is not None

    edge = Relation(
        id=rel_id,
        subject_id=node_a_id,
        object_id=node_b_id,
        subject_name="A",
        object_name="B",
        relation_type="REL",
        description="",
        relation_strength=1.0,
        source_chunk_id=[],
    )
    await storage.upsert_edges([edge])

    edges = await storage.get_edges([(node_a_id, node_b_id, rel_id)])
    assert edges[0] is not None

    await storage.delete_edges([(node_a_id, node_b_id, rel_id)])
    await storage.delete_nodes([node_a_id, node_b_id])
