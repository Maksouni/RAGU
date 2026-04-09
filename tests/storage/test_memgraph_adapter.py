import pytest

from ragu.graph.types import Entity, Relation
from ragu.storage.graph_storage_adapters.memgraph_adapter import (
    EDGE_TYPE,
    NODE_LABEL,
    MemgraphStorage,
    _esc,
    _parse_json_list,
)


@pytest.fixture
def storage():
    return MemgraphStorage(uri="bolt://localhost:7687")


@pytest.fixture
def sample_entity():
    return Entity(
        id="ent-1",
        entity_name="Alice",
        entity_type="Person",
        description="Alice's profile",
        source_chunk_id=["chunk-1"],
        documents_id=["doc-1"],
        clusters=[],
    )


@pytest.fixture
def sample_relation():
    return Relation(
        id="rel-1",
        subject_id="ent-1",
        object_id="ent-2",
        subject_name="Alice",
        object_name="Bob",
        relation_type="KNOWS",
        description="Alice knows Bob",
        relation_strength=0.9,
        source_chunk_id=["chunk-1"],
    )


def test_parse_json_list_handles_invalid_values():
    assert _parse_json_list(None) == []
    assert _parse_json_list(["a", "b"]) == ["a", "b"]
    assert _parse_json_list('["x", "y"]') == ["x", "y"]
    assert _parse_json_list('{"not":"a-list"}') == []
    assert _parse_json_list("invalid-json") == []


def test_escape_handles_quotes_and_backslashes():
    assert _esc("a'b\\c") == "a\\'b\\\\c"
    assert _esc(None) == ""


@pytest.mark.asyncio
async def test_get_nodes_and_edges_mapping(storage):
    async def fake_run(query: str, params=None):
        if f"MATCH (n:{NODE_LABEL} {{id: $id}}) RETURN n LIMIT 1" in query:
            if params["id"] == "ent-1":
                return [
                    {
                        "n": {
                            "id": "ent-1",
                            "entity_name": "Alice",
                            "entity_type": "Person",
                            "description": "Engineer",
                            "source_chunk_id": ["chunk-1"],
                            "documents_id": ["doc-1"],
                            "clusters": [],
                        }
                    }
                ]
            return []
        if "RETURN r, s.id AS subject_id, o.id AS object_id LIMIT 1" in query:
            if params["subject_id"] == "ent-1" and params["object_id"] == "ent-2":
                return [
                    {
                        "r": {
                            "id": "rel-1",
                            "subject_name": "Alice",
                            "object_name": "Bob",
                            "relation_type": "KNOWS",
                            "description": "Alice knows Bob",
                            "relation_strength": 1.0,
                            "source_chunk_id": ["chunk-1"],
                        },
                        "subject_id": "ent-1",
                        "object_id": "ent-2",
                    }
                ]
            return []
        return []

    storage._run = fake_run

    nodes = await storage.get_nodes(["ent-1", "missing"])
    assert nodes[0] is not None
    assert nodes[0].entity_name == "Alice"
    assert nodes[1] is None

    edges = await storage.get_edges([("ent-1", "ent-2", "rel-1"), ("x", "y", None)])
    assert edges[0] is not None
    assert edges[0].id == "rel-1"
    assert edges[0].subject_id == "ent-1"
    assert edges[1] is None


@pytest.mark.asyncio
async def test_upsert_builds_expected_cypher(storage, sample_entity, sample_relation):
    commands = []
    params = []

    async def fake_run(query: str, query_params=None):
        commands.append(query)
        params.append(query_params or {})
        return []

    storage._run = fake_run

    await storage.upsert_nodes([sample_entity])
    await storage.upsert_edges([sample_relation])

    assert any(
        command.startswith(f"MERGE (n:{NODE_LABEL} {{id: $id}}) SET n += $attrs")
        and p.get("id") == "ent-1"
        for command, p in zip(commands, params)
    )
    assert any(
        command.startswith(
            f"MATCH (s:{NODE_LABEL} {{id: $subject_id}})-[r:{EDGE_TYPE}]->(o:{NODE_LABEL} {{id: $object_id}}) "
        )
        and "DELETE r" in command
        for command in commands
    )
    assert any(
        f"MERGE (s)-[r:{EDGE_TYPE} {{id: $id}}]->(o)" in command
        and p.get("attrs", {}).get("relation_type") == "KNOWS"
        for command, p in zip(commands, params)
    )


@pytest.mark.asyncio
async def test_edges_degrees_reads_count(storage):
    async def fake_run(query: str, params=None):
        if params and params.get("node_ids") == ["ent-1", "ent-2"]:
            return [{"count": 3}]
        return []

    storage._run = fake_run
    degrees = await storage.edges_degrees(
        [("ent-1", "ent-2", "rel-1"), ("ent-404", "ent-405", None)]
    )
    assert degrees == [3, 0]
