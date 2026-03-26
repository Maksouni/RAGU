import httpx
import pytest

from ragu.graph.types import Entity, Relation
from ragu.storage.graph_storage_adapters import arcadedb_adapter as adapter_module
from ragu.storage.graph_storage_adapters.arcadedb_adapter import (
    ArcadeDBStorage,
    _entity_from_row,
    _entity_to_attrs,
    _esc,
    _parse_json_list,
    _relation_from_row,
)


class DummyResponse:
    """A tiny HTTP response double for adapter unit tests."""

    def __init__(self, payload, error=None):
        self.payload = payload
        self.error = error
        self.raise_for_status_called = False

    def raise_for_status(self):
        self.raise_for_status_called = True
        if self.error is not None:
            raise self.error

    def json(self):
        return self.payload


class DummyAsyncClient:
    """A tiny async HTTP client double that records outbound requests."""

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.is_closed = False
        self.closed = False
        self.post_calls = []
        self.response_queue = []

    async def post(self, url, json):
        self.post_calls.append((url, json))
        if self.response_queue:
            return self.response_queue.pop(0)
        return DummyResponse({"result": []})

    async def aclose(self):
        self.is_closed = True
        self.closed = True


class DummyClosableClient:
    """A simple closable object used to verify close() behavior."""

    def __init__(self):
        self.closed = False

    async def aclose(self):
        self.closed = True


class RecordingArcadeDBStorage(ArcadeDBStorage):
    """A test double that records SQL and returns predefined responses."""

    def __init__(self, responses=None):
        super().__init__(url="http://example.com", database="testdb", auth=None)
        self.responses = list(responses or [])
        self.commands = []

    async def _command(self, sql: str):
        self.commands.append(sql)
        if self.responses:
            return self.responses.pop(0)
        return []


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, ""),
        ("plain", "plain"),
        ("a'b", "a\\'b"),
        (r"a\\b", r"a\\\\b"),
    ],
)
def test_esc_escapes_single_quotes_and_backslashes(value, expected):
    """The SQL escaping helper must protect quotes and backslashes."""
    assert _esc(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, []),
        (["x"], ["x"]),
        ('["x", "y"]', ["x", "y"]),
        ('{"not": "a list"}', []),
        ("not-json", []),
        (123, []),
    ],
)
def test_parse_json_list_handles_supported_and_invalid_values(value, expected):
    """The parser must be permissive and never fail on malformed payloads."""
    assert _parse_json_list(value) == expected


def test_entity_to_attrs_serializes_lists_and_normalizes_empty_description():
    """Entity serialization should persist all list-like fields as JSON strings."""
    entity = Entity(
        id="node-1",
        entity_name="VLC",
        entity_type="Software",
        description=None,
        source_chunk_id=["chunk-1"],
        documents_id=["doc-1"],
        clusters=["cluster-a"],
    )

    attrs = _entity_to_attrs(entity)

    assert attrs["id"] == "node-1"
    assert attrs["entity_name"] == "VLC"
    assert attrs["entity_type"] == "Software"
    assert attrs["description"] == ""
    assert attrs["source_chunk_id"] == '["chunk-1"]'
    assert attrs["documents_id"] == '["doc-1"]'
    assert attrs["clusters"] == '["cluster-a"]'


def test_entity_from_row_handles_missing_and_invalid_values():
    """Entity deserialization should fall back to safe defaults."""
    row = {
        "id": "node-2",
        "entity_name": "Windows 11",
        "source_chunk_id": "not-json",
    }

    entity = _entity_from_row(row)

    assert entity.id == "node-2"
    assert entity.entity_name == "Windows 11"
    assert entity.entity_type == ""
    assert entity.description == ""
    assert entity.source_chunk_id == []
    assert entity.documents_id == []
    assert entity.clusters == []


def test_entity_helpers_roundtrip_preserves_serializable_fields():
    """Entity helpers should preserve all serializable fields."""
    entity = Entity(
        id="node-1",
        entity_name="VLC",
        entity_type="Software",
        description="Open source media player",
        source_chunk_id=["chunk-1"],
        documents_id=["doc-1"],
        clusters=["cluster-a"],
    )

    restored = _entity_from_row(_entity_to_attrs(entity))

    assert restored.id == entity.id
    assert restored.entity_name == entity.entity_name
    assert restored.entity_type == entity.entity_type
    assert restored.description == entity.description
    assert restored.source_chunk_id == entity.source_chunk_id
    assert restored.documents_id == entity.documents_id
    assert restored.clusters == entity.clusters


def test_relation_helper_uses_defaults_and_parses_collections():
    """Relation helper should parse correctly from row properties."""
    row = {
        "id": "rel-1",
        "subject_id": "source-node",
        "object_id": "target-node",
        "subject_name": "source-node",
        "object_name": "target-node",
        "description": "Works on the target OS",
        "relation_strength": "2.5",
        "source_chunk_id": '["chunk-7"]',
    }

    relation = _relation_from_row(row)

    assert relation.id == "rel-1"
    assert relation.subject_id == "source-node"
    assert relation.object_id == "target-node"
    assert relation.subject_name == "source-node"
    assert relation.object_name == "target-node"
    assert relation.relation_type == "UNKNOWN"
    assert relation.description == "Works on the target OS"
    assert relation.relation_strength == 2.5
    assert relation.source_chunk_id == ["chunk-7"]


@pytest.mark.asyncio
async def test_endpoint_builds_database_specific_command_url():
    """The adapter should build command URLs against the selected database."""
    storage = ArcadeDBStorage(url="http://localhost:2480/", database="tododb")
    assert storage._endpoint() == "http://localhost:2480/api/v1/command/tododb"


@pytest.mark.asyncio
async def test_get_client_creates_async_client_with_basic_auth_and_reuses_instance(monkeypatch):
    """The adapter should lazily create and cache its HTTP client."""
    created_clients = []

    def client_factory(*args, **kwargs):
        client = DummyAsyncClient(*args, **kwargs)
        created_clients.append(client)
        return client

    monkeypatch.setattr(adapter_module.httpx, "AsyncClient", client_factory)

    storage = ArcadeDBStorage(
        url="http://localhost:2480",
        database="tododb",
        auth=("root", "playwithdata"),
    )

    first = await storage._get_client()
    second = await storage._get_client()

    assert first is second
    assert len(created_clients) == 1
    assert created_clients[0].kwargs["timeout"] == 30.0
    assert isinstance(created_clients[0].kwargs["auth"], httpx.BasicAuth)


@pytest.mark.asyncio
async def test_get_client_recreates_closed_client(monkeypatch):
    """A closed HTTP client should be replaced by a fresh instance."""
    created_clients = []

    def client_factory(*args, **kwargs):
        client = DummyAsyncClient(*args, **kwargs)
        created_clients.append(client)
        return client

    monkeypatch.setattr(adapter_module.httpx, "AsyncClient", client_factory)

    storage = ArcadeDBStorage(auth=None)

    first = await storage._get_client()
    await first.aclose()
    second = await storage._get_client()

    assert first is not second
    assert len(created_clients) == 2
    assert created_clients[0].closed is True
    assert created_clients[1].kwargs["auth"] is None


@pytest.mark.asyncio
async def test_command_posts_sql_and_returns_result_rows(monkeypatch):
    """The command helper should send SQL and unwrap the result payload."""
    storage = ArcadeDBStorage(url="http://localhost:2480", database="tododb", auth=None)
    client = DummyAsyncClient()
    response = DummyResponse({"result": [{"count": 1}]})
    client.response_queue.append(response)

    async def fake_get_client():
        return client

    monkeypatch.setattr(storage, "_get_client", fake_get_client)

    rows = await storage._command("SELECT 1")

    assert rows == [{"count": 1}]
    assert response.raise_for_status_called is True
    assert client.post_calls == [
        (
            "http://localhost:2480/api/v1/command/tododb",
            {"language": "sql", "command": "SELECT 1"},
        )
    ]


@pytest.mark.asyncio
async def test_command_returns_empty_list_for_non_dict_payload(monkeypatch):
    """Unexpected response shapes should degrade to an empty result set."""
    storage = ArcadeDBStorage(auth=None)
    client = DummyAsyncClient()
    client.response_queue.append(DummyResponse([{"count": 1}]))

    async def fake_get_client():
        return client

    monkeypatch.setattr(storage, "_get_client", fake_get_client)

    assert await storage._command("SELECT 1") == []


@pytest.mark.asyncio
async def test_command_propagates_http_failures(monkeypatch):
    """Transport or HTTP-level failures should not be silently swallowed."""
    storage = ArcadeDBStorage(auth=None)
    client = DummyAsyncClient()
    client.response_queue.append(DummyResponse({}, error=RuntimeError("boom")))

    async def fake_get_client():
        return client

    monkeypatch.setattr(storage, "_get_client", fake_get_client)

    with pytest.raises(RuntimeError, match="boom"):
        await storage._command("SELECT broken")


@pytest.mark.asyncio
async def test_ensure_schema_creates_expected_types_properties_and_indexes():
    """Schema bootstrap should issue all required DDL statements."""
    storage = RecordingArcadeDBStorage()

    await storage._ensure_schema()

    assert len(storage.commands) == 19
    assert storage.commands[0] == "CREATE VERTEX TYPE RaguEntity IF NOT EXISTS"
    assert storage.commands[1] == "CREATE EDGE TYPE RaguRelation IF NOT EXISTS"
    assert "CREATE PROPERTY RaguEntity.id IF NOT EXISTS STRING" in storage.commands
    assert "CREATE PROPERTY RaguEntity.clusters IF NOT EXISTS STRING" in storage.commands
    assert "CREATE INDEX IF NOT EXISTS ON RaguEntity (id) UNIQUE" in storage.commands
    assert "CREATE PROPERTY RaguRelation.subject_id IF NOT EXISTS STRING" in storage.commands
    assert "CREATE PROPERTY RaguRelation.object_id IF NOT EXISTS STRING" in storage.commands
    assert "CREATE PROPERTY RaguRelation.relation_strength IF NOT EXISTS DOUBLE" in storage.commands


@pytest.mark.asyncio
async def test_ensure_schema_swallows_internal_errors():
    """Schema bootstrap is intentionally best-effort and must not bubble failures."""

    class FailingArcadeDBStorage(RecordingArcadeDBStorage):
        async def _command(self, sql: str):
            self.commands.append(sql)
            raise RuntimeError("ddl failure")

    storage = FailingArcadeDBStorage()

    await storage._ensure_schema()

    assert storage.commands == ["CREATE VERTEX TYPE RaguEntity IF NOT EXISTS"]


@pytest.mark.asyncio
async def test_index_start_callback_delegates_to_schema_creation(monkeypatch):
    """The index start hook should initialize the graph schema."""
    storage = ArcadeDBStorage(auth=None)
    called = {"value": False}

    async def fake_ensure_schema():
        called["value"] = True

    monkeypatch.setattr(storage, "_ensure_schema", fake_ensure_schema)

    await storage.index_start_callback()

    assert called["value"] is True


@pytest.mark.asyncio
async def test_index_done_and_query_done_callbacks_are_noops():
    """Optional lifecycle hooks currently do nothing and should stay harmless."""
    storage = ArcadeDBStorage(auth=None)

    assert await storage.index_done_callback() is None
    assert await storage.query_done_callback() is None


@pytest.mark.asyncio
async def test_edges_degrees_returns_counts_for_each_edge_spec():
    """Edge degree queries should preserve input ordering and default missing counts to zero."""
    storage = RecordingArcadeDBStorage(responses=[[{"count": 2}], []])

    degrees = await storage.edges_degrees(
        [
            ("node-a", "node-b", "rel-1"),
            ("node-c", "node-d", None),
        ]
    )

    assert degrees == [2, 0]
    assert "subject_id IN ['node-a', 'node-b']" in storage.commands[0]
    assert "subject_id IN ['node-c', 'node-d']" in storage.commands[1]


@pytest.mark.asyncio
async def test_get_nodes_returns_entity_and_none_for_missing_node():
    """The adapter should map database rows to entities and preserve misses as None."""
    storage = RecordingArcadeDBStorage(
        responses=[
            [
                {
                    "id": "node-1",
                    "entity_name": "VLC",
                    "entity_type": "Software",
                    "description": "Player",
                    "source_chunk_id": '["chunk-1"]',
                    "documents_id": '["doc-1"]',
                    "clusters": '["cluster-1"]',
                }
            ],
            [],
        ]
    )

    nodes = await storage.get_nodes(["node-1", "missing-node"])

    assert nodes[0] is not None
    assert nodes[0].entity_name == "VLC"
    assert nodes[1] is None
    assert "WHERE id = 'node-1'" in storage.commands[0]
    assert "WHERE id = 'missing-node'" in storage.commands[1]


@pytest.mark.asyncio
async def test_upsert_nodes_generates_upsert_sql_with_serialized_lists():
    """Node upsert must serialize list fields and use UPSERT by id."""
    storage = RecordingArcadeDBStorage()
    node = Entity(
        id="node-1",
        entity_name="VLC",
        entity_type="Software",
        description="Open source",
        source_chunk_id=["chunk-1", "chunk-2"],
        documents_id=["doc-1"],
        clusters=[],
    )

    await storage.upsert_nodes([node])

    sql = storage.commands[0]
    assert "UPDATE RaguEntity SET" in sql
    assert "UPSERT WHERE id = 'node-1'" in sql
    assert "source_chunk_id = '[\"chunk-1\", \"chunk-2\"]'" in sql
    assert "documents_id = '[\"doc-1\"]'" in sql
    assert "clusters = '[]'" in sql


@pytest.mark.asyncio
async def test_delete_nodes_generates_delete_sql_for_each_node():
    """Node deletion should emit one delete statement per identifier."""
    storage = RecordingArcadeDBStorage()

    await storage.delete_nodes(["node-a", "node-b"])

    assert storage.commands == [
        "DELETE FROM RaguEntity WHERE id = 'node-a'",
        "DELETE FROM RaguEntity WHERE id = 'node-b'",
    ]


@pytest.mark.asyncio
async def test_get_edges_maps_relation_and_filters_by_relation_id_when_present():
    """Edge lookup should optionally include the relation id filter."""
    storage = RecordingArcadeDBStorage(
        responses=[[
            {
                "id": "rel-1",
                "subject_id": "node-a",
                "object_id": "node-b",
                "subject_name": "VLC",
                "object_name": "Windows 11",
                "relation_type": "WORKS_ON",
                "description": "Supported officially",
                "relation_strength": 1.0,
                "source_chunk_id": '["chunk-1"]',
            }
        ]]
    )

    edges = await storage.get_edges([("node-a", "node-b", "rel-1")])

    assert edges[0] is not None
    assert edges[0].subject_id == "node-a"
    assert edges[0].object_id == "node-b"
    assert edges[0].relation_type == "WORKS_ON"
    assert "AND id = 'rel-1'" in storage.commands[0]


@pytest.mark.asyncio
async def test_get_edges_without_relation_id_does_not_append_id_filter():
    """Lookup without a relation id should only filter by endpoints."""
    storage = RecordingArcadeDBStorage(responses=[[]])

    await storage.get_edges([("node-a", "node-b", None)])

    assert storage.commands[0] == (
        "SELECT FROM RaguRelation WHERE subject_id = 'node-a' AND object_id = 'node-b'"
    )


@pytest.mark.asyncio
async def test_upsert_edges_deletes_previous_variants_and_creates_new_edge():
    """Edge upsert must remove conflicting edges before creating a new one."""
    storage = RecordingArcadeDBStorage()
    edge = Relation(
        id="rel-1",
        subject_id="node-a",
        object_id="node-b",
        subject_name="VLC",
        object_name="Windows 11",
        relation_type="WORKS_ON",
        description="Supported officially",
        relation_strength=1.25,
        source_chunk_id=["chunk-1"],
    )

    await storage.upsert_edges([edge])

    delete_sql, create_sql = storage.commands
    assert "DELETE FROM RaguRelation" in delete_sql
    assert "subject_id = 'node-a'" in delete_sql
    assert "object_id = 'node-b'" in delete_sql
    assert "relation_type = 'WORKS_ON'" in delete_sql
    assert "CREATE EDGE RaguRelation" in create_sql
    assert "FROM (SELECT FROM RaguEntity WHERE id = 'node-a')" in create_sql
    assert "TO (SELECT FROM RaguEntity WHERE id = 'node-b')" in create_sql
    assert "relation_strength = 1.25" in create_sql
    assert "source_chunk_id = '[\"chunk-1\"]'" in create_sql


@pytest.mark.asyncio
async def test_delete_edges_supports_optional_relation_id_filter():
    """Edge deletion should optionally narrow the delete by relation id."""
    storage = RecordingArcadeDBStorage()

    await storage.delete_edges(
        [
            ("node-a", "node-b", None),
            ("node-c", "node-d", "rel-2"),
        ]
    )

    assert storage.commands == [
        "DELETE FROM RaguRelation WHERE subject_id = 'node-a' AND object_id = 'node-b'",
        "DELETE FROM RaguRelation WHERE subject_id = 'node-c' AND object_id = 'node-d' AND id = 'rel-2'",
    ]


@pytest.mark.asyncio
async def test_get_all_edges_for_nodes_maps_identifiers_correctly():
    """Expanded edge traversal should fetch relations based on flat subject_id or object_id."""
    storage = RecordingArcadeDBStorage(
        responses=[[
            {
                "id": "rel-1",
                "subject_id": "node-a",
                "object_id": "node-b",
                "subject_name": "VLC",
                "object_name": "Windows 11",
                "relation_type": "WORKS_ON",
                "description": "Supported officially",
                "relation_strength": 1.0,
                "source_chunk_id": '["chunk-1"]',
            },
        ]]
    )

    result = await storage.get_all_edges_for_nodes(["node-a"])

    assert len(result) == 1
    assert len(result[0]) == 1
    assert result[0][0].subject_id == "node-a"
    assert result[0][0].object_id == "node-b"
    assert storage.commands[0] == (
        "SELECT FROM RaguRelation WHERE subject_id = 'node-a' OR object_id = 'node-a'"
    )


@pytest.mark.asyncio
async def test_get_all_nodes_returns_all_entities():
    """Full node scans should map every raw row to an Entity instance."""
    storage = RecordingArcadeDBStorage(
        responses=[[
            {
                "id": "node-1",
                "entity_name": "VLC",
                "entity_type": "Software",
                "description": "Player",
                "source_chunk_id": '["chunk-1"]',
                "documents_id": '["doc-1"]',
                "clusters": '[]',
            },
            {
                "id": "node-2",
                "entity_name": "Windows 11",
                "entity_type": "OS",
                "description": "Operating system",
                "source_chunk_id": '["chunk-2"]',
                "documents_id": '["doc-2"]',
                "clusters": '["cluster-a"]',
            },
        ]]
    )

    nodes = await storage.get_all_nodes()

    assert [node.id for node in nodes] == ["node-1", "node-2"]
    assert [node.entity_name for node in nodes] == ["VLC", "Windows 11"]
    assert storage.commands == ["SELECT FROM RaguEntity"]


@pytest.mark.asyncio
async def test_get_all_edges_returns_all_relations():
    """Full edge scans should return relations built from properties."""
    storage = RecordingArcadeDBStorage(
        responses=[[
            {
                "id": "rel-1",
                "subject_id": "node-a",
                "object_id": "node-b",
                "subject_name": "VLC",
                "object_name": "Windows 11",
                "relation_type": "WORKS_ON",
                "description": "Supported officially",
                "relation_strength": 1.0,
                "source_chunk_id": '["chunk-1"]',
            },
            {
                "id": "rel-2",
                "subject_id": "node-c",
                "object_id": "node-d",
                "subject_name": "Qt",
                "object_name": "Linux",
                "relation_type": "RUNS_ON",
                "description": "Another relation",
                "relation_strength": 2.0,
                "source_chunk_id": '["chunk-2"]',
            },
        ]]
    )

    edges = await storage.get_all_edges()

    assert [edge.id for edge in edges] == ["rel-1", "rel-2"]
    assert edges[0].subject_id == "node-a"
    assert edges[0].object_id == "node-b"
    assert edges[1].subject_id == "node-c"
    assert edges[1].object_id == "node-d"
    assert storage.commands == ["SELECT FROM RaguRelation"]


@pytest.mark.asyncio
async def test_close_closes_existing_client_and_resets_state():
    """The adapter should close its HTTP client and drop the reference."""
    storage = ArcadeDBStorage(auth=None)
    client = DummyClosableClient()
    storage._client = client

    await storage.close()

    assert client.closed is True
    assert storage._client is None


@pytest.mark.asyncio
async def test_close_is_a_noop_when_client_was_never_created():
    """Closing an unused adapter should not fail."""
    storage = ArcadeDBStorage(auth=None)

    await storage.close()

    assert storage._client is None
