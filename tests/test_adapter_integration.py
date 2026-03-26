import os
import uuid

import pytest
import pytest_asyncio

from ragu.graph.types import Entity, Relation
from ragu.storage.graph_storage_adapters.arcadedb_adapter import ArcadeDBStorage


pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


@pytest.fixture
def integration_enabled():
    """Integration tests are opt-in because they require a live ArcadeDB instance."""
    if os.getenv("RUN_ARCADEDB_INTEGRATION") != "1":
        pytest.skip("Set RUN_ARCADEDB_INTEGRATION=1 to run live ArcadeDB tests.")


@pytest_asyncio.fixture
async def storage(integration_enabled):
    """Create a live storage adapter connected to a local ArcadeDB instance."""
    instance = ArcadeDBStorage(
        url=os.getenv("ARCADEDB_URL", "http://localhost:2480"),
        database=os.getenv("ARCADEDB_DATABASE", "tododb"),
        auth=(
            os.getenv("ARCADEDB_USER", "root"),
            os.getenv("ARCADEDB_PASSWORD", "playwithdata"),
        ),
    )
    await instance.index_start_callback()
    try:
        yield instance
    finally:
        await instance.close()


async def _delete_graph(storage, node_ids, edge_specs):
    """Best-effort cleanup keeps the test database reusable across runs."""
    if edge_specs:
        await storage.delete_edges(edge_specs)
    if node_ids:
        await storage.delete_nodes(node_ids)





async def test_live_node_upsert_updates_existing_vertex_payload(storage):
    """UPSERT on the same node id should replace the stored payload."""
    suffix = uuid.uuid4().hex[:8]
    node_id = f"node-{suffix}"

    first = Entity(
        id=node_id,
        entity_name="VLC",
        entity_type="Software",
        description="Original description",
        source_chunk_id=["chunk-1"],
        documents_id=["doc-1"],
        clusters=[],
    )
    updated = Entity(
        id=node_id,
        entity_name="VLC media player",
        entity_type="Desktop App",
        description="Updated description",
        source_chunk_id=["chunk-2"],
        documents_id=["doc-2"],
        clusters=["cluster-a"],
    )

    try:
        await storage.upsert_nodes([first])
        await storage.upsert_nodes([updated])

        nodes = await storage.get_nodes([node_id])
        assert nodes[0] is not None
        assert nodes[0].entity_name == "VLC media player"
        assert nodes[0].entity_type == "Desktop App"
        assert nodes[0].description == "Updated description"
        assert nodes[0].source_chunk_id == ["chunk-2"]
        assert nodes[0].documents_id == ["doc-2"]
        assert nodes[0].clusters == ["cluster-a"]
    finally:
        await _delete_graph(storage, [node_id], [])


async def test_live_edge_upsert_replaces_same_triplet_and_relation_type(storage):
    """Edge upsert should remove older edges for the same endpoints and relation type."""
    suffix = uuid.uuid4().hex[:8]
    subject_id = f"subject-{suffix}"
    object_id = f"object-{suffix}"
    first_relation_id = f"rel-first-{suffix}"
    second_relation_id = f"rel-second-{suffix}"

    subject = Entity(
        id=subject_id,
        entity_name="Qt",
        entity_type="Framework",
        description="Cross-platform framework",
        source_chunk_id=["chunk-1"],
        documents_id=["doc-1"],
        clusters=[],
    )
    target = Entity(
        id=object_id,
        entity_name="Linux",
        entity_type="OS",
        description="Operating system",
        source_chunk_id=["chunk-1"],
        documents_id=["doc-1"],
        clusters=[],
    )
    first_relation = Relation(
        id=first_relation_id,
        subject_id=subject_id,
        object_id=object_id,
        subject_name="Qt",
        object_name="Linux",
        relation_type="RUNS_ON",
        description="Initial relation",
        relation_strength=1.0,
        source_chunk_id=["chunk-1"],
    )
    second_relation = Relation(
        id=second_relation_id,
        subject_id=subject_id,
        object_id=object_id,
        subject_name="Qt",
        object_name="Linux",
        relation_type="RUNS_ON",
        description="Updated relation",
        relation_strength=2.0,
        source_chunk_id=["chunk-2"],
    )

    try:
        await storage.upsert_nodes([subject, target])
        await storage.upsert_edges([first_relation])
        await storage.upsert_edges([second_relation])

        old_edge = await storage.get_edges([(subject_id, object_id, first_relation_id)])
        new_edge = await storage.get_edges([(subject_id, object_id, second_relation_id)])
        all_edges = await storage.get_all_edges_for_nodes([subject_id])

        matching_edges = [
            edge
            for edge in all_edges[0]
            if edge.object_id == object_id and edge.relation_type == "RUNS_ON"
        ]

        assert old_edge[0] is None
        assert new_edge[0] is not None
        assert new_edge[0].description == "Updated relation"
        assert new_edge[0].relation_strength == 2.0
        assert len(matching_edges) == 1
        assert matching_edges[0].id == second_relation_id
    finally:
        await _delete_graph(
            storage,
            [subject_id, object_id],
            [
                (subject_id, object_id, first_relation_id),
                (subject_id, object_id, second_relation_id),
            ],
        )


async def test_live_get_all_nodes_and_edges_exposes_created_records(storage):
    """Full scans should include records created by the current test run."""
    suffix = uuid.uuid4().hex[:8]
    subject_id = f"scanner-subject-{suffix}"
    object_id = f"scanner-object-{suffix}"
    relation_id = f"scanner-rel-{suffix}"

    subject = Entity(
        id=subject_id,
        entity_name="Python",
        entity_type="Language",
        description="Programming language",
        source_chunk_id=["chunk-1"],
        documents_id=["doc-1"],
        clusters=[],
    )
    target = Entity(
        id=object_id,
        entity_name="FastAPI",
        entity_type="Framework",
        description="Web framework",
        source_chunk_id=["chunk-1"],
        documents_id=["doc-1"],
        clusters=[],
    )
    relation = Relation(
        id=relation_id,
        subject_id=subject_id,
        object_id=object_id,
        subject_name="Python",
        object_name="FastAPI",
        relation_type="USED_WITH",
        description="Common pairing",
        relation_strength=1.0,
        source_chunk_id=["chunk-1"],
    )

    try:
        await storage.upsert_nodes([subject, target])
        await storage.upsert_edges([relation])

        all_nodes = await storage.get_all_nodes()
        all_edges = await storage.get_all_edges()

        assert any(node.id == subject_id for node in all_nodes)
        assert any(node.id == object_id for node in all_nodes)
        assert any(edge.id == relation_id for edge in all_edges)
    finally:
        await _delete_graph(storage, [subject_id, object_id], [(subject_id, object_id, relation_id)])