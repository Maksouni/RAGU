import json
import os
import re
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, List, Union

import uvicorn
from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, HTTPException
from pydantic import BaseModel

from ragu import (
    ArtifactsExtractorLLM,
    BuilderArguments,
    GlobalSearchEngine,
    KnowledgeGraph,
    LocalSearchEngine,
    Settings,
    SimpleChunker,
)
from ragu.embedder import OpenAIEmbedder
from ragu.llm import OpenAIClient
from ragu.storage.graph_storage_adapters.memgraph_adapter import MemgraphStorage
from ragu.storage.index import StorageArguments

# Load .env from repo root for local runs.
load_dotenv(Path(__file__).resolve().parents[2] / ".env", override=False)


class AppState:
    knowledge_graph: KnowledgeGraph = None
    local_search_engine: LocalSearchEngine = None
    global_search_engine: GlobalSearchEngine = None
    is_indexing: bool = False


state = AppState()


class QueryRequest(BaseModel):
    question: str


class JsonIngestRequest(BaseModel):
    data: List[Union[Dict[str, Any], str]]


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _tokenize(text: str) -> List[str]:
    return [t for t in re.findall(r"\w+", text.lower()) if len(t) >= 3]


def _match_score(content: str, tokens: List[str]) -> int:
    lowered = content.lower()
    return sum(1 for token in tokens if token in lowered)


async def build_no_llm_answer(question: str) -> str:
    if not state.knowledge_graph:
        return "Режим без LLM включен, но граф еще не инициализирован."

    backend = state.knowledge_graph.index.graph_backend
    nodes = await backend.get_all_nodes()
    edges = await backend.get_all_edges()
    tokens = _tokenize(question)

    node_candidates = []
    for node in nodes:
        haystack = f"{node.entity_name} {node.entity_type} {node.description}"
        score = _match_score(haystack, tokens)
        if score > 0:
            node_candidates.append((score, node))
    node_candidates.sort(key=lambda x: x[0], reverse=True)

    edge_candidates = []
    for edge in edges:
        haystack = (
            f"{edge.subject_name} {edge.object_name} {edge.relation_type} {edge.description}"
        )
        score = _match_score(haystack, tokens)
        if score > 0:
            edge_candidates.append((score, edge))
    edge_candidates.sort(key=lambda x: x[0], reverse=True)

    lines = [
        "Режим без LLM включен.",
        f"В графе: {len(nodes)} сущностей, {len(edges)} связей.",
    ]

    if node_candidates:
        lines.append("Наиболее релевантные сущности:")
        for _, node in node_candidates[:5]:
            lines.append(f"- {node.entity_name} [{node.entity_type}] (id={node.id})")
    else:
        lines.append("По вопросу не найдено явных совпадений по сущностям.")

    if edge_candidates:
        lines.append("Наиболее релевантные связи:")
        for _, edge in edge_candidates[:5]:
            lines.append(
                f"- {edge.subject_name} -[{edge.relation_type}]-> {edge.object_name} (id={edge.id})"
            )

    lines.append(
        "Для генеративного ответа отключите флаг DISABLE_LLM_ANSWERS в .env."
    )
    return "\n".join(lines)


@asynccontextmanager
async def lifespan(app: FastAPI):
    client = OpenAIClient(
        model_name=os.getenv("LLM_MODEL_NAME"),
        base_url=os.getenv("BASE_URL"),
        api_token=os.getenv("API_KEY"),
    )
    embedder = OpenAIEmbedder(
        model_name=os.getenv("EMBEDDER_MODEL_NAME"),
        base_url=os.getenv("EMBEDDING_BASE_URL"),
        api_token=os.getenv("API_KEY"),
        dim=768,
    )

    Settings.storage_folder = "ragu_working_dir/service_graph"
    Settings.language = "russian"

    storage_args = StorageArguments(
        graph_backend_storage=MemgraphStorage,
        graph_storage_kwargs={
            "uri": os.getenv("MEMGRAPH_URI", "bolt://memgraph:7687"),
            "database": os.getenv("MEMGRAPH_DATABASE") or None,
            "username": os.getenv("MEMGRAPH_USERNAME") or None,
            "password": os.getenv("MEMGRAPH_PASSWORD") or None,
        },
    )

    state.knowledge_graph = KnowledgeGraph(
        client=client,
        embedder=embedder,
        chunker=SimpleChunker(max_chunk_size=1000),
        artifact_extractor=ArtifactsExtractorLLM(client=client, do_validation=False),
        builder_settings=BuilderArguments(use_llm_summarization=True, vectorize_chunks=True),
        storage_settings=storage_args,
    )
    await state.knowledge_graph.index.graph_backend.index_start_callback()

    state.local_search_engine = LocalSearchEngine(client, state.knowledge_graph, embedder)
    state.global_search_engine = GlobalSearchEngine(client, state.knowledge_graph)

    print("GraphRAG Server ready.")
    yield


app = FastAPI(title="GraphRAG JSON Service", lifespan=lifespan)


async def run_indexing(docs: List[str], source_desc: str):
    state.is_indexing = True
    try:
        print(f"Indexing {len(docs)} docs from {source_desc}...")
        await state.knowledge_graph.build_from_docs(docs)
        print(f"Indexing from {source_desc} finished.")
    except Exception as e:
        print(f"Indexing error: {e}")
    finally:
        state.is_indexing = False


@app.post("/ask/local")
async def ask_local(request: QueryRequest):
    if _env_flag("DISABLE_LLM_ANSWERS", False):
        answer = await build_no_llm_answer(request.question)
        return {"answer": answer, "mode": "no_llm"}

    if not state.local_search_engine:
        raise HTTPException(status_code=503, detail="Search engine not ready")
    answer = await state.local_search_engine.a_query(request.question)
    return {"answer": answer, "mode": "llm"}


@app.post("/ask/global")
async def ask_global(request: QueryRequest):
    if _env_flag("DISABLE_LLM_ANSWERS", False):
        answer = await build_no_llm_answer(request.question)
        return {"answer": answer, "mode": "no_llm"}

    if not state.global_search_engine:
        raise HTTPException(status_code=503, detail="Search engine not ready")
    answer = await state.global_search_engine.a_query(request.question)
    return {"answer": answer, "mode": "llm"}


@app.post("/ingest/json")
async def ingest_json(request: JsonIngestRequest, bg_tasks: BackgroundTasks):
    if state.is_indexing:
        raise HTTPException(status_code=409, detail="Indexer is busy")

    processed_docs: List[str] = []
    for item in request.data:
        plain_text = item if isinstance(item, str) else json.dumps(item, ensure_ascii=False)
        if plain_text.strip():
            processed_docs.append(plain_text)

    if not processed_docs:
        raise HTTPException(status_code=400, detail="No valid data to ingest")

    bg_tasks.add_task(run_indexing, processed_docs, "API JSON payload")
    return {"status": "accepted", "count": len(processed_docs)}


@app.get("/status")
async def status():
    return {"is_indexing": state.is_indexing}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
