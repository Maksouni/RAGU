import json
import os
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Union

import uvicorn
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
from ragu.storage.graph_storage_adapters.arcadedb_adapter import ArcadeDBStorage
from ragu.storage.index import StorageArguments


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
        graph_backend_storage=ArcadeDBStorage,
        graph_storage_kwargs={
            "url": "http://knb:2480",
            "database": "tododb",
            "auth": ("root", "playwithdata"),
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
    if not state.local_search_engine:
        raise HTTPException(status_code=503, detail="Search engine not ready")
    answer = await state.local_search_engine.a_query(request.question)
    return {"answer": answer}


@app.post("/ask/global")
async def ask_global(request: QueryRequest):
    if not state.global_search_engine:
        raise HTTPException(status_code=503, detail="Search engine not ready")
    answer = await state.global_search_engine.a_query(request.question)
    return {"answer": answer}


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