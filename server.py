import os
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Union

from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
import uvicorn

from ragu import (
    ArtifactsExtractorLLM,
    BuilderArguments,
    KnowledgeGraph,
    LocalSearchEngine,
    Settings,
    SimpleChunker,
)
from ragu.embedder import OpenAIEmbedder
from ragu.llm import OpenAIClient
from ragu.utils.ragu_utils import read_text_from_files
from ragu.storage.index import StorageArguments
from arcadedb_adapter import ArcadeDBStorage

class AppState:
    knowledge_graph: KnowledgeGraph = None
    search_engine: LocalSearchEngine = None
    is_indexing: bool = False

state = AppState()

def json_to_text(data: Union[Dict, List, Any]) -> str:
    """
    Converts raw JSON into readable text, 
    from which LLM can easily extract entities and relationships.
    """
    if isinstance(data, list):
        return "\n\n---\n\n".join([json_to_text(item) for item in data])

    if not isinstance(data, dict):
        return str(data)

    lines = []
    
    main_title = data.get("name", data.get("title", data.get("package", data.get("id", "Unknown Entity"))))
    lines.append(f"Object: {main_title}")

    for key, value in data.items():
        if key in ["name", "title", "package", "id"]:
            continue
        
        clean_key = str(key).replace("_", " ").title()

        if isinstance(value, list):
            joined_vals = ", ".join([str(v) for v in value if v is not None])
            lines.append(f"- {clean_key}: {joined_vals}")
        elif isinstance(value, dict):
            lines.append(f"- {clean_key}:")
            for sub_key, sub_val in value.items():
                if sub_val is not None:
                    lines.append(f"  * {str(sub_key).replace('_', ' ').title()}: {sub_val}")
        else:
            if value is not None:
                lines.append(f"- {clean_key}: {value}")

    return "\n".join(lines)

# --- Models ---

class QueryRequest(BaseModel):
    question: str

class IngestRequest(BaseModel):
    folder_path: str = "data/lit"

class JsonIngestRequest(BaseModel):
    # Accepts a list of dictionaries (JSON objects) or raw strings
    data: List[Union[Dict[str, Any], str]]

# --- Lifecycle ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    api_key = os.getenv("API_KEY")
    base_url = os.getenv("BASE_URL")
    embed_base_url = os.getenv("EMBEDDING_BASE_URL")
    llm_model = os.getenv("LLM_MODEL_NAME")
    embed_model = os.getenv("EMBEDDER_MODEL_NAME")    

    storage_args = StorageArguments(
        graph_backend_storage=ArcadeDBStorage, 
        graph_storage_kwargs={
            "url": "http://knb:2480",
            "database": "tododb", 
            "auth": ("root", "playwithdata")
        }
    )

    Settings.storage_folder = "ragu_working_dir/service_graph"
    Settings.language = "russian"

    client = OpenAIClient(
        model_name=llm_model,
        base_url=base_url,
        api_token=api_key,
        max_requests_per_second=1,
        max_requests_per_minute=60,
        cache_flush_every=10,
        request_timeout=120
    )

    embedder = OpenAIEmbedder(
        model_name=embed_model,
        base_url=embed_base_url,
        api_token=api_key,
        dim=768,
        max_requests_per_second=1,
        max_requests_per_minute=60,
        use_cache=True,
    )

    state.knowledge_graph = KnowledgeGraph(
        client=client,
        embedder=embedder,
        chunker=SimpleChunker(max_chunk_size=1000),
        artifact_extractor=ArtifactsExtractorLLM(
            client=client, 
            do_validation=False, 
        ),
        builder_settings=BuilderArguments(use_llm_summarization=True, vectorize_chunks=True),
        storage_settings=storage_args
    )

    # Ensures database schema and indices are initialized before processing
    await state.knowledge_graph.index.graph_backend.index_start_callback()

    state.search_engine = LocalSearchEngine(
        client,
        state.knowledge_graph,
        embedder,
        tokenizer_model="gpt-4o-mini",
    )
    
    print("GraphRAG Server ready.")
    yield

app = FastAPI(title="GraphRAG JSON Service", lifespan=lifespan)

# --- Background Task Logic ---

async def run_indexing(docs: List[str], source_desc: str):
    """
    Handles the heavy lifting of embedding and graph construction.
    Locked by state.is_indexing to prevent VRAM overflow.
    """
    state.is_indexing = True
    try:
        print(f"Indexing {len(docs)} docs from {source_desc}...")
        await state.knowledge_graph.build_from_docs(docs)
        print(f"Indexing from {source_desc} finished.")
    except Exception as e:
        print(f"Indexing error: {e}")
    finally:
        state.is_indexing = False

# --- Endpoints ---

@app.post("/ask")
async def ask(request: QueryRequest):
    if not state.search_engine:
        raise HTTPException(status_code=503, detail="Search engine not ready")
    answer = await state.search_engine.a_query(request.question)
    return {"answer": answer}

@app.post("/ingest/json")
async def ingest_json(request: JsonIngestRequest, bg_tasks: BackgroundTasks):
    if state.is_indexing:
        raise HTTPException(status_code=409, detail="Indexer is busy")

    processed_docs = []
    for item in request.data:
        readable_text = json_to_text(item)
        
        if readable_text.strip():
            processed_docs.append(readable_text)
    
    if not processed_docs:
        raise HTTPException(status_code=400, detail="No valid data to ingest")
        
    bg_tasks.add_task(run_indexing, processed_docs, "API JSON Payload")
    return {"status": "accepted", "count": len(processed_docs)}

@app.post("/ingest/folder")
async def ingest_folder(request: IngestRequest, bg_tasks: BackgroundTasks):
    if state.is_indexing:
        raise HTTPException(status_code=409, detail="Indexer is busy")

    docs = read_text_from_files(request.folder_path)
    if not docs:
        raise HTTPException(status_code=404, detail="No files found")
        
    bg_tasks.add_task(run_indexing, docs, f"Folder: {request.folder_path}")
    return {"status": "accepted", "count": len(docs)}

@app.get("/status")
async def status():
    return {"is_indexing": state.is_indexing}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)