import json
import os
import re
from contextlib import asynccontextmanager
from hashlib import md5
from pathlib import Path
from typing import Any, Dict, List, Literal, Union

import uvicorn
from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, HTTPException
from openai import AsyncOpenAI
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
from ragu.graph.types import Entity, Relation
from ragu.llm import OpenAIClient
from ragu.storage.graph_storage_adapters.memgraph_adapter import MemgraphStorage
from ragu.storage.index import StorageArguments

# Load .env from repo root for local runs.
# Use override=True so explicit project config wins over stale shell vars.
load_dotenv(Path(__file__).resolve().parents[2] / ".env", override=True)

AnswerMode = Literal["auto", "llm", "no_llm"]
EMBEDDING_TEXT_LIMIT = 1800


class AppState:
    knowledge_graph: KnowledgeGraph = None
    local_search_engine: LocalSearchEngine = None
    global_search_engine: GlobalSearchEngine = None
    raw_llm_client: AsyncOpenAI = None
    is_indexing: bool = False
    embedding_dim: int = 20


state = AppState()


class QueryRequest(BaseModel):
    question: str
    answer_mode: AnswerMode = "auto"


class JsonIngestRequest(BaseModel):
    data: List[Union[Dict[str, Any], str]]


class BeautifyAnswerRequest(BaseModel):
    question: str
    structured_answer: str


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _llm_enabled(answer_mode: AnswerMode) -> bool:
    if answer_mode == "llm":
        return True
    if answer_mode == "no_llm":
        return False
    return not _env_flag("DISABLE_LLM_ANSWERS", False)


def _tokenize(text: str) -> List[str]:
    return [t for t in re.findall(r"\w+", text.lower()) if len(t) >= 3]


def _match_score(content: str, tokens: List[str]) -> int:
    lowered = content.lower()
    return sum(1 for token in tokens if token in lowered)


def _stable_id(prefix: str, text: str) -> str:
    return f"{prefix}_{md5(text.encode('utf-8')).hexdigest()[:16]}"


def _for_embedding(text: str, limit: int = EMBEDDING_TEXT_LIMIT) -> str:
    normalized = " ".join((text or "").split())
    if len(normalized) <= limit:
        return normalized
    return normalized[:limit].rstrip() + " ..."


def _parse_doc(doc: str) -> Dict[str, Any] | None:
    try:
        parsed = json.loads(doc)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _artifact_nodes_from_answer(event_id: str, answer: str) -> tuple[list[Entity], list[Relation]]:
    nodes: list[Entity] = []
    relations: list[Relation] = []
    for line in answer.splitlines():
        if "->" not in line:
            continue
        left, url = [part.strip() for part in line.split("->", 1)]
        if not left or not url.startswith(("http://", "https://")):
            continue
        node_id = _stable_id("artifact", f"{left}|{url}")
        nodes.append(
            Entity(
                id=node_id,
                entity_name=left[:120],
                entity_type="PackageArtifact",
                description=_for_embedding(f"Пакетный артефакт: {left}. URL: {url}"),
                source_chunk_id=[event_id],
                documents_id=[event_id],
                clusters=[],
            )
        )
        relations.append(
            Relation(
                subject_id=event_id,
                object_id=node_id,
                subject_name="ask_exchange",
                object_name=left[:120],
                relation_type="FOUND_ARTIFACT",
                description=_for_embedding(f"Запрос вернул артефакт {left}: {url}"),
                source_chunk_id=[event_id],
            )
        )
    return nodes, relations


def build_fast_graph_from_docs(docs: List[str]) -> tuple[List[Entity], List[Relation]]:
    entities: list[Entity] = []
    relations: list[Relation] = []

    for idx, doc in enumerate(docs):
        normalized = " ".join(doc.split())
        parsed = _parse_doc(doc)
        if parsed and parsed.get("event_type") == "ask_exchange":
            event_id = str(parsed.get("event_id") or _stable_id("event", normalized))
            question = str(parsed.get("question") or "")
            answer = str(parsed.get("answer") or "")
            mode = str(parsed.get("mode") or "local")
            metadata = parsed.get("metadata") if isinstance(parsed.get("metadata"), dict) else {}

            question_id = _stable_id("question", question)
            answer_id = _stable_id("answer", f"{question}|{answer}")
            event_description = _for_embedding(
                f"Обмен вопрос-ответ. Режим поиска: {mode}. "
                f"Режим ответа: {metadata.get('answer_mode') or metadata.get('requested_answer_mode') or 'auto'}. "
                f"Вопрос: {question}. Ответ: {answer}"
            )
            entities.extend(
                [
                    Entity(
                        id=event_id,
                        entity_name=f"ask_exchange:{event_id[:8]}",
                        entity_type="AskExchange",
                        description=event_description,
                        source_chunk_id=[event_id],
                        documents_id=[event_id],
                        clusters=[],
                    ),
                    Entity(
                        id=question_id,
                        entity_name=question[:120] or "empty question",
                        entity_type="UserQuery",
                        description=_for_embedding(f"Пользовательский запрос: {question}"),
                        source_chunk_id=[event_id],
                        documents_id=[event_id],
                        clusters=[],
                    ),
                    Entity(
                        id=answer_id,
                        entity_name=f"answer:{event_id[:8]}",
                        entity_type="StructuredAnswer",
                        description=_for_embedding(answer),
                        source_chunk_id=[event_id],
                        documents_id=[event_id],
                        clusters=[],
                    ),
                ]
            )
            relations.extend(
                [
                    Relation(
                        subject_id=event_id,
                        object_id=question_id,
                        subject_name=f"ask_exchange:{event_id[:8]}",
                        object_name=question[:120] or "empty question",
                        relation_type="HAS_QUESTION",
                        description="Событие обработки содержит пользовательский запрос.",
                        source_chunk_id=[event_id],
                    ),
                    Relation(
                        subject_id=question_id,
                        object_id=answer_id,
                        subject_name=question[:120] or "empty question",
                        object_name=f"answer:{event_id[:8]}",
                        relation_type="ANSWERED_BY",
                        description="Запрос связан со сформированным ответом.",
                        source_chunk_id=[event_id],
                    ),
                ]
            )
            artifact_nodes, artifact_relations = _artifact_nodes_from_answer(event_id, answer)
            entities.extend(artifact_nodes)
            relations.extend(artifact_relations)
            continue

        doc_id = _stable_id("doc", f"{idx}|{normalized}")
        entities.append(
            Entity(
                id=doc_id,
                entity_name=normalized[:80] if normalized else f"doc_{idx}",
                entity_type="DocumentSnippet",
                description=_for_embedding(normalized),
                source_chunk_id=[doc_id],
                documents_id=[doc_id],
                clusters=[],
            )
        )

    return entities, relations


async def _semantic_candidates(question: str, top_k: int = 8) -> tuple[list[Entity], list[Relation], str]:
    if not state.knowledge_graph:
        return [], [], "graph_not_ready"

    index = state.knowledge_graph.index
    try:
        entities = await index.query_entities(question, top_k=top_k)
    except Exception as exc:
        print(f"Warning: semantic entity search failed: {exc}")
        entities = []

    try:
        relations = await index.query_relations(question, top_k=top_k)
    except Exception as exc:
        print(f"Warning: semantic relation search failed: {exc}")
        relations = []

    source = "embeddings" if entities or relations else "lexical_fallback"
    if entities or relations:
        return entities, relations, source

    tokens = _tokenize(question)
    nodes = await index.graph_backend.get_all_nodes()
    all_edges = await index.graph_backend.get_all_edges()

    scored_nodes = []
    for node in nodes:
        haystack = f"{node.entity_name} {node.entity_type} {node.description}"
        score = _match_score(haystack, tokens)
        if score > 0:
            scored_nodes.append((score, node))
    scored_nodes.sort(key=lambda item: item[0], reverse=True)

    scored_edges = []
    for edge in all_edges:
        haystack = f"{edge.subject_name} {edge.object_name} {edge.relation_type} {edge.description}"
        score = _match_score(haystack, tokens)
        if score > 0:
            scored_edges.append((score, edge))
    scored_edges.sort(key=lambda item: item[0], reverse=True)

    return [node for _, node in scored_nodes[:top_k]], [edge for _, edge in scored_edges[:top_k]], source


async def build_no_llm_answer(question: str, search_scope: Literal["local", "global"]) -> str:
    if not state.knowledge_graph:
        return "NO-LLM режим включен, но граф еще не инициализирован."

    backend = state.knowledge_graph.index.graph_backend
    try:
        all_nodes = await backend.get_all_nodes()
        all_edges = await backend.get_all_edges()
    except Exception as exc:
        return f"NO-LLM режим включен. Временная ошибка чтения графа: {exc}"

    entities, relations, retrieval_source = await _semantic_candidates(question)
    title = "NO-LLM шаблонный семантический ответ"
    lines = [
        title,
        f"Внутренний режим: {search_scope}; генерация LLM: выключена; поиск: {retrieval_source}.",
        f"В графе сейчас: {len(all_nodes)} сущностей, {len(all_edges)} связей.",
        "",
        "Релевантные сущности:",
    ]

    if entities:
        for node in entities[:8]:
            description = " ".join((node.description or "").split())[:180]
            lines.append(f"- {node.entity_name} [{node.entity_type}] id={node.id}")
            if description:
                lines.append(f"  {description}")
    else:
        lines.append("- Явных совпадений по сущностям не найдено.")

    lines.append("")
    lines.append("Релевантные связи:")
    if relations:
        for edge in relations[:8]:
            lines.append(f"- {edge.subject_name} -[{edge.relation_type}]-> {edge.object_name}")
    else:
        lines.append("- Явных совпадений по связям не найдено.")

    if search_scope == "global":
        by_type: dict[str, int] = {}
        for node in all_nodes:
            by_type[node.entity_type] = by_type.get(node.entity_type, 0) + 1
        lines.extend(["", "Сводка по типам узлов:"])
        for node_type, count in sorted(by_type.items()):
            lines.append(f"- {node_type}: {count}")

    lines.extend(
        [
            "",
            "Чтобы получить генеративный красивый ответ, используйте /llm или DISABLE_LLM_ANSWERS=false в .env.",
        ]
    )
    return "\n".join(lines)


@asynccontextmanager
async def lifespan(app: FastAPI):
    client = OpenAIClient(
        model_name=os.getenv("LLM_MODEL_NAME"),
        base_url=os.getenv("BASE_URL"),
        api_token=os.getenv("API_KEY"),
    )
    state.embedding_dim = min(20, max(1, _env_int("EMBEDDING_DIM", 20)))
    state.raw_llm_client = AsyncOpenAI(
        base_url=os.getenv("BASE_URL"),
        api_key=os.getenv("API_KEY"),
        timeout=90,
    )
    embedder = OpenAIEmbedder(
        model_name=os.getenv("EMBEDDER_MODEL_NAME"),
        base_url=os.getenv("EMBEDDING_BASE_URL"),
        api_token=os.getenv("API_KEY"),
        dim=state.embedding_dim,
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
        if _env_flag("DISABLE_LLM_ANSWERS", False):
            entities, relations = build_fast_graph_from_docs(docs)
            await state.knowledge_graph.index.insert_entities(entities)
            if relations:
                await state.knowledge_graph.index.insert_relations(relations)
        else:
            await state.knowledge_graph.build_from_docs(docs)
        print(f"Indexing from {source_desc} finished.")
    except Exception as e:
        print(f"Indexing error: {e}")
    finally:
        state.is_indexing = False


@app.post("/ask/local")
async def ask_local(request: QueryRequest):
    if not _llm_enabled(request.answer_mode):
        answer = await build_no_llm_answer(request.question, "local")
        return {"answer": answer, "mode": "no_llm_semantic_local", "answer_mode": "no_llm"}

    if not state.local_search_engine:
        raise HTTPException(status_code=503, detail="Search engine not ready")
    try:
        answer = await state.local_search_engine.a_query(request.question)
        return {"answer": answer, "mode": "llm_local", "answer_mode": "llm"}
    except Exception as exc:
        print(f"Warning: LLM local search failed, no-LLM fallback is used: {exc}")
        answer = await build_no_llm_answer(request.question, "local")
        return {"answer": answer, "mode": "llm_fallback_no_llm_local", "answer_mode": "no_llm"}


@app.post("/ask/global")
async def ask_global(request: QueryRequest):
    if not _llm_enabled(request.answer_mode):
        answer = await build_no_llm_answer(request.question, "global")
        return {"answer": answer, "mode": "no_llm_semantic_global", "answer_mode": "no_llm"}

    if not state.global_search_engine:
        raise HTTPException(status_code=503, detail="Search engine not ready")
    try:
        answer = await state.global_search_engine.a_query(request.question)
        return {"answer": answer, "mode": "llm_global", "answer_mode": "llm"}
    except Exception as exc:
        print(f"Warning: LLM global search failed, no-LLM fallback is used: {exc}")
        answer = await build_no_llm_answer(request.question, "global")
        return {"answer": answer, "mode": "llm_fallback_no_llm_global", "answer_mode": "no_llm"}


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


@app.post("/answer/llm")
async def answer_llm(request: BeautifyAnswerRequest):
    if not state.raw_llm_client:
        raise HTTPException(status_code=503, detail="LLM client is not ready")

    structured = request.structured_answer.strip()
    if not structured:
        raise HTTPException(status_code=400, detail="structured_answer is empty")
    prompt = (
        "Ты помощник для поиска пакетов. На основе готовых структурированных данных "
        "сформируй красивый, но проверяемый ответ на русском языке. "
        "Не выдумывай версии, ссылки и источники. Сохрани фильтры, количество найденного "
        "и 3-7 наиболее полезных ссылок, если они есть. "
        "Начни ответ строкой: LLM режим: генеративное оформление.\n\n"
        f"Вопрос пользователя:\n{request.question}\n\n"
        f"Структурированные данные:\n{structured[:6000]}"
    )
    try:
        response = await state.raw_llm_client.chat.completions.create(
            model=os.getenv("LLM_MODEL_NAME"),
            messages=[
                {"role": "system", "content": "Отвечай кратко, структурировано и только по предоставленным данным."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"LLM formatter failed: {exc}") from exc
    answer = (response.choices[0].message.content or "").strip()
    if not answer:
        raise HTTPException(status_code=503, detail="LLM formatter returned empty answer")
    return {"answer": answer, "mode": "llm_formatter", "answer_mode": "llm"}


@app.get("/status")
async def status():
    return {
        "is_indexing": state.is_indexing,
        "default_answer_mode": "no_llm" if _env_flag("DISABLE_LLM_ANSWERS", False) else "llm",
        "embedding_dim": state.embedding_dim,
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
