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

from apps.common.bot_messages import START_MESSAGE
from apps.orchestrator.scenario_manager import ScenarioManager
from apps.registry.repository import RegistryRepository
from apps.scraper.service import PackageScraperService
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


def _load_project_env() -> None:
    """Load .env for local runs without assuming the Docker image path depth."""
    current_file = Path(__file__).resolve()
    candidates = [
        current_file.parent / ".env",
        Path.cwd() / ".env",
    ]
    if len(current_file.parents) > 2:
        candidates.append(current_file.parents[2] / ".env")

    for env_path in candidates:
        if env_path.exists():
            load_dotenv(env_path, override=True)
            return


_load_project_env()

AnswerMode = Literal["auto", "llm", "no_llm"]
EMBEDDING_TEXT_LIMIT = 1800
LEXICAL_MIN_SCORE = 2
PREPARED_IT_MIN_SCORE = 2
FAST_GRAPH_EVENT_TYPES = {"ask_exchange", "prepared_it_answer"}
LEXICAL_STOPWORDS = {
    "and",
    "are",
    "between",
    "for",
    "from",
    "how",
    "into",
    "the",
    "what",
    "why",
    "with",
    "без",
    "для",
    "зачем",
    "как",
    "между",
    "мне",
    "нужны",
    "объясни",
    "почему",
    "покажи",
    "расскажи",
    "что",
    "чем",
}
KNOWN_GENERAL_ANSWERS = {
    "redis": (
        "Redis - это in-memory key-value хранилище данных. Его часто используют как кеш, broker для очередей, "
        "хранилище с TTL и быстрый слой для счетчиков, сессий и rate limiting. Redis не является языком программирования: "
        "это сервер базы данных, который поддерживает строки, списки, множества, hash-структуры, sorted sets и pub/sub."
    ),
    "mysql": "MySQL - это реляционная СУБД для SQL-данных, транзакций, индексов и клиент-серверных приложений.",
    "mariadb": "MariaDB - это открытая реляционная СУБД, совместимая с MySQL на уровне SQL и многих клиентских инструментов.",
    "sqlite": "SQLite - это встраиваемая SQL-база данных в одном файле, без отдельного серверного процесса.",
    "mongodb": "MongoDB - это документная NoSQL-СУБД, где данные обычно хранятся в BSON-документах и коллекциях.",
    "go": "Go - это компилируемый язык программирования от Google с простой моделью конкурентности через goroutines.",
    "golang": "Go - это компилируемый язык программирования от Google с простой моделью конкурентности через goroutines.",
    "nodejs": "Node.js - это runtime для JavaScript на сервере, построенный вокруг событийной модели и неблокирующего ввода-вывода.",
    "ruby": "Ruby - это динамический объектно-ориентированный язык программирования, известный лаконичным синтаксисом и Rails-экосистемой.",
    "php": "PHP - это серверный язык программирования, широко используемый для веб-приложений и CMS.",
    "openjdk": "OpenJDK - это открытая реализация Java Development Kit: компилятор, JVM и стандартные инструменты Java.",
    "java": "Java - это язык и платформа выполнения на JVM; для установки в Linux обычно используется OpenJDK.",
    "rust": "Rust - это системный язык программирования с упором на безопасность памяти без сборщика мусора.",
    "rustc": "rustc - это компилятор языка Rust; вместе с cargo он используется для сборки Rust-проектов.",
}


class AppState:
    knowledge_graph: KnowledgeGraph = None
    local_search_engine: LocalSearchEngine = None
    global_search_engine: GlobalSearchEngine = None
    scenario_manager: ScenarioManager = None
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


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _llm_provider() -> str:
    provider = (os.getenv("LLM_PROVIDER") or "ollama").strip().lower() or "ollama"
    return "ollama" if provider == "ollama" else "unsupported"


def _llm_enabled(answer_mode: AnswerMode) -> bool:
    if answer_mode == "llm":
        return True
    if answer_mode == "no_llm":
        return False
    return not _env_flag("DISABLE_LLM_ANSWERS", False)


def _effective_answer_mode(answer_mode: AnswerMode) -> Literal["llm", "no_llm"]:
    return "llm" if _llm_enabled(answer_mode) else "no_llm"


def _tokenize(text: str) -> List[str]:
    return [t for t in re.findall(r"\w+", text.lower()) if len(t) >= 3 and t not in LEXICAL_STOPWORDS]


def _match_score(content: str, tokens: List[str]) -> int:
    content_tokens = set(_tokenize(content))
    return sum(1 for token in tokens if token in content_tokens)


def _is_definition_question(question: str, topic: str) -> bool:
    normalized = _normalize_question(question)
    escaped_topic = re.escape(topic)
    patterns = (
        rf"^(?:что\s+такое|что\s+значит|что\s+означает)\s+(?:язык\s+|субд\s+|база\s+данных\s+)?{escaped_topic}$",
        rf"^(?:what\s+is|define)\s+(?:the\s+)?{escaped_topic}(?:\s+language)?$",
        rf"^{escaped_topic}\s+(?:это\s+)?что$",
    )
    return any(re.fullmatch(pattern, normalized, re.IGNORECASE) for pattern in patterns)


def _known_general_answer(question: str, search_scope: Literal["local", "global"]) -> str | None:
    for topic, answer in KNOWN_GENERAL_ANSWERS.items():
        if _is_definition_question(question, topic):
            return "\n".join(
                [
                    "NO-LLM заготовленный IT-ответ",
                    f"Внутренний режим: {search_scope}; генерация LLM: выключена; поиск: built_in_topic:{topic}.",
                    f"Тема: {topic}",
                    "",
                    answer,
                ]
            )
    return None


def _normalize_question(text: str) -> str:
    return " ".join((text or "").strip().lower().rstrip(".?!").split())


def _extract_prepared_answer(description: str) -> str:
    marker = "Ответ:"
    if marker not in description:
        return description.strip()
    return description.split(marker, 1)[1].strip()


def _strip_saved_answer_headers(answer: str) -> str:
    cleaned = re.sub(
        r"^\s*LLM режим:.*?Внутренний режим:\s*[^.]+?\.\s*",
        "",
        answer.strip(),
        flags=re.IGNORECASE | re.DOTALL,
    ).strip()
    lines = cleaned.splitlines()
    while lines and (
        not lines[0].strip()
        or lines[0].startswith("LLM режим:")
        or lines[0].startswith("Внутренний режим:")
    ):
        lines.pop(0)
    return "\n".join(lines).strip() or answer.strip()


def _build_prepared_it_answer(node: Entity, search_scope: Literal["local", "global"], retrieval_source: str) -> str:
    answer = _extract_prepared_answer(node.description or "")
    return "\n".join(
        [
            "NO-LLM заготовленный IT-ответ",
            f"Внутренний режим: {search_scope}; генерация LLM: выключена; поиск: {retrieval_source}.",
            f"Тема: {node.entity_name}",
            "",
            answer,
        ]
    )


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


def _split_fast_graph_docs(docs: List[str]) -> tuple[List[str], List[str]]:
    fast_docs: List[str] = []
    regular_docs: List[str] = []
    for doc in docs:
        parsed = _parse_doc(doc)
        if parsed and parsed.get("event_type") in FAST_GRAPH_EVENT_TYPES:
            fast_docs.append(doc)
        else:
            regular_docs.append(doc)
    return fast_docs, regular_docs


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
        if parsed and parsed.get("event_type") == "prepared_it_answer":
            question = str(parsed.get("question") or parsed.get("title") or "").strip()
            answer = str(parsed.get("answer") or "").strip()
            topic = str(parsed.get("topic") or question or f"prepared_it_answer_{idx}").strip()
            keywords = parsed.get("keywords") if isinstance(parsed.get("keywords"), list) else []
            keyword_text = ", ".join(str(item) for item in keywords if str(item).strip())
            prepared_id = str(parsed.get("id") or _stable_id("prepared_it", f"{topic}|{question}|{answer}"))
            description = _for_embedding(
                f"Готовый IT-ответ. Тема: {topic}. Вопрос: {question}. "
                f"Ключевые слова: {keyword_text}. Ответ: {answer}"
            )
            entities.append(
                Entity(
                    id=prepared_id,
                    entity_name=topic[:120],
                    entity_type="PreparedITAnswer",
                    description=description,
                    source_chunk_id=[prepared_id],
                    documents_id=[prepared_id],
                    clusters=[],
                )
            )
            continue
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
        if score >= LEXICAL_MIN_SCORE:
            scored_nodes.append((score, node))
    scored_nodes.sort(key=lambda item: item[0], reverse=True)

    scored_edges = []
    for edge in all_edges:
        haystack = f"{edge.subject_name} {edge.object_name} {edge.relation_type} {edge.description}"
        score = _match_score(haystack, tokens)
        if score >= LEXICAL_MIN_SCORE:
            scored_edges.append((score, edge))
    scored_edges.sort(key=lambda item: item[0], reverse=True)

    return [node for _, node in scored_nodes[:top_k]], [edge for _, edge in scored_edges[:top_k]], source


def _extract_saved_question(description: str) -> str:
    marker = "Вопрос:"
    keywords_marker = "Ключевые слова:"
    answer_marker = "Ответ:"
    if marker not in description or answer_marker not in description:
        return ""
    saved_question = description.split(marker, 1)[1]
    if keywords_marker in saved_question:
        saved_question = saved_question.split(keywords_marker, 1)[0]
    else:
        saved_question = saved_question.split(answer_marker, 1)[0]
    return saved_question.strip().rstrip(".")


def _prepared_it_score(question: str, node: Entity) -> int:
    tokens = _tokenize(question)
    if not tokens:
        return 0
    saved_question = _extract_saved_question(node.description or "")
    if saved_question and _normalize_question(saved_question) == _normalize_question(question):
        return 100 + len(tokens)
    haystack = f"{node.entity_name} {node.description}"
    return _match_score(haystack, tokens)


def _best_prepared_it_answer(question: str, nodes: List[Entity]) -> tuple[Entity | None, int]:
    candidates = [
        (_prepared_it_score(question, node), node)
        for node in nodes
        if node.entity_type == "PreparedITAnswer"
    ]
    candidates = [(score, node) for score, node in candidates if score >= PREPARED_IT_MIN_SCORE]
    if not candidates:
        return None, 0
    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][1], candidates[0][0]


def _trusted_prepared_from_candidates(question: str, nodes: List[Entity]) -> tuple[Entity | None, int]:
    prepared, score = _best_prepared_it_answer(question, nodes)
    if prepared is None or score < PREPARED_IT_MIN_SCORE:
        return None, score
    return prepared, score


def _find_exact_saved_answer(
    question: str,
    nodes: List[Entity],
    relations: List[Relation],
) -> tuple[str | None, str]:
    question_norm = _normalize_question(question)
    nodes_by_id = {str(node.id): node for node in nodes}

    for node in nodes:
        if node.entity_type != "UserQuery":
            continue
        saved_question = (node.description or "").replace("Пользовательский запрос:", "", 1).strip()
        if _normalize_question(saved_question) != question_norm:
            continue
        for edge in relations:
            if str(edge.subject_id) != str(node.id):
                continue
            answer_node = nodes_by_id.get(str(edge.object_id))
            if answer_node and answer_node.entity_type == "StructuredAnswer":
                return answer_node.description.strip(), f"exact_saved_qa:{answer_node.id}"

    for node in nodes:
        if node.entity_type != "AskExchange":
            continue
        saved_question = _extract_saved_question(node.description or "")
        if saved_question and _normalize_question(saved_question) == question_norm:
            answer = _extract_prepared_answer(node.description or "")
            if answer:
                return answer, f"exact_saved_exchange:{node.id}"

    return None, ""


def _format_saved_qa_answer(
    saved_answer: str,
    search_scope: Literal["local", "global"],
    retrieval_source: str,
) -> str:
    clean_answer = _strip_saved_answer_headers(saved_answer)
    return "\n".join(
        [
            "NO-LLM ответ из сохраненной базы",
            f"Внутренний режим: {search_scope}; генерация LLM: выключена; поиск: {retrieval_source}.",
            "",
            clean_answer,
        ]
    )


def _candidate_score(question: str, entities: List[Entity], relations: List[Relation]) -> int:
    tokens = _tokenize(question)
    if not tokens:
        return 0
    scores = [
        _match_score(f"{node.entity_name} {node.entity_type} {node.description}", tokens)
        for node in entities
    ]
    scores.extend(
        _match_score(f"{edge.subject_name} {edge.object_name} {edge.relation_type} {edge.description}", tokens)
        for edge in relations
    )
    return max(scores, default=0)


def _format_no_reliable_context_answer(
    question: str,
    search_scope: Literal["local", "global"],
    retrieval_source: str,
    nodes_count: int,
    edges_count: int,
) -> str:
    return "\n".join(
        [
            "NO-LLM шаблонный ответ",
            f"Внутренний режим: {search_scope}; генерация LLM: выключена; поиск: {retrieval_source}.",
            f"В графе сейчас: {nodes_count} сущностей, {edges_count} связей.",
            "",
            "Статус: в локальной базе не найдено достаточно релевантной IT-заготовки.",
            "Я не буду подмешивать случайные старые package-результаты из графа в обычный вопрос.",
            "Для генеративного ответа используйте /llm; для расширения no-LLM базы запустите scripts/seed_demo_it_knowledge.py.",
            "",
            f"Запрос: {question}",
        ]
    )


async def build_no_llm_answer(question: str, search_scope: Literal["local", "global"]) -> str:
    if not state.knowledge_graph:
        return "NO-LLM режим включен, но граф еще не инициализирован."

    backend = state.knowledge_graph.index.graph_backend
    try:
        all_nodes = await backend.get_all_nodes()
        all_edges = await backend.get_all_edges()
    except Exception as exc:
        return f"NO-LLM режим включен. Временная ошибка чтения графа: {exc}"

    prepared, prepared_score = _trusted_prepared_from_candidates(question, all_nodes)
    if prepared is not None:
        return _build_prepared_it_answer(prepared, search_scope, f"prepared_it_lexical:{prepared_score}")

    saved_answer, saved_source = _find_exact_saved_answer(question, all_nodes, all_edges)
    if saved_answer:
        return _format_saved_qa_answer(saved_answer, search_scope, saved_source)

    entities, relations, retrieval_source = await _semantic_candidates(question)
    prepared, prepared_score = _trusted_prepared_from_candidates(question, entities)
    if prepared is not None:
        return _build_prepared_it_answer(prepared, search_scope, f"{retrieval_source}:{prepared_score}")

    if _candidate_score(question, entities, relations) < LEXICAL_MIN_SCORE:
        return _format_no_reliable_context_answer(
            question=question,
            search_scope=search_scope,
            retrieval_source=retrieval_source,
            nodes_count=len(all_nodes),
            edges_count=len(all_edges),
        )

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


async def build_llm_general_answer(question: str, search_scope: Literal["local", "global"]) -> str:
    if not state.raw_llm_client:
        raise HTTPException(status_code=503, detail="LLM client is not ready")

    context_source = "model"
    context = ""
    if state.knowledge_graph:
        try:
            nodes = await state.knowledge_graph.index.graph_backend.get_all_nodes()
            prepared, prepared_score = _trusted_prepared_from_candidates(question, nodes)
            if prepared is not None:
                context_source = f"prepared_it_answer:{prepared_score}"
                context = _extract_prepared_answer(prepared.description or "")
        except Exception as exc:
            print(f"Warning: prepared IT lookup failed: {exc}")

    prompt = (
        "Ответь на вопрос пользователя на русском языке. Если есть локальный контекст, опирайся на него. "
        "Если локального контекста нет, дай аккуратный общий ответ как LLM. "
        "Не копируй локальный контекст дословно: переформулируй его под вопрос пользователя, "
        "добавь 1-3 полезные детали, но не противоречь локальному контексту. "
        "Если вопрос просит конкретные файлы установки, версии пакетов или ссылки на скачивание, не выдумывай ссылки: "
        "скажи, что для этого нужен package-запрос с продуктом, версией, ОС и format=deb|rpm|exe.\n\n"
        f"Вопрос:\n{question}\n\n"
        f"Локальный контекст:\n{context or 'Нет достаточно точной записи в локальной IT-базе.'}"
    )
    response = await state.raw_llm_client.chat.completions.create(
        model=os.getenv("LLM_MODEL_NAME"),
        messages=[
            {
                "role": "system",
                "content": (
                    "Ты кратко и точно отвечаешь на вопросы пользователя. "
                    "Если дан локальный контекст, используй его как источник фактов, но формулируй ответ заново. "
                    "Не выдумывай download URL и версии пакетов."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        temperature=0.2,
    )
    generated = (response.choices[0].message.content or "").strip()
    if not generated:
        raise HTTPException(status_code=503, detail="LLM returned empty answer")
    return "\n".join(
        [
            "LLM режим: общий IT-ответ",
            f"Внутренний режим: {search_scope}; источник: {context_source}.",
            "",
            generated,
        ]
    )


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
    state.scenario_manager = ScenarioManager(
        registry=RegistryRepository(),
        scraper=PackageScraperService(timeout_sec=_env_float("ASK_TIMEOUT_SEC", 60.0)),
    )

    print("GraphRAG Server ready.")
    try:
        yield
    finally:
        scenario_manager = state.scenario_manager
        scraper = getattr(scenario_manager, "_scraper", None) if scenario_manager is not None else None
        if scraper is not None and hasattr(scraper, "aclose"):
            await scraper.aclose()


app = FastAPI(title="GraphRAG JSON Service", lifespan=lifespan)


async def run_indexing(docs: List[str], source_desc: str):
    state.is_indexing = True
    try:
        print(f"Indexing {len(docs)} docs from {source_desc}...")
        fast_docs, regular_docs = _split_fast_graph_docs(docs)

        if fast_docs:
            entities, relations = build_fast_graph_from_docs(fast_docs)
            await state.knowledge_graph.index.insert_entities(entities)
            if relations:
                await state.knowledge_graph.index.insert_relations(relations)

        if regular_docs:
            if _env_flag("DISABLE_LLM_ANSWERS", False):
                entities, relations = build_fast_graph_from_docs(regular_docs)
                await state.knowledge_graph.index.insert_entities(entities)
                if relations:
                    await state.knowledge_graph.index.insert_relations(relations)
            else:
                await state.knowledge_graph.build_from_docs(regular_docs)
        print(f"Indexing from {source_desc} finished.")
    except Exception as e:
        print(f"Indexing error: {e}")
    finally:
        state.is_indexing = False


async def _package_answer_if_supported(
    question: str,
    search_scope: Literal["local", "global"],
    answer_mode: AnswerMode,
) -> dict[str, str] | None:
    scenario_manager = state.scenario_manager
    if scenario_manager is None:
        return None

    effective_answer_mode = _effective_answer_mode(answer_mode)
    scenario_result = await scenario_manager.handle_if_supported(
        question,
        requested_mode=search_scope,
        answer_mode=effective_answer_mode,
    )
    if not scenario_result.handled:
        return None

    answer = scenario_result.answer
    response_mode = f"registry_scrape_{search_scope}"
    artifacts_count = int(scenario_result.metadata.get("artifacts_count") or 0)
    if effective_answer_mode == "llm" and artifacts_count > 0:
        try:
            beautified = await answer_llm(
                BeautifyAnswerRequest(question=question, structured_answer=answer)
            )
            beautified_answer = str(beautified.get("answer", "")).strip()
            if beautified_answer:
                answer = beautified_answer
                response_mode = f"registry_scrape_llm_{search_scope}"
        except Exception as exc:
            print(f"Warning: package LLM formatter failed, structured answer is used: {exc}")

    return {"answer": answer, "mode": response_mode, "answer_mode": effective_answer_mode}


@app.get("/start")
async def start():
    return {"answer": START_MESSAGE, "mode": "start"}


@app.post("/start")
async def start_post():
    return await start()


@app.post("/ask/local")
async def ask_local(request: QueryRequest):
    package_answer = await _package_answer_if_supported(request.question, "local", request.answer_mode)
    if package_answer is not None:
        return package_answer

    known_answer = _known_general_answer(request.question, "local")
    if known_answer is not None:
        return {"answer": known_answer, "mode": "known_general_local", "answer_mode": "no_llm"}

    if not _llm_enabled(request.answer_mode):
        answer = await build_no_llm_answer(request.question, "local")
        return {"answer": answer, "mode": "no_llm_semantic_local", "answer_mode": "no_llm"}

    try:
        answer = await build_llm_general_answer(request.question, "local")
        return {"answer": answer, "mode": "llm_general_local", "answer_mode": "llm"}
    except Exception as exc:
        print(f"Warning: LLM general answer failed, no-LLM fallback is used: {exc}")
        answer = await build_no_llm_answer(request.question, "local")
        return {"answer": answer, "mode": "llm_fallback_no_llm_local", "answer_mode": "no_llm"}


@app.post("/ask")
async def ask_default(request: QueryRequest):
    return await ask_local(request)


@app.post("/ask/global")
async def ask_global(request: QueryRequest):
    package_answer = await _package_answer_if_supported(request.question, "global", request.answer_mode)
    if package_answer is not None:
        return package_answer

    known_answer = _known_general_answer(request.question, "global")
    if known_answer is not None:
        return {"answer": known_answer, "mode": "known_general_global", "answer_mode": "no_llm"}

    if not _llm_enabled(request.answer_mode):
        answer = await build_no_llm_answer(request.question, "global")
        return {"answer": answer, "mode": "no_llm_semantic_global", "answer_mode": "no_llm"}

    try:
        answer = await build_llm_general_answer(request.question, "global")
        return {"answer": answer, "mode": "llm_general_global", "answer_mode": "llm"}
    except Exception as exc:
        print(f"Warning: LLM general answer failed, no-LLM fallback is used: {exc}")
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
        "и лимит show из структурированных данных: не добавляй выдачу сверх того, что уже передано. "
        "Начни ответ строкой: LLM режим: генеративное оформление.\n\n"
        f"Вопрос пользователя:\n{request.question}\n\n"
        f"Структурированные данные:\n{structured[:6000]}"
    )
    try:
        response = await state.raw_llm_client.chat.completions.create(
            model=os.getenv("LLM_MODEL_NAME"),
            messages=[
                {"role": "system", "content": "Отвечай кратко, структурировано и только по предоставленным данным. Не расширяй список версий или ссылок сверх переданного show-лимита."},
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
    components: dict[str, dict[str, str]] = {
        "fastapi": {"status": "OK", "message": "service is running"},
        "search_engine": {
            "status": "OK" if state.knowledge_graph and state.local_search_engine else "WARN",
            "message": "knowledge graph is initialized" if state.knowledge_graph else "knowledge graph is not ready",
        },
        "memgraph": {
            "status": "OK" if state.knowledge_graph else "WARN",
            "message": os.getenv("MEMGRAPH_URI", "bolt://memgraph:7687"),
        },
        "llm_formatter": {
            "status": "WARN" if _env_flag("DISABLE_LLM_ANSWERS", False) else ("OK" if state.raw_llm_client else "FAIL"),
            "message": (
                "disabled by DISABLE_LLM_ANSWERS"
                if _env_flag("DISABLE_LLM_ANSWERS", False)
                else f"configured local OpenAI-compatible endpoint={os.getenv('BASE_URL')}"
            ),
        },
    }
    summary = "OK"
    if any(item["status"] == "FAIL" for item in components.values()):
        summary = "FAIL"
    elif any(item["status"] == "WARN" for item in components.values()):
        summary = "WARN"
    return {
        "summary": summary,
        "is_indexing": state.is_indexing,
        "default_answer_mode": "no_llm" if _env_flag("DISABLE_LLM_ANSWERS", False) else "llm",
        "llm_provider": _llm_provider(),
        "llm_model": os.getenv("LLM_MODEL_NAME"),
        "embedder_model": os.getenv("EMBEDDER_MODEL_NAME"),
        "embedding_dim": state.embedding_dim,
        "components": components,
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
