from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apps.common.settings import IntegrationSettings


PREPARED_IT_ANSWERS: list[dict[str, Any]] = [
    {
        "event_type": "prepared_it_answer",
        "id": "prepared_it_docker_image_definition",
        "topic": "Docker image",
        "question": "Что такое Docker image?",
        "keywords": ["docker", "image", "docker image", "образ", "контейнеризация"],
        "answer": (
            "Docker image - это неизменяемый шаблон для запуска контейнера. В нем упакованы файловая система, "
            "зависимости, переменные окружения, metadata и команда запуска приложения. Image хранится в registry "
            "или локальном Docker cache, а container создается как запущенный экземпляр этого image."
        ),
    },
    {
        "event_type": "prepared_it_answer",
        "id": "prepared_it_docker_image_container",
        "topic": "Docker image vs container",
        "question": "Чем Docker image отличается от container?",
        "keywords": ["docker", "image", "container", "контейнер", "образ"],
        "answer": (
            "Docker image - это неизменяемый шаблон приложения: файловая система, зависимости, "
            "команды запуска и метаданные. Container - это запущенный экземпляр image. "
            "Один image можно запустить много раз, получив несколько независимых containers. "
            "Если коротко: image хранится, container выполняется."
        ),
    },
    {
        "event_type": "prepared_it_answer",
        "id": "prepared_it_postgresql_index",
        "topic": "PostgreSQL index",
        "question": "Как работает индекс в PostgreSQL?",
        "keywords": ["postgresql", "postgres", "sql", "index", "индекс"],
        "answer": (
            "Индекс в PostgreSQL - это отдельная структура данных, которая помогает быстрее найти строки "
            "без полного чтения таблицы. Чаще всего используется B-tree: он хранит отсортированные ключи "
            "и ссылки на строки. Индекс ускоряет SELECT/WHERE/JOIN/ORDER BY, но замедляет INSERT/UPDATE/DELETE, "
            "потому что структуру индекса тоже нужно обновлять."
        ),
    },
    {
        "event_type": "prepared_it_answer",
        "id": "prepared_it_embeddings_rag",
        "topic": "Embeddings in RAG",
        "question": "Зачем нужны embeddings в RAG?",
        "keywords": ["rag", "embedding", "embeddings", "vector", "вектор", "эмбеддинг"],
        "answer": (
            "Embeddings нужны, чтобы превратить текст в числовой вектор смысла. После этого похожие вопросы, "
            "ответы и документы можно искать не только по одинаковым словам, а по близости смысла. "
            "В RAG это позволяет сначала найти релевантный контекст в локальной базе, а уже потом отдать "
            "этот контекст formatter-слою или LLM."
        ),
    },
    {
        "event_type": "prepared_it_answer",
        "id": "prepared_it_memgraph_role",
        "topic": "Memgraph role in RAGU",
        "question": "Что делает Memgraph в проекте?",
        "keywords": ["memgraph", "graph", "граф", "связи", "entities", "relations"],
        "answer": (
            "Memgraph хранит граф знаний проекта: запросы пользователя, ответы, найденные артефакты, "
            "сущности и связи между ними. Благодаря этому можно смотреть не только текст ответа, "
            "но и путь: какой запрос породил ответ, какие пакеты были найдены и какие relation types "
            "появились после обработки."
        ),
    },
    {
        "event_type": "prepared_it_answer",
        "id": "prepared_it_vector_vs_graph_search",
        "topic": "Vector search vs graph search",
        "question": "Чем vector search отличается от graph search?",
        "keywords": ["vector search", "graph search", "semantic", "graph", "vector"],
        "answer": (
            "Vector search ищет похожие тексты по близости embeddings: хорошо подходит для вопросов, "
            "которые сформулированы другими словами. Graph search идет по явным связям между узлами: "
            "например, UserQuery -> StructuredAnswer -> PackageArtifact. В RAGU эти подходы дополняют "
            "друг друга: vectors помогают найти кандидатов, graph объясняет связи."
        ),
    },
    {
        "event_type": "prepared_it_answer",
        "id": "prepared_it_bot_routing",
        "topic": "Telegram and VK bot routing",
        "question": "Как работает Telegram/VK bot routing?",
        "keywords": ["telegram", "vk", "bot", "routing", "маршрутизация"],
        "answer": (
            "Telegram и VK получают сообщение пользователя, отделяют команды /start, /help, /llm и /nollm, "
            "а затем отправляют текст в общий AskOrchestrator. Оркестратор выбирает режим ответа, проверяет "
            "тип запроса и либо запускает package-сценарий, либо отправляет обычный IT-вопрос в semantic/graph search."
        ),
    },
    {
        "event_type": "prepared_it_answer",
        "id": "prepared_it_google_sheets_service_account",
        "topic": "Google Sheets service account",
        "question": "Зачем нужен service account для Google Sheets?",
        "keywords": ["google sheets", "service account", "sheets", "таблица"],
        "answer": (
            "Service account нужен, чтобы приложение могло писать в Google Sheets без ручного входа пользователя. "
            "В .env указывается путь к JSON-ключу, а сама таблица должна быть расшарена на email этого service account. "
            "После этого sheets_sync может записывать queries, answers, ingest_jobs и errors_audit."
        ),
    },
    {
        "event_type": "prepared_it_answer",
        "id": "prepared_it_ollama_mistral",
        "topic": "Ollama vs Mistral in RAGU",
        "question": "Чем Ollama отличается от Mistral в этом проекте?",
        "keywords": ["ollama", "mistral", "llm", "provider", "model"],
        "answer": (
            "Ollama запускает LLM и embeddings локально на ноутбуке, поэтому не требует внешнего API, "
            "но нагружает машину. Mistral работает через внешний API: ноутбуку легче, но нужен ключ и интернет. "
            "В обоих случаях модель не скрапит источники сама, а только оформляет уже подготовленный контекст."
        ),
    },
    {
        "event_type": "prepared_it_answer",
        "id": "prepared_it_api_endpoint",
        "topic": "API endpoint",
        "question": "Что такое API endpoint?",
        "keywords": ["api", "endpoint", "http", "rest"],
        "answer": (
            "API endpoint - это конкретный URL и HTTP-метод, через который клиент обращается к функции сервиса. "
            "Например, POST /ask/local принимает вопрос и возвращает ответ, а GET /status возвращает состояние сервиса. "
            "Endpoint задает точку входа, формат запроса и ожидаемый ответ."
        ),
    },
    {
        "event_type": "prepared_it_answer",
        "id": "prepared_it_retry_backoff",
        "topic": "Retry and backoff in scraper",
        "question": "Зачем нужен retry/backoff в scraper?",
        "keywords": ["retry", "backoff", "scraper", "timeout", "source"],
        "answer": (
            "Retry нужен, чтобы повторить запрос к источнику после временной ошибки сети или сервера. "
            "Backoff добавляет паузу между попытками, чтобы не долбить источник слишком часто. "
            "В RAGU это помогает не ждать бесконечно и вернуть пользователю понятное сообщение, если источник недоступен."
        ),
    },
    {
        "event_type": "prepared_it_answer",
        "id": "prepared_it_git_merge_rebase",
        "topic": "Git merge vs rebase",
        "question": "Чем git merge отличается от git rebase?",
        "keywords": ["git", "merge", "rebase", "branch", "ветка"],
        "answer": (
            "git merge объединяет ветки, создавая merge-коммит и сохраняя реальную историю параллельной работы. "
            "git rebase переносит ваши коммиты поверх другой ветки и делает историю линейной. "
            "Merge удобен для явной истории интеграции, rebase - для аккуратной локальной истории перед pull request."
        ),
    },
    {
        "event_type": "prepared_it_answer",
        "id": "prepared_it_sql_transaction",
        "topic": "SQL transaction and ACID",
        "question": "Что такое транзакция в базе данных?",
        "keywords": ["sql", "database", "transaction", "acid", "транзакция", "база"],
        "answer": (
            "Транзакция - это группа операций с базой данных, которая должна выполниться целиком или не выполниться совсем. "
            "Классические свойства ACID: атомарность, согласованность, изоляция и долговечность. "
            "Например, при переводе денег списание и зачисление должны быть одной транзакцией."
        ),
    },
    {
        "event_type": "prepared_it_answer",
        "id": "prepared_it_jwt",
        "topic": "JWT token",
        "question": "Что такое JWT токен?",
        "keywords": ["jwt", "token", "auth", "oauth", "токен", "аутентификация"],
        "answer": (
            "JWT - это компактный подписанный токен, в котором обычно хранятся claims: кто пользователь, когда токен истекает "
            "и какие права ему выданы. Сервер проверяет подпись и срок действия, но не должен класть в JWT секретные данные, "
            "потому что payload легко декодируется."
        ),
    },
    {
        "event_type": "prepared_it_answer",
        "id": "prepared_it_reverse_proxy",
        "topic": "Reverse proxy",
        "question": "Зачем нужен reverse proxy?",
        "keywords": ["nginx", "proxy", "reverse proxy", "http", "tls", "прокси"],
        "answer": (
            "Reverse proxy принимает внешние HTTP/HTTPS-запросы и передает их внутренним сервисам. "
            "Обычно он завершает TLS, маршрутизирует запросы, добавляет лимиты, gzip, кеширование и скрывает внутреннюю структуру приложения. "
            "Типичный пример - Nginx перед FastAPI или другим backend-сервисом."
        ),
    },
    {
        "event_type": "prepared_it_answer",
        "id": "prepared_it_debian_13_release",
        "topic": "Debian 13 release date",
        "question": "В каком году вышел Debian 13?",
        "keywords": ["debian", "debian 13", "trixie", "release", "релиз", "вышел", "год"],
        "answer": (
            "Debian 13 вышел в 2025 году. Его кодовое имя - trixie, официальный релиз Debian 13.0 "
            "состоялся 9 августа 2025 года. Bullseye - это Debian 11, а не Debian 13."
        ),
    },
]


async def _wait_until_idle(client: httpx.AsyncClient, timeout_sec: float = 90.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_sec
    while asyncio.get_running_loop().time() < deadline:
        try:
            response = await client.get("/status")
            response.raise_for_status()
            payload = response.json()
            if not payload.get("is_indexing"):
                return
        except Exception:
            pass
        await asyncio.sleep(1)
    raise TimeoutError("Indexer did not become idle in time.")


async def _post_batch_with_retry(
    client: httpx.AsyncClient,
    batch: list[dict[str, Any]],
    *,
    max_busy_retries: int,
) -> httpx.Response:
    for attempt in range(max_busy_retries + 1):
        response = await client.post("/ingest/json", json={"data": batch})
        if response.status_code != 409:
            return response
        if attempt >= max_busy_retries:
            return response
        await asyncio.sleep(min(10, 1 + attempt * 2))
    return response


async def main() -> None:
    parser = argparse.ArgumentParser(description="Seed prepared IT demo answers through /ingest/json.")
    parser.add_argument("--api-base-url", default=None, help="Override API_BASE_URL from .env.")
    parser.add_argument("--batch-size", type=int, default=50, help="How many prepared answers to ingest per request.")
    parser.add_argument("--busy-retries", type=int, default=20, help="How many times to retry while indexer is busy.")
    args = parser.parse_args()

    settings = IntegrationSettings()
    base_url = (args.api_base_url or settings.api_base_url).rstrip("/")

    accepted = 0
    failed = 0
    batch_size = max(1, args.batch_size)

    async with httpx.AsyncClient(
        base_url=base_url,
        timeout=settings.ingest_timeout_sec,
        trust_env=False,
    ) as client:
        for offset in range(0, len(PREPARED_IT_ANSWERS), batch_size):
            batch = PREPARED_IT_ANSWERS[offset : offset + batch_size]
            await _wait_until_idle(client)
            response = await _post_batch_with_retry(client, batch, max_busy_retries=max(0, args.busy_retries))
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError:
                failed += len(batch)
                print(f"Failed batch: {', '.join(item['id'] for item in batch)}")
                print(response.text)
                continue

            payload = response.json()
            accepted += int(payload.get("count", len(batch)))
            print(f"Accepted batch: {', '.join(item['id'] for item in batch)}")

        await _wait_until_idle(client)

    print(
        f"Seed finished: accepted={accepted}, failed={failed}"
    )

if __name__ == "__main__":
    asyncio.run(main())
