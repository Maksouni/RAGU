# Локальный стек Ollama

Этот runbook запускает дипломное демо RAGU через Docker Compose. Ollama работает на хостовой машине; Memgraph, FastAPI, orchestrator и опциональные worker-сервисы работают в контейнерах.

## Требования

- Docker Desktop или Docker Engine.
- Ollama, установленная на хосте.
- Python virtual environment для локальных служебных скриптов.
- Файл `.env`, скопированный из `.env.example`.

Необходимые модели Ollama:

```powershell
ollama pull qwen2.5:3b
ollama pull nomic-embed-text
```

Если нужно, чтобы стартовый скрипт сам скачивал отсутствующие модели, установите `OLLAMA_AUTO_PULL=true`.

## Запуск

```powershell
.\scripts\start_ollama_stack.ps1
```

Скрипт проверяет Docker, запускает `ollama serve` при необходимости, проверяет наличие моделей и затем выполняет:

```powershell
docker compose -f examples\fastapi_demo\docker-compose.yml up -d --build
```

Базовые сервисы:

- `memgraph`
- `fastapi`
- `orchestrator`

Опциональные профили:

- `MEMGRAPH_LAB_ENABLED=true` включает `memgraph-lab`
- `SHEETS_SYNC_ENABLED=true` включает `sheets_sync`
- `VK_BOT_ENABLED=true` включает `vk_bot`

## Проверка здоровья

```powershell
.\venv\Scripts\python.exe scripts\check_demo_health.py --json
```

Опциональные проверки Sheets и VK возвращают `WARN`, если эти интеграции намеренно выключены.

## Загрузка подготовленных IT-ответов

Подготовленные ответы не загружаются автоматически при старте. Запустите seed явно:

```powershell
.\venv\Scripts\python.exe scripts\seed_demo_it_knowledge.py --api-base-url http://127.0.0.1:8000
```

Seed-скрипт отправляет JSON-объекты в `/ingest/json`.

Обычные строки тоже поддерживаются:

```powershell
$body = @{
  data = @(
    @{
      event_type = "prepared_it_answer"
      id = "prepared_it_memgraph_choice"
      topic = "Выбор Memgraph"
      question = "Почему в проекте выбран Memgraph?"
      keywords = @("memgraph", "graph", "rag")
      answer = "Memgraph выбран как графовая БД для хранения сущностей, связей и маршрута получения ответа."
    },
    "Короткая строка для индексации через /ingest/json."
  )
} | ConvertTo-Json -Depth 6

Invoke-RestMethod -Uri "http://127.0.0.1:8000/ingest/json" -Method Post -ContentType "application/json" -Body $body
```

## Демонстрационные запросы

```text
дай список всех версий PostgreSQL для debian 13
postgresql для ubuntu
какие версии postgresql подходят для ubuntu 24
покажи последние версии postgresql для ubuntu 24 выведи 5
```

Контекст follow-up поддерживается. Сначала спросите PostgreSQL для Debian, затем:

```text
а последнюю покажи
```

## Остановка

```powershell
.\scripts\stop_ollama_stack.ps1
```

По умолчанию stop-скрипт оставляет Ollama запущенной. Чтобы останавливать и Ollama, установите `STOP_OLLAMA_ON_STOP=true`.
