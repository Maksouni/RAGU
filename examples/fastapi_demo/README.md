# Демо FastAPI

Это демо запускает интеграционный стек RAGU со следующими сервисами:

- `fastapi`: API-сервис из `server.py`
- `memgraph`: графовое хранилище
- `orchestrator`: worker для outbox ingest
- `sheets_sync`: опциональный worker синхронизации Google Sheets
- `vk_bot`: опциональный VK-бот

Ollama работает на хостовой машине и передает контейнерам OpenAI-совместимый endpoint через `host.docker.internal`.

## API

Сервис доступен на `http://127.0.0.1:8000` и предоставляет:

- `POST /ingest/json` - загрузка JSON-объектов или обычных текстовых строк
- `POST /ask/local` - локальный поиск с учетом графа
- `POST /ask/global` - глобальный поиск
- `GET /status` - состояние индексации и компонентов

`/ask/local` и `/ask/global` принимают параметр `answer_mode`:

- `auto` - использовать настройку `.env` (`DISABLE_LLM_ANSWERS`)
- `llm` - принудительно сгенерировать ответ через локальную LLM
- `no_llm` - принудительно вернуть детерминированный ответ без LLM

## Опциональный VK-бот

```dotenv
VK_BOT_ENABLED=false
VK_BOT_TOKEN=
VK_GROUP_ID=
VK_API_VERSION=5.199
VK_LONG_POLL_WAIT_SEC=25
```

Пользователь может писать вопросы обычным текстом. Команды `/local` и `/global` сохранены для совместимости. Команды `/llm` и `/nollm` переключают стиль ответа для конкретного сообщения.

## Запуск

Из корня репозитория:

```bash
docker compose -f examples/fastapi_demo/docker-compose.yml up -d --build
```

Или через проектный скрипт:

```powershell
.\scripts\start_ollama_stack.ps1
```

Опциональные профили:

```bash
docker compose -f examples/fastapi_demo/docker-compose.yml --profile lab --profile sheets --profile vk up -d --build
```

## Примечания

- Индексация выполняется в фоне; новые ingest-запросы возвращают `409`, пока индексатор занят.
- Пустые ingest-payload возвращают `400`.
- Memgraph Bolt endpoint доступен на `127.0.0.1:7687`.
- Для локального запуска на ноутбуке рекомендуется оставить `EMBEDDING_DIM=20`; векторы усекаются до этого размера, даже если embedding-модель возвращает более длинный вектор.
