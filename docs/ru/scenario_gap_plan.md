# План демонстрационных сценариев RAGU

Дата: 2026-05-13
Цель: держать проект в воспроизводимом демонстрационном состоянии для сценариев Telegram/VK -> orchestrator -> registry/scraper -> Memgraph -> Google Sheets.

## Что уже есть

- Telegram-бот и VK-бот используют общий `START_MESSAGE`, routing и `AskOrchestrator`.
- `BOT_PLATFORM=telegram|vk|both|none` выбирает активные bot entrypoints.
- FastAPI demo предоставляет:
  - `POST /ask/local`
  - `POST /ask/global`
  - `POST /ingest/json`
  - `POST /answer/llm`
  - `GET /status`
- Registry/scraper слой собирает пакетные артефакты из настроенных серверных источников.
- Orchestrator парсит `product`, `version`, `os`, `format`, `limit`, `show`, `sort`, `source`.
- `/nollm` формирует детерминированный шаблонный ответ.
- `/llm` форматирует уже подготовленный структурированный контекст; LLM не скрапит и не ищет сама.
- Outbox индексирует успешные события в Memgraph и передает их в Google Sheets через `sheets_sync`.
- `EMBEDDING_DIM` принудительно ограничивается максимумом 20.

## Целевые PRD-сценарии

1. `/nollm Python 3.12 для Ubuntu limit=10`
   - шаблонный ответ;
   - LLM не вызывается;
   - событие сохраняется как успешный ask exchange.

2. `/llm дай список всех пакетов PostgreSQL 17.6 limit=13 show=4`
   - используется тот же registry/scraper pipeline;
   - LLM получает только подготовленный контекст;
   - ответ визуально отличается от no-LLM.

3. Обычный запрос без `/llm` и `/nollm`
   - использует default из `.env`;
   - `DISABLE_LLM_ANSWERS=true` -> no-LLM;
   - `DISABLE_LLM_ANSWERS=false` -> LLM formatting.

4. Неподдержанный источник
   - пример: `скачай Foo Package для Solaris format=deb`;
   - система возвращает понятное сообщение;
   - старые semantic fallback-данные не выдаются как успешный результат.

## Проверки перед сдачей

```powershell
.\venv\Scripts\python.exe -m pytest tests\integration_layer -q
.\venv\Scripts\python.exe scripts\check_demo_health.py
.\venv\Scripts\python.exe scripts\check_google_sheets.py
```

После ручного E2E:

- `/status` доступен и показывает `embedding_dim <= 20`;
- нужный бот стартует согласно `BOT_PLATFORM`;
- `/start` и `/help` показывают актуальную инструкцию;
- no-LLM и LLM ответы заметно отличаются;
- Memgraph содержит `AskExchange`, `UserQuery`, `StructuredAnswer`, `PackageArtifact`;
- связи имеют понятные типы: `HAS_QUESTION`, `ANSWERED_BY`, `FOUND_ARTIFACT`;
- Google Sheets получает записи в `overview`, `queries`, `answers`, `ingest_jobs`;
- неподдержанный запрос не попадает в успешные результаты.
