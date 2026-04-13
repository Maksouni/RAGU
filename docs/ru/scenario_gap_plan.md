# Gap Plan: Сценарии PostgreSQL (Debian 13 и версия 17.6)

Дата: 2026-04-13  
Цель: довести проект до состояния, когда 2 целевых сценария стабильно воспроизводятся end-to-end.

## 1) Что уже есть

- Интеграционный слой Telegram -> API -> outbox -> ingest -> Google Sheets:
  - `apps/bot`
  - `apps/orchestrator`
  - `apps/sheets_sync`
  - `apps/common`
- FastAPI endpoints:
  - `POST /ask/local`
  - `POST /ask/global`
  - `POST /ingest/json`
  - `GET /status`

## 2) Чего не хватает для ваших сценариев

### Критичные пробелы (blockers)

1. Нет `apps/registry` с шаблонами источников (repo templates + rules).
2. Нет `apps/scraper` / `apps/xdt_mgr` для обхода источников и сборки карточек пакетов.
3. Нет логики в `mgr`, которая:
   - понимает параметры запроса (`product`, `os`, `os_version`, `package_version`),
   - выбирает набор источников из registry,
   - запускает сбор по ним,
   - нормализует итог в единый контракт ответа.
4. Нет контрактов данных для сценариев (единый ответ с `package_name`, `version`, `format`, `artifact_url`, `source_url`).
5. Нет e2e тестов по 2 сценариям с фикстурами источников.

### Некритичные, но важные

1. Нет кэша сырого ответа по источникам (чтобы не DDOS-ить репозитории).
2. Нет SLA/таймаут-политики на внешний fetch (retry/backoff/circuit breaker на источник).
3. Нет curated "golden dataset" для приемки.

## 3) План внедрения (что добавить в код)

## Этап A: Registry + contracts (обязательно сначала)

### Новые файлы

- `apps/registry/__init__.py`
- `apps/registry/models.py`
- `apps/registry/repository.py`
- `apps/registry/templates/debian.yaml`
- `apps/registry/templates/ubuntu.yaml`
- `apps/registry/templates/redhat.yaml`
- `apps/registry/templates/alpine.yaml`

### Что реализовать

- Модель шаблона источника:
  - `repo_id`, `vendor`, `os`, `os_version`, `format`, `fetch_method`, `url_template`, `parse_rules`.
- Фильтр по параметрам сценария:
  - Сценарий 1: `os=debian`, `os_version=13`.
  - Сценарий 2: все шаблоны.

## Этап B: Source fetchers (API + scrape)

### Новые файлы

- `apps/scraper/__init__.py`
- `apps/scraper/client.py`
- `apps/scraper/fetchers/base.py`
- `apps/scraper/fetchers/http_json.py`
- `apps/scraper/fetchers/html_index.py`
- `apps/scraper/parsers/deb_parser.py`
- `apps/scraper/parsers/rpm_parser.py`
- `apps/scraper/parsers/apk_parser.py`
- `apps/scraper/models.py`

### Что реализовать

- Унифицированный контракт записи:
  - `product`, `package_name`, `package_version`, `os`, `os_version`, `format`, `artifact_url`, `source_url`.
- Дедупликация по `(package_name, package_version, format, artifact_url)`.

## Этап C: Manager (mgr) orchestration logic

### Новые файлы

- `apps/orchestrator/scenario_manager.py`
- `apps/orchestrator/query_parser.py`
- `apps/orchestrator/result_formatter.py`

### Изменяемые файлы

- `apps/orchestrator/service.py`
- `apps/common/models.py`

### Что реализовать

- Парсинг запросов:
  - "все версии PostgreSQL для debian 13" -> Scenario 1.
  - "все пакеты PostgreSQL 17.6" -> Scenario 2.
- Вызов registry + scraper pipeline.
- Формирование ответа в человекочитаемом виде + JSON payload для ingest.

## Этап D: API surface для сценарных ответов

### Новые файлы

- `apps/orchestrator/http_server.py` (опционально, если разделять от текущего FastAPI)

### Изменяемые файлы

- `examples/fastapi_demo/server.py`

### Что реализовать

- Добавить endpoint (минимум один):
  - `POST /ask/packages` с параметрами `product`, `os`, `os_version`, `package_version`.
- Оставить совместимость с `/ask/local` и `/ask/global`.

## Этап E: Sheets-модель под сценарии

### Изменяемые файлы

- `apps/sheets_sync/worker.py`

### Что реализовать

- Новые листы/колонки:
  - `overview` (уже есть),
  - `packages_catalog` (`product`, `package_name`, `package_version`, `format`, `artifact_url`, `source_url`),
  - `scenario_runs` (`scenario_id`, `query`, `duration_ms`, `sources_count`, `records_count`, `status`).

## Этап F: Тесты и приемка

### Новые файлы

- `tests/scenarios/test_scenario1_debian13_versions.py`
- `tests/scenarios/test_scenario2_formats_for_version.py`
- `tests/scenarios/fixtures/registry_templates/*.yaml`
- `tests/scenarios/fixtures/source_snapshots/*`
- `tests/scenarios/test_e2e_bot_to_sheets.py`

### Что проверить

- Scenario 1:
  - возвращается список версий PostgreSQL для Debian 13,
  - для каждой записи есть пакет и ссылка на `.deb`.
- Scenario 2:
  - для PostgreSQL 17.6 возвращаются форматы (deb/rpm/apk),
  - есть пакеты и источники по каждому формату.
- Данные попадают в Memgraph и Google Sheets.

## 4) Минимальный Definition of Done

1. Два сценария проходят e2e без ручных правок.
2. Ответы детерминированы по тестовым снапшотам источников.
3. `overview` и `packages_catalog` в Sheets заполняются автоматически.
4. Регресс-тесты запускаются одной командой:
   - `python -m pytest tests/scenarios -q`

## 5) Предлагаемый порядок работ (риск-минимум)

1. Этап A (registry)
2. Этап B (fetchers/parsers)
3. Этап C (mgr orchestration)
4. Этап F (unit/integration на сценарии)
5. Этап E (расширение Sheets)
6. Этап D (внешний endpoint, если нужен отдельно)

## 6) Ссылки на текущие точки входа

- Bot: `apps/bot/main.py`
- Orchestrator worker: `apps/orchestrator/main.py`
- Ask orchestration: `apps/orchestrator/service.py`
- Ingest worker: `apps/orchestrator/ingest_worker.py`
- Sheets worker: `apps/sheets_sync/worker.py`
- FastAPI demo: `examples/fastapi_demo/server.py`
- Start script: `scripts/start_ollama_stack.ps1`
- Stop script: `scripts/stop_ollama_stack.ps1`
