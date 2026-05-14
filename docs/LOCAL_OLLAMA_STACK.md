# Local Runbook: LLM Provider + Memgraph + RAGU Demo

## What Runs In The Demo

- FastAPI demo: `examples/fastapi_demo/server.py`
- Orchestrator worker: routes bot requests, parser/scenario logic, outbox ingest
- Memgraph in Docker: graph storage for queries, answers, artifacts, and relations
- Embeddings: enabled independently from LLM answers, capped to `EMBEDDING_DIM <= 20`
- Optional LLM provider: local Ollama or external Mistral/OpenAI-compatible endpoint; it only formats prepared context and does not scrape, browse, or search by itself
- `sheets_sync`: async Google Sheets writer for `overview`, `queries`, `answers`, `ingest_jobs`, `errors_audit`
- Telegram bot, VK bot, both, or no bot according to `BOT_PLATFORM`

## One-Time Setup

1. Install Docker Desktop.
2. Install Ollama if `LLM_PROVIDER=ollama`. For `LLM_PROVIDER=mistral`, a Mistral API key is enough.
3. Create virtual environment and install dependencies:

```powershell
cd C:\Users\mrmar\PycharmProjects\RAGU
python -m venv venv
.\venv\Scripts\python.exe -m pip install -e . fastapi==0.110.2 uvicorn==0.29.0 httpx==0.27.0
```

4. Configure `.env` in the repo root. Do not commit `.env`, bot tokens, or Google service account JSON.

## Required `.env` Highlights

```dotenv
BOT_PLATFORM=telegram          # telegram | vk | both | none
API_BASE_URL=http://127.0.0.1:8000
OUTBOX_DB_PATH=ragu_working_dir/integration/outbox.sqlite

LLM_PROVIDER=ollama            # ollama | mistral | custom
DISABLE_LLM_ANSWERS=false      # false -> default /llm style; true -> default /nollm style
API_KEY=local
BASE_URL=http://127.0.0.1:11434/v1
EMBEDDING_BASE_URL=http://127.0.0.1:11434/v1
LLM_MODEL_NAME=qwen2.5:3b
EMBEDDER_MODEL_NAME=nomic-embed-text
EMBEDDING_DIM=20               # values above 20 are capped by the API service

TELEGRAM_BOT_TOKEN=...
VK_BOT_TOKEN=...
VK_GROUP_ID=...

SHEETS_SYNC_ENABLED=true
GOOGLE_SHEETS_SPREADSHEET_ID=...
GOOGLE_SERVICE_ACCOUNT_JSON_PATH=C:\secure\service-account.json
```

Light laptop mode with Mistral API:

```dotenv
LLM_PROVIDER=mistral
MISTRAL_API_KEY=PASTE_KEY_HERE
DISABLE_LLM_ANSWERS=false
BASE_URL=https://api.mistral.ai/v1
EMBEDDING_BASE_URL=https://api.mistral.ai/v1
LLM_MODEL_NAME=mistral-small-latest
EMBEDDER_MODEL_NAME=mistral-embed
EMBEDDING_DIM=20
```

In this mode `start_ollama_stack.ps1` skips local Ollama startup and model checks, but still starts Docker/Memgraph/FastAPI/orchestrator/sheets/bots. Mistral exposes chat completions and embeddings under `/v1`, so the existing OpenAI-compatible clients can be reused.

Google Sheets requirements:

- The JSON file must be a Google service account key.
- The service account email must have edit access to the spreadsheet.
- Verify writes with:

```powershell
.\venv\Scripts\python.exe scripts\check_google_sheets.py
```

## Start Everything

```powershell
cd C:\Users\mrmar\PycharmProjects\RAGU
.\scripts\start_ollama_stack.ps1
```

The script loads `.env`, starts Memgraph/FastAPI/orchestrator/sheets_sync, and starts Telegram and/or VK according to `BOT_PLATFORM`. Keep the existing console links to Swagger and `/status`; they are the primary handoff links for the demo.

## Health Checks

FastAPI status:

```powershell
Invoke-RestMethod -Uri "http://127.0.0.1:8000/status" -Method Get
```

Full local diagnostic summary:

```powershell
.\venv\Scripts\python.exe scripts\check_demo_health.py
.\venv\Scripts\python.exe scripts\check_demo_health.py --json
```

The diagnostic reports `OK`, `WARN`, or `FAIL` for:

- FastAPI `/status`
- orchestrator process
- Memgraph Bolt
- `sheets_sync`
- Telegram bot when `BOT_PLATFORM=telegram|both`
- VK bot when `BOT_PLATFORM=vk|both`
- LLM endpoint when `DISABLE_LLM_ANSWERS=false`

Components disabled by `.env` are reported as `WARN`, not as a full failure.

## Bot Usage

`/start` and `/help` show the current instruction in Telegram and VK.

- Package queries use parser/scraper/registry first.
- General IT questions route to local semantic/graph search over the accumulated RAGU knowledge base.
- `/nollm <query>`: deterministic template or semantic graph answer, no generative model.
- `/llm <query>`: same prepared package/graph context, then LLM formats the answer.
- Plain query: uses `.env`.

Default answer mode:

- `DISABLE_LLM_ANSWERS=true` -> plain queries behave as no-LLM.
- `DISABLE_LLM_ANSWERS=false` -> plain queries use LLM formatting when available.

Important: LLM never performs scraping or web search. External source collection is done by the registry/scraper layer before the LLM sees the prepared context.

Demo queries:

```text
/nollm Python 3.12 для Ubuntu limit=10
/llm дай список всех пакетов PostgreSQL 17.6 limit=13 show=4
найди для debian 13 все версии PostgreSQL limit=10
```

Unsupported source example:

```text
скачай Foo Package для Solaris format=deb
```

Expected behavior: clear unsupported-source message, no random semantic fallback from old graph data, no successful-result pollution.

## API Checks

Swagger:

```text
http://127.0.0.1:8000/docs
```

Manual no-LLM API query:

```powershell
$q = @{ question = "Python 3.12 для Ubuntu limit=10"; answer_mode = "no_llm" } | ConvertTo-Json
Invoke-RestMethod -Uri "http://127.0.0.1:8000/ask/local" -Method Post -ContentType "application/json" -Body $q
```

Manual LLM API query:

```powershell
$q = @{ question = "дай список всех пакетов PostgreSQL 17.6 limit=13 show=4"; answer_mode = "llm" } | ConvertTo-Json
Invoke-RestMethod -Uri "http://127.0.0.1:8000/ask/local" -Method Post -ContentType "application/json" -Body $q
```

## Graph Demo

Successful ask events are written through the outbox and ingested into Memgraph as:

- `AskExchange`
- `UserQuery`
- `StructuredAnswer`
- `PackageArtifact`
- relations such as `HAS_QUESTION`, `ANSWERED_BY`, `FOUND_ARTIFACT`

Useful Memgraph Lab queries:

```cypher
MATCH (n)-[r]->(m)
RETURN n, r, m
LIMIT 100;
```

```cypher
MATCH (q:UserQuery)-[:ANSWERED_BY]->(a:StructuredAnswer)
RETURN q, a
ORDER BY q.entity_name
LIMIT 25;
```

```cypher
MATCH (e:AskExchange)-[:FOUND_ARTIFACT]->(p:PackageArtifact)
RETURN e, p
LIMIT 50;
```

HTML visualization:

```powershell
.\venv\Scripts\python.exe scripts\visualize_knowledge_graph.py --output .run\demo_graph.html --summary-output .run\demo_graph_summary.json
```

## Stop Everything

```powershell
cd C:\Users\mrmar\PycharmProjects\RAGU
.\scripts\stop_ollama_stack.ps1
```

The stop script stops only project-managed processes from `.run/*.pid` and avoids killing unrelated stale PIDs.

## Final Demo Checklist

1. Docker Desktop is running.
2. `.\scripts\start_ollama_stack.ps1` finishes without errors.
3. `/status` is reachable and `embedding_dim <= 20`.
4. `scripts\check_demo_health.py` has no required `FAIL`.
5. The configured bot starts according to `BOT_PLATFORM`.
6. `/start` or `/help` shows the current instruction.
7. `/nollm Python 3.12 для Ubuntu limit=10` returns a template answer.
8. `/llm дай список всех пакетов PostgreSQL 17.6 limit=13 show=4` returns a visibly different formatted answer.
9. Memgraph contains query, answer, artifact, and relation nodes.
10. Google Sheets receives rows in the expected tabs.
11. Unsupported-source request returns a clear unsupported message.
12. `.\scripts\stop_ollama_stack.ps1` shuts down project processes cleanly.
