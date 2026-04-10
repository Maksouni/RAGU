# Local Runbook: Ollama + Memgraph + RAGU

## What Is Running Right Now

- `Ollama`:
  - LLM (`qwen2.5:3b`)
  - Embeddings (`nomic-embed-text`)
- `Memgraph` in Docker: graph storage
- `FastAPI demo` (`examples/fastapi_demo/server.py`): RAG entrypoint

This is the working core for your future architecture:

- Telegram bot -> API request
- Scraper/API ingester -> sends data to RAG
- Graph + vectors in RAGU/Memgraph
- Answers back to bot/API
- Export selected data to Google Sheets

## 0. One-Time Setup

1. Install Docker Desktop.
2. Install Ollama.
3. Create virtual environment and install dependencies:

```powershell
cd C:\Users\mrmar\PycharmProjects\RAGU
python -m venv venv
.\venv\Scripts\python.exe -m pip install -e . fastapi==0.110.2 uvicorn==0.29.0 httpx==0.27.0
```

4. Configure `.env` in repo root:

`C:\Users\mrmar\PycharmProjects\RAGU\.env`

## 1. Start Everything (Recommended)

```powershell
cd C:\Users\mrmar\PycharmProjects\RAGU
.\scripts\start_ollama_stack.ps1
```

Script will:

1. Read `.env`.
2. Ensure Ollama models exist (pull if missing).
3. Start Memgraph container.
4. Start FastAPI service.
5. Wait for `/status` readiness.

## 2. Stop Everything

```powershell
cd C:\Users\mrmar\PycharmProjects\RAGU
.\scripts\stop_ollama_stack.ps1
```

## 3. Verify API

- Swagger: `http://127.0.0.1:8000/docs`
- Health: `http://127.0.0.1:8000/status`

Example:

```powershell
Invoke-RestMethod -Uri "http://127.0.0.1:8000/status" -Method Get
```

## 4. Basic Usage Flow

1. Ingest source data:

```powershell
$body = @{ data = @("Иван основал Альфа в 2019 году.") } | ConvertTo-Json
Invoke-RestMethod -Uri "http://127.0.0.1:8000/ingest/json" -Method Post -ContentType "application/json" -Body $body
```

2. Ask question:

```powershell
$q = @{ question = "Кто основал Альфа?" } | ConvertTo-Json
Invoke-RestMethod -Uri "http://127.0.0.1:8000/ask/local" -Method Post -ContentType "application/json" -Body $q
```

## 5. Where To Put Telegram Token

In root `.env`:

```dotenv
TELEGRAM_BOT_TOKEN=PASTE_TELEGRAM_BOT_TOKEN_HERE
```

This repo currently has the RAG core and API demo. Telegram bot / scraper / Google Sheets sync should be added as integration layer around this API.

## 5.1 Fast Mode Without LLM (for development)

If you want instant responses in `/ask/local` and `/ask/global` without model generation, set in `.env`:

```dotenv
DISABLE_LLM_ANSWERS=true
```

Then restart stack with `start_ollama_stack.ps1`.

In this mode, API returns graph-based heuristic summary (`mode=no_llm`) instead of generated text.

## 6. Planned Integration Mapping

Suggested modules (next step):

- `apps/bot/` - Telegram bot (`python-telegram-bot` or `aiogram`)
- `apps/scraper/` - scheduled collectors
- `apps/sheets_sync/` - export/import with Google Sheets API
- `apps/orchestrator/` - routes events between bot, scraper, and RAG API

Current RAG API can stay as:

- `POST /ingest/json`
- `POST /ask/local`
- `POST /ask/global`
- `GET /status`
