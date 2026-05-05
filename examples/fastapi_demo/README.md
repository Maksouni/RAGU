# FastAPI Demo

This demo runs a GraphRAG-style API service with:

- `grg`: FastAPI app from `server.py`
- `memgraph`: Memgraph backend for graph storage
- `vllm`: OpenAI-compatible LLM endpoint
- `vllm-embed`: OpenAI-compatible embeddings endpoint

The stack is defined in `examples/fastapi_demo/docker-compose.yml`.

## What This Demo Exposes

The service listens on `http://127.0.0.1:8000` and provides:

- `POST /ingest/json` - ingest JSON objects or plain text strings
- `POST /ask/local` - local graph-aware search
- `POST /ask/global` - global (community-level) search
- `GET /status` - indexing status

`/ask/local` and `/ask/global` accept `answer_mode` in the request body:

- `auto` - use `.env` (`DISABLE_LLM_ANSWERS`)
- `llm` - force LLM answer generation for this request
- `no_llm` - force template answer without LLM generation; embeddings are still used for semantic search

## Bot Integrations

The integration layer can run Telegram, VK, both bots, or no bot from `.env`:

```dotenv
BOT_PLATFORM=telegram   # telegram | vk | both | none
TELEGRAM_BOT_TOKEN=
VK_BOT_TOKEN=
VK_GROUP_ID=
VK_API_VERSION=5.199
VK_LONG_POLL_WAIT_SEC=25
```

Users can ask questions as normal text. `/local` and `/global` are still supported for compatibility,
but the default flow uses `DEFAULT_ASK_MODE=local`, so users do not need to type a mode before every query.

Answer style can be switched per message:

- `/llm <question>` - generated natural answer
- `/nollm <question>` - deterministic structured answer without LLM generation

## Prerequisites

- Docker + Docker Compose
- NVIDIA GPU with working container runtime (required by `vllm` services in current compose file)

## Run

From repository root:

```bash
docker compose -f examples/fastapi_demo/docker-compose.yml up --build
```

When startup is complete, you should see the API at:

- `http://127.0.0.1:8000`
- Swagger UI: `http://127.0.0.1:8000/docs`

## Notes

- Ingestion runs in the background; new ingestion requests return `409` while indexing is busy.
- Empty ingestion payloads return `400`.
- If service initialization has not finished, query endpoints may return `503`.
- Memgraph Bolt endpoint is available at `127.0.0.1:7687`.
- Optional Memgraph connection env vars for `grg`: `MEMGRAPH_URI`, `MEMGRAPH_DATABASE`, `MEMGRAPH_USERNAME`, `MEMGRAPH_PASSWORD`.
- For laptop-friendly local runs, keep `EMBEDDING_DIM=20`; vectors are truncated to this size even if the embedding model returns a larger vector.

