# FastAPI Demo

This demo runs a GraphRAG-style API service with:

- `grg`: FastAPI app from `server.py`
- `knb`: ArcadeDB backend for graph storage
- `vllm`: OpenAI-compatible LLM endpoint
- `vllm-embed`: OpenAI-compatible embeddings endpoint

The stack is defined in `examples/fastapi_demo/docker-compose.yml`.

## What This Demo Exposes

The service listens on `http://127.0.0.1:8000` and provides:

- `POST /ingest/json` - ingest JSON objects or plain text strings
- `POST /ask/local` - local graph-aware search
- `POST /ask/global` - global (community-level) search
- `GET /status` - indexing status

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

