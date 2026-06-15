FROM ghcr.io/astral-sh/uv:0.10.8-python3.13-trixie AS builder

WORKDIR /src

COPY pyproject.toml ./
COPY ragu ./ragu
COPY apps ./apps
RUN uv build --wheel


FROM ghcr.io/astral-sh/uv:0.10.8-python3.13-trixie AS runtime

WORKDIR /app

COPY --from=builder /src/dist/*.whl /tmp/
RUN uv pip install --system /tmp/*.whl && rm -f /tmp/*.whl
RUN uv pip install --system fastapi==0.110.2 uvicorn==0.29.0 httpx==0.28.1 neo4j==5.28.1 python-dotenv==1.0.1 gspread==6.2.1 google-auth==2.41.1

COPY examples/fastapi_demo/server.py /app/server.py

CMD ["python", "server.py"]
