FROM ghcr.io/astral-sh/uv:0.10.8-python3.13-trixie AS builder

WORKDIR /src

COPY pyproject.toml ./
COPY ragu ./ragu
RUN uv build --wheel


FROM ghcr.io/astral-sh/uv:0.10.8-python3.13-trixie AS runtime

WORKDIR /app

COPY --from=builder /src/dist/*.whl /tmp/
RUN uv pip install --system /tmp/*.whl && rm -f /tmp/*.whl
RUN uv pip install --system fastapi==0.110.2 uvicorn==0.29.0 httpx==0.27.0

COPY examples/fastapi_demo/server.py /app/server.py

CMD ["python", "server.py"]