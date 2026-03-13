FROM ghcr.io/astral-sh/uv:0.10.8-python3.13-trixie

WORKDIR /app

COPY pyproject.toml .

RUN uv pip install --system -e .

COPY . .

CMD ["python", "server.py"]