FROM ghcr.io/astral-sh/uv:0.10.8-python3.13-trixie

WORKDIR /app

COPY pyproject.toml ./
COPY ragu ./ragu
COPY tests ./tests

# install project
RUN uv pip install --system .

# install test deps
RUN uv pip install --system pytest pytest-asyncio pytest-cov

CMD ["pytest", "-q"]