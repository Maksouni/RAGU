from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import Iterator

_CORRELATION_ID: ContextVar[str] = ContextVar("correlation_id", default="-")


def get_correlation_id() -> str:
    return _CORRELATION_ID.get()


def set_correlation_id(value: str) -> Token[str]:
    return _CORRELATION_ID.set(value)


def reset_correlation_id(token: Token[str]) -> None:
    _CORRELATION_ID.reset(token)


@contextmanager
def correlation_scope(correlation_id: str) -> Iterator[None]:
    token = set_correlation_id(correlation_id)
    try:
        yield
    finally:
        reset_correlation_id(token)

