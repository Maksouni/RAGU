from __future__ import annotations

from typing import Any

import httpx

from apps.common.settings import AnswerMode, AskMode, IntegrationSettings


class ApiClientError(RuntimeError):
    pass


class RaguApiClient:
    def __init__(self, settings: IntegrationSettings) -> None:
        self._settings = settings
        self._client = httpx.AsyncClient(base_url=settings.api_base_url.rstrip("/"), timeout=None)

    async def ask(self, question: str, mode: AskMode, answer_mode: AnswerMode = "auto") -> dict[str, Any]:
        response = await self._client.post(
            f"/ask/{mode}",
            json={"question": question, "answer_mode": answer_mode},
            timeout=self._settings.ask_timeout_sec,
        )
        if response.status_code >= 400:
            raise ApiClientError(f"/ask/{mode} failed with {response.status_code}: {response.text}")
        return response.json()

    async def ingest_json(self, docs: list[dict[str, Any]]) -> httpx.Response:
        return await self._client.post(
            "/ingest/json",
            json={"data": docs},
            timeout=self._settings.ingest_timeout_sec,
        )

    async def beautify_answer(self, question: str, structured_answer: str) -> dict[str, Any]:
        response = await self._client.post(
            "/answer/llm",
            json={"question": question, "structured_answer": structured_answer},
            timeout=self._settings.ask_timeout_sec,
        )
        if response.status_code >= 400:
            raise ApiClientError(f"/answer/llm failed with {response.status_code}: {response.text}")
        return response.json()

    async def aclose(self) -> None:
        await self._client.aclose()
