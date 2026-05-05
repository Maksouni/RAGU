from __future__ import annotations

import asyncio
import logging
import random
import uuid
from dataclasses import dataclass
from typing import Any

import httpx

from apps.common.api_client import ApiClientError, RaguApiClient
from apps.common.bot_messages import EMPTY_QUERY_MESSAGE, START_MESSAGE, TEMP_ERROR_MESSAGE
from apps.common.context import correlation_scope
from apps.common.logging import configure_logging
from apps.common.outbox import OutboxRepository
from apps.common.settings import IntegrationSettings, get_settings
from apps.common.text_limits import truncate_for_message
from apps.orchestrator.service import AskOrchestrator

logger = logging.getLogger(__name__)


class VkApiError(RuntimeError):
    pass


@dataclass(frozen=True)
class VkLongPollServer:
    server: str
    key: str
    ts: str


class VkApiClient:
    def __init__(self, settings: IntegrationSettings) -> None:
        self._settings = settings
        self._client = httpx.AsyncClient(base_url="https://api.vk.com/method", timeout=40.0)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _method(self, method: str, **params: Any) -> dict[str, Any]:
        payload = {
            **params,
            "access_token": self._settings.vk_bot_token,
            "v": self._settings.vk_api_version,
        }
        response = await self._client.post(f"/{method}", data=payload)
        response.raise_for_status()
        data = response.json()
        if "error" in data:
            error = data["error"]
            raise VkApiError(f"{method}: {error.get('error_code')} {error.get('error_msg')}")
        return data.get("response", {})

    async def resolve_group_id(self) -> str:
        configured = self._settings.vk_group_id.strip()
        if configured:
            return configured.lstrip("-")
        response = await self._method("groups.getById")
        if isinstance(response, list) and response:
            return str(response[0]["id"])
        if isinstance(response, dict) and response.get("groups"):
            return str(response["groups"][0]["id"])
        raise VkApiError("VK_GROUP_ID is required when group id cannot be inferred from token")

    async def get_long_poll_server(self, group_id: str) -> VkLongPollServer:
        response = await self._method("groups.getLongPollServer", group_id=group_id)
        return VkLongPollServer(
            server=str(response["server"]),
            key=str(response["key"]),
            ts=str(response["ts"]),
        )

    async def send_message(self, peer_id: int, text: str) -> None:
        await self._method(
            "messages.send",
            peer_id=peer_id,
            random_id=random.randint(1, 2_147_483_647),
            message=text,
        )


class VkLongPollClient:
    def __init__(self, settings: IntegrationSettings, api_client: VkApiClient) -> None:
        self._settings = settings
        self._api_client = api_client
        self._client = httpx.AsyncClient(timeout=settings.vk_long_poll_wait_sec + 10)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def listen(self, server: VkLongPollServer):
        current = server
        while True:
            try:
                response = await self._client.get(
                    current.server,
                    params={
                        "act": "a_check",
                        "key": current.key,
                        "ts": current.ts,
                        "wait": self._settings.vk_long_poll_wait_sec,
                        "mode": 2,
                        "version": 3,
                    },
                )
                response.raise_for_status()
                payload = response.json()
            except Exception:
                logger.exception("VK long poll request failed; reconnecting")
                await asyncio.sleep(3)
                group_id = await self._api_client.resolve_group_id()
                current = await self._api_client.get_long_poll_server(group_id)
                continue

            if "failed" in payload:
                logger.warning("VK long poll failed=%s; reconnecting", payload.get("failed"))
                group_id = await self._api_client.resolve_group_id()
                current = await self._api_client.get_long_poll_server(group_id)
                continue

            current = VkLongPollServer(server=current.server, key=current.key, ts=str(payload.get("ts", current.ts)))
            for update in payload.get("updates", []):
                yield update


async def _handle_message(
    *,
    update: dict[str, Any],
    vk_api: VkApiClient,
    orchestrator: AskOrchestrator,
) -> None:
    if update.get("type") != "message_new":
        return
    message = update.get("object", {}).get("message", {})
    text = str(message.get("text") or "").strip()
    peer_id = int(message.get("peer_id") or 0)
    from_id = str(message.get("from_id") or "unknown")
    if not peer_id:
        return

    if text.lower() in {"/start", "start", "начать"}:
        await vk_api.send_message(peer_id, START_MESSAGE)
        return

    correlation_id = str(uuid.uuid4())
    with correlation_scope(correlation_id):
        try:
            result = await orchestrator.handle_user_message(
                raw_text=text,
                chat_id=f"vk:{peer_id}",
                user_id=f"vk:{from_id}",
                correlation_id=correlation_id,
            )
            await vk_api.send_message(peer_id, truncate_for_message(result.answer, limit=3500))
        except ValueError:
            await vk_api.send_message(peer_id, EMPTY_QUERY_MESSAGE)
        except ApiClientError as exc:
            logger.exception("API error while handling VK message")
            await vk_api.send_message(peer_id, f"Ошибка API: {exc}")
        except Exception:
            logger.exception("Unexpected VK bot handler error")
            await vk_api.send_message(peer_id, TEMP_ERROR_MESSAGE)


async def run_vk_bot() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    logging.getLogger("httpx").setLevel(logging.WARNING)

    if not settings.vk_bot_token:
        raise RuntimeError("VK_BOT_TOKEN is required")

    outbox = OutboxRepository(settings.resolve_outbox_db_path())
    ragu_api = RaguApiClient(settings)
    orchestrator = AskOrchestrator(settings=settings, api_client=ragu_api, outbox=outbox)
    vk_api = VkApiClient(settings)
    long_poll = VkLongPollClient(settings, vk_api)

    try:
        group_id = await vk_api.resolve_group_id()
        server = await vk_api.get_long_poll_server(group_id)
        logger.info("VK bot long polling started group_id=%s", group_id)
        async for update in long_poll.listen(server):
            await _handle_message(update=update, vk_api=vk_api, orchestrator=orchestrator)
    finally:
        await long_poll.aclose()
        await vk_api.aclose()
        await orchestrator.aclose()
        await ragu_api.aclose()


if __name__ == "__main__":
    asyncio.run(run_vk_bot())
