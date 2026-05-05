from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from apps.common.api_client import RaguApiClient  # noqa: E402
from apps.common.outbox import OutboxRepository  # noqa: E402
from apps.common.settings import get_settings  # noqa: E402
from apps.orchestrator.service import AskOrchestrator  # noqa: E402


async def main() -> None:
    settings = get_settings()
    api_client = RaguApiClient(settings)
    outbox = OutboxRepository(settings.resolve_outbox_db_path())
    orchestrator = AskOrchestrator(settings, api_client, outbox)
    try:
        result = await orchestrator.handle_user_message(
            "если не написал llm nollm что по умолчанию работает",
            chat_id="diagnostic-chat",
            user_id="diagnostic-user",
            correlation_id="diagnostic-correlation",
        )
        print(f"response_mode={result.response_mode}")
        print(f"first_line={result.answer.splitlines()[0]}")
    finally:
        await orchestrator.aclose()
        await api_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
