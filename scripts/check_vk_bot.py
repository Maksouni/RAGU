from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from apps.common.settings import get_settings
from apps.vk_bot.main import VkApiClient


async def main() -> None:
    settings = get_settings()
    client = VkApiClient(settings)
    try:
        group_id = await client.resolve_group_id()
        server = await client.get_long_poll_server(group_id)
        print(f"group_id={group_id}")
        print(f"long_poll_server={bool(server.server)}")
        print(f"long_poll_key={bool(server.key)}")
        print(f"long_poll_ts={bool(server.ts)}")
    finally:
        await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
