from __future__ import annotations

import asyncio
import logging

from apps.common.api_client import RaguApiClient
from apps.common.logging import configure_logging
from apps.common.outbox import OutboxRepository
from apps.common.settings import get_settings
from apps.orchestrator.ingest_worker import IngestOutboxWorker

logger = logging.getLogger(__name__)


async def run_orchestrator() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)

    outbox = OutboxRepository(settings.resolve_outbox_db_path())
    api_client = RaguApiClient(settings)
    worker = IngestOutboxWorker(settings=settings, api_client=api_client, outbox=outbox)

    logger.info("Orchestrator worker started interval=%ss", settings.ingest_interval_sec)

    try:
        while True:
            try:
                await worker.flush_once()
            except Exception:
                logger.exception("Unhandled orchestrator iteration error")
            await asyncio.sleep(settings.ingest_interval_sec)
    finally:
        await api_client.aclose()


if __name__ == "__main__":
    asyncio.run(run_orchestrator())
