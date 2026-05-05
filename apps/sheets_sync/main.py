from __future__ import annotations

import logging
import time

from apps.common.logging import configure_logging
from apps.common.outbox import OutboxRepository
from apps.common.settings import get_settings
from apps.sheets_sync.worker import GoogleSheetsBackend, SheetsSyncWorker

logger = logging.getLogger(__name__)


def run_sheets_sync() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)

    outbox = OutboxRepository(settings.resolve_outbox_db_path())
    backend = GoogleSheetsBackend(settings=settings)
    worker = SheetsSyncWorker(settings=settings, outbox=outbox, backend=backend)
    logger.info("Sheets sync worker started interval=%ss", settings.sheets_sync_interval_sec)

    while True:
        try:
            worker.sync_once()
        except Exception:
            logger.exception("Sheets sync loop iteration failed")
        time.sleep(settings.sheets_sync_interval_sec)


if __name__ == "__main__":
    run_sheets_sync()
