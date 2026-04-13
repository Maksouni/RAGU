from __future__ import annotations

import logging

from apps.common.api_client import RaguApiClient
from apps.common.outbox import OutboxRepository
from apps.common.settings import IntegrationSettings

logger = logging.getLogger(__name__)


class IngestOutboxWorker:
    def __init__(
        self,
        settings: IntegrationSettings,
        api_client: RaguApiClient,
        outbox: OutboxRepository,
    ) -> None:
        self._settings = settings
        self._api_client = api_client
        self._outbox = outbox

    def _backoff_seconds(self, attempts: int) -> int:
        base = self._settings.ingest_backoff_base_sec
        max_backoff = self._settings.ingest_backoff_max_sec
        return min(max_backoff, base * (2 ** max(0, attempts)))

    async def flush_once(self) -> int:
        batch = self._outbox.fetch_pending_ingest(limit=self._settings.ingest_batch_size)
        if not batch:
            return 0

        docs = [record.event.to_ingest_document() for record in batch]
        event_ids = [record.event.event_id for record in batch]

        try:
            response = await self._api_client.ingest_json(docs)
        except Exception as exc:
            logger.exception("Ingest request failed. records=%s", len(batch))
            for record in batch:
                self._outbox.mark_ingest_retry(
                    event_id=record.event.event_id,
                    error_text=f"network: {exc}",
                    backoff_seconds=self._backoff_seconds(record.attempts),
                    max_attempts=self._settings.ingest_max_attempts,
                )
            return 0

        if response.status_code == 409:
            logger.warning("Indexer is busy (409). Scheduling retries for %s records", len(batch))
            for record in batch:
                self._outbox.mark_ingest_retry(
                    event_id=record.event.event_id,
                    error_text="indexer busy",
                    backoff_seconds=self._backoff_seconds(record.attempts),
                    max_attempts=self._settings.ingest_max_attempts,
                )
            return 0

        if response.status_code >= 400:
            logger.error("Ingest failed status=%s body=%s", response.status_code, response.text[:500])
            for record in batch:
                self._outbox.mark_ingest_retry(
                    event_id=record.event.event_id,
                    error_text=f"status={response.status_code}: {response.text[:500]}",
                    backoff_seconds=self._backoff_seconds(record.attempts),
                    max_attempts=self._settings.ingest_max_attempts,
                )
            return 0

        self._outbox.mark_ingested(event_ids=event_ids, ingest_status_code=response.status_code)
        logger.info("Ingest success for %s records", len(batch))
        return len(batch)

