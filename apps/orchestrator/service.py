from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime, timezone

from apps.common.api_client import ApiClientError, RaguApiClient
from apps.common.models import AskExchangeEvent, AskResult
from apps.common.outbox import OutboxRepository
from apps.common.routing import route_mode_and_question
from apps.common.settings import IntegrationSettings
from apps.orchestrator.intent_guard import INVALID_QUERY_MESSAGE, is_supported_package_query
from apps.orchestrator.scenario_manager import ScenarioManager
from apps.registry.repository import RegistryRepository
from apps.scraper.service import PackageScraperService

logger = logging.getLogger(__name__)


class AskOrchestrator:
    def __init__(
        self,
        settings: IntegrationSettings,
        api_client: RaguApiClient,
        outbox: OutboxRepository,
        scenario_manager: ScenarioManager | None = None,
    ) -> None:
        self._settings = settings
        self._api_client = api_client
        self._outbox = outbox
        self._scenario_manager = scenario_manager or ScenarioManager(
            registry=RegistryRepository(),
            scraper=PackageScraperService(timeout_sec=settings.ask_timeout_sec),
        )

    async def aclose(self) -> None:
        scraper = getattr(self._scenario_manager, "_scraper", None)
        if scraper is not None and hasattr(scraper, "aclose"):
            await scraper.aclose()

    async def handle_user_message(
        self,
        raw_text: str,
        chat_id: str,
        user_id: str,
        correlation_id: str,
    ) -> AskResult:
        started_at = time.perf_counter()
        routed = route_mode_and_question(
            raw_text,
            default_mode=self._settings.default_ask_mode,
            default_answer_mode=self._settings.default_answer_mode,
        )
        if not routed.question:
            raise ValueError("Question is empty after mode parsing.")
        if not is_supported_package_query(routed.question):
            response_time_ms = int((time.perf_counter() - started_at) * 1000)
            return AskResult(
                question=routed.question,
                answer=INVALID_QUERY_MESSAGE,
                requested_mode=routed.mode,
                answer_mode=routed.answer_mode,
                response_mode="invalid_query",
                response_time_ms=response_time_ms,
            )

        answer = ""
        response_mode = "unknown"
        response_metadata: dict[str, object] = {}

        if not routed.mode_explicit and not routed.answer_mode_explicit:
            cached = self._outbox.find_recent_answer(routed.question)
            if cached:
                answer = cached.answer
                response_mode = "local_cache"
                response_metadata = {
                    "cache_hit": True,
                    "cache_source_event_id": cached.event_id,
                }

        effective_mode = routed.mode if routed.mode_explicit else "local"
        if not answer:
            scenario_result = await self._scenario_manager.handle_if_supported(
                routed.question,
                requested_mode=effective_mode,
                answer_mode=routed.answer_mode,
            )
            if scenario_result.handled:
                answer = scenario_result.answer
                response_mode = f"registry_scrape_{effective_mode}"
                response_metadata = {
                    **scenario_result.metadata,
                    "cache_hit": False,
                    "answer_mode": routed.answer_mode,
                }
                if routed.answer_mode == "llm":
                    try:
                        beautified = await self._api_client.beautify_answer(
                            routed.question,
                            scenario_result.answer,
                        )
                        llm_answer = str(beautified.get("answer", "")).strip()
                        if llm_answer:
                            answer = llm_answer
                            response_mode = f"registry_scrape_llm_{effective_mode}"
                            response_metadata["llm_formatter"] = True
                    except Exception as exc:
                        logger.exception("LLM answer formatter failed, returning structured answer")
                        response_metadata["llm_formatter"] = False
                        response_metadata["llm_formatter_error"] = str(exc)[:300]
            else:
                logger.info("Dispatching ask request to mode=%s", effective_mode)
                response_payload = await self._api_client.ask(
                    routed.question,
                    mode=effective_mode,
                    answer_mode=routed.answer_mode,
                )
                answer = str(response_payload.get("answer", "")).strip()
                response_mode = str(response_payload.get("mode", "unknown"))
                response_metadata = {
                    "cache_hit": False,
                    "answer_mode": response_payload.get("answer_mode", routed.answer_mode),
                }
        if not answer:
            raise ApiClientError("Ask API returned empty answer.")
        response_time_ms = int((time.perf_counter() - started_at) * 1000)
        response_metadata["response_time_ms"] = response_time_ms

        event = AskExchangeEvent(
            event_id=self._build_event_id(chat_id, user_id, effective_mode, routed.question, answer, correlation_id),
            question=routed.question,
            answer=answer,
            mode=effective_mode,
            user_id=user_id,
            chat_id=chat_id,
            correlation_id=correlation_id,
            timestamp=datetime.now(timezone.utc),
            metadata={
                "requested_mode": routed.mode,
                "effective_mode": effective_mode,
                "mode_explicit": routed.mode_explicit,
                "requested_answer_mode": routed.answer_mode,
                "answer_mode_explicit": routed.answer_mode_explicit,
                "response_mode": response_mode,
                **response_metadata,
            },
        )
        inserted = self._outbox.enqueue_event(event)
        logger.info("Outbox event persisted event_id=%s inserted=%s", event.event_id, inserted)

        return AskResult(
            question=routed.question,
            answer=answer,
            requested_mode=routed.mode,
            answer_mode=routed.answer_mode,
            response_mode=response_mode,
            response_time_ms=response_time_ms,
        )

    @staticmethod
    def _build_event_id(
        chat_id: str,
        user_id: str,
        mode: str,
        question: str,
        answer: str,
        correlation_id: str,
    ) -> str:
        raw = "|".join([chat_id, user_id, mode, question, answer, correlation_id])
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()
