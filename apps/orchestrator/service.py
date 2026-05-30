from __future__ import annotations

import hashlib
import logging
import os
import re
import time
from datetime import datetime, timezone

from apps.common.api_client import ApiClientError, RaguApiClient
from apps.common.models import AskExchangeEvent, AskResult
from apps.common.outbox import OutboxRepository
from apps.common.routing import route_mode_and_question
from apps.common.settings import IntegrationSettings
from apps.orchestrator.intent_guard import (
    INVALID_QUERY_MESSAGE,
    is_supported_general_it_query,
    is_supported_package_query,
)
from apps.orchestrator.scenario_manager import ScenarioManager
from apps.registry.repository import RegistryRepository
from apps.scraper.service import PackageScraperService

logger = logging.getLogger(__name__)

_CONTEXT_REFERENCE_RE = re.compile(
    r"(?:\b(?:это|этот|этого|этой|эту|его|нее|него|that|it|same|previous|latest)\b|"
    r"предыдущ\w*|последн\w*|такой же)",
    re.IGNORECASE,
)
_LATEST_VERSION_RE = re.compile(
    r"(?:последн\w*|новейш\w*|свеж\w*|latest|newest).{0,50}(?:верс\w*|version)|"
    r"(?:верс\w*|version).{0,50}(?:последн\w*|новейш\w*|свеж\w*|latest|newest)",
    re.IGNORECASE,
)
_PRODUCT_PARAM_RE = re.compile(r"\bproduct\s*=\s*(?P<product>[a-z0-9+_.-]+)\b", re.IGNORECASE)
_KNOWN_SUBJECT_RE = re.compile(
    r"\b(?P<subject>redis|postgresql|postgres|python|docker|nginx|kubernetes|k8s|linux|debian|ubuntu)\b",
    re.IGNORECASE,
)
_SUBJECT_ALIASES = {
    "postgres": "postgresql",
    "k8s": "kubernetes",
}


def _needs_dialog_context(question: str) -> bool:
    value = (question or "").strip()
    if not value:
        return False
    return bool(_CONTEXT_REFERENCE_RE.search(value))


def _is_latest_version_followup(question: str) -> bool:
    return bool(_LATEST_VERSION_RE.search(question or ""))


def _normalize_subject(value: str) -> str:
    subject = value.strip().lower()
    return _SUBJECT_ALIASES.get(subject, subject)


def _extract_subject_from_text(text: str) -> str | None:
    product_match = _PRODUCT_PARAM_RE.search(text or "")
    if product_match:
        return _normalize_subject(product_match.group("product"))
    subject_match = _KNOWN_SUBJECT_RE.search(text or "")
    if subject_match:
        return _normalize_subject(subject_match.group("subject"))
    return None


def _extract_subject_from_event(previous: AskExchangeEvent) -> str | None:
    product = previous.metadata.get("product") if previous.metadata else None
    if isinstance(product, str) and product.strip():
        return _normalize_subject(product)
    return _extract_subject_from_text(f"{previous.question}\n{previous.answer}")


def _build_contextual_question(question: str, previous: AskExchangeEvent | None) -> str:
    if previous is None or not _needs_dialog_context(question):
        return question
    if _is_latest_version_followup(question):
        subject = _extract_subject_from_text(question) or _extract_subject_from_event(previous)
        if subject:
            return f"product={subject} latest version {question}"
    previous_question = " ".join(previous.question.split())
    if not previous_question:
        return question
    return f"{previous_question}. {question}"


def _default_answer_mode_from_env() -> str:
    raw = os.getenv("DISABLE_LLM_ANSWERS", "false").strip().lower()
    return "no_llm" if raw in {"1", "true", "yes", "on"} else "llm"


def _is_no_reliable_context_answer(answer: str) -> bool:
    return "не найдено достаточно релевантной" in answer.lower()


def _strip_saved_answer_headers(answer: str) -> str:
    cleaned = re.sub(
        r"^\s*LLM режим:.*?Внутренний режим:\s*[^.]+?\.\s*",
        "",
        answer.strip(),
        flags=re.IGNORECASE | re.DOTALL,
    ).strip()
    lines = cleaned.splitlines()
    while lines and (
        not lines[0].strip()
        or lines[0].startswith("LLM режим:")
        or lines[0].startswith("Внутренний режим:")
    ):
        lines.pop(0)
    return "\n".join(lines).strip() or answer.strip()


def _format_cached_no_llm_answer(cached: AskExchangeEvent) -> str:
    return "\n".join(
        [
            "NO-LLM ответ из сохраненной базы",
            f"Генерация LLM: выключена; поиск: exact_outbox:{cached.event_id}.",
            "",
            _strip_saved_answer_headers(cached.answer),
        ]
    )


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
        effective_answer_mode = "no_llm" if routed.db_only else (
            _default_answer_mode_from_env() if routed.answer_mode == "auto" else routed.answer_mode
        )
        if not routed.question:
            raise ValueError("Question is empty after mode parsing.")
        find_recent_context = getattr(self._outbox, "find_recent_context", None)
        previous_context = find_recent_context(chat_id=chat_id, user_id=user_id) if callable(find_recent_context) else None
        processing_question = _build_contextual_question(routed.question, previous_context)
        is_package_query = is_supported_package_query(processing_question)
        is_general_it_query = is_supported_general_it_query(processing_question)
        if not is_package_query and not is_general_it_query:
            response_time_ms = int((time.perf_counter() - started_at) * 1000)
            return AskResult(
                question=routed.question,
                answer=INVALID_QUERY_MESSAGE,
                requested_mode=routed.mode,
                answer_mode=effective_answer_mode,
                response_mode="invalid_query",
                response_time_ms=response_time_ms,
            )

        answer = ""
        response_mode = "unknown"
        response_metadata: dict[str, object] = {
            "db_only": routed.db_only,
            "context_applied": processing_question != routed.question,
        }
        if processing_question != routed.question:
            response_metadata["original_question"] = routed.question
            response_metadata["contextual_question"] = processing_question
            if previous_context is not None:
                response_metadata["context_source_event_id"] = previous_context.event_id

        if not routed.mode_explicit and not routed.answer_mode_explicit and not routed.db_only:
            cached = self._outbox.find_recent_answer(processing_question)
            if cached:
                answer = cached.answer
                response_mode = "local_cache"
                response_metadata.update({
                    "cache_hit": True,
                    "cache_source_event_id": cached.event_id,
                })

        effective_mode = routed.mode if routed.mode_explicit else "local"
        if not answer and is_package_query and not routed.db_only:
            scenario_result = await self._scenario_manager.handle_if_supported(
                processing_question,
                requested_mode=effective_mode,
                answer_mode=effective_answer_mode,
            )
            if scenario_result.handled:
                answer = scenario_result.answer
                response_mode = f"registry_scrape_{effective_mode}"
                response_metadata.update({
                    **scenario_result.metadata,
                    "cache_hit": False,
                    "answer_mode": effective_answer_mode,
                    "requested_answer_mode": routed.answer_mode,
                })
                artifacts_count = int(scenario_result.metadata.get("artifacts_count") or 0)
                if effective_answer_mode == "llm" and artifacts_count > 0:
                    try:
                        beautified = await self._api_client.beautify_answer(
                            processing_question,
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
        if not answer:
            logger.info("Dispatching ask request to mode=%s", effective_mode)
            response_payload = await self._api_client.ask(
                processing_question,
                mode=effective_mode,
                answer_mode=effective_answer_mode,
            )
            answer = str(response_payload.get("answer", "")).strip()
            response_mode = str(response_payload.get("mode", "unknown"))
            response_metadata.update({
                "cache_hit": False,
                "answer_mode": response_payload.get("answer_mode", effective_answer_mode),
                "requested_answer_mode": routed.answer_mode,
                "query_kind": "general_it" if is_general_it_query else "package_fallback",
            })
            if (
                effective_answer_mode == "no_llm"
                and is_general_it_query
                and _is_no_reliable_context_answer(answer)
            ):
                cached = self._outbox.find_recent_answer(processing_question)
                if cached:
                    answer = _format_cached_no_llm_answer(cached)
                    response_mode = "local_cache_no_llm"
                    response_metadata["cache_hit"] = True
                    response_metadata["cache_source_event_id"] = cached.event_id
        if not answer:
            raise ApiClientError("Ask API returned empty answer.")
        response_time_ms = int((time.perf_counter() - started_at) * 1000)
        response_metadata["response_time_ms"] = response_time_ms

        if response_metadata.get("unsupported_sources"):
            return AskResult(
                question=routed.question,
                answer=answer,
                requested_mode=routed.mode,
                answer_mode=effective_answer_mode,
                response_mode=response_mode,
                response_time_ms=response_time_ms,
            )

        event = AskExchangeEvent(
            event_id=self._build_event_id(chat_id, user_id, effective_mode, processing_question, answer, correlation_id),
            question=processing_question,
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
                "effective_answer_mode": effective_answer_mode,
                "answer_mode_explicit": routed.answer_mode_explicit,
                "db_only_explicit": routed.db_only_explicit,
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
            answer_mode=effective_answer_mode,
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
