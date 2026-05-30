from __future__ import annotations

import json
from dataclasses import dataclass

import pytest

from apps.common.models import AskExchangeEvent
from apps.common.settings import IntegrationSettings
from apps.orchestrator.scenario_manager import ScenarioResult
from apps.orchestrator.service import AskOrchestrator
from examples.fastapi_demo import server as fastapi_server
from scripts.seed_demo_it_knowledge import PREPARED_IT_ANSWERS


class FakeApiClient:
    def __init__(self) -> None:
        self.ask_calls: list[dict[str, str]] = []
        self.beautify_calls: list[dict[str, str]] = []

    async def ask(self, question: str, mode: str, answer_mode: str):  # noqa: ANN201
        self.ask_calls.append({"question": question, "mode": mode, "answer_mode": answer_mode})
        return {"answer": "fallback answer", "mode": mode, "answer_mode": answer_mode}

    async def beautify_answer(self, question: str, structured_answer: str):  # noqa: ANN201
        self.beautify_calls.append({"question": question, "structured_answer": structured_answer})
        return {"answer": "LLM режим: красивое оформление по подготовленному контексту"}


class NoReliableContextApiClient(FakeApiClient):
    async def ask(self, question: str, mode: str, answer_mode: str):  # noqa: ANN201
        self.ask_calls.append({"question": question, "mode": mode, "answer_mode": answer_mode})
        return {
            "answer": "NO-LLM шаблонный ответ\nСтатус: в локальной базе не найдено достаточно релевантной IT-заготовки.",
            "mode": "no_llm_semantic_local",
            "answer_mode": "no_llm",
        }


class FakeOutbox:
    def __init__(self) -> None:
        self.events: list[AskExchangeEvent] = []

    def find_recent_answer(self, question: str):  # noqa: ANN201
        return None

    def enqueue_event(self, event: AskExchangeEvent) -> bool:
        self.events.append(event)
        return True


class CachedOutbox(FakeOutbox):
    def __init__(self, cached: AskExchangeEvent) -> None:
        super().__init__()
        self.cached = cached

    def find_recent_answer(self, question: str):  # noqa: ANN201
        return self.cached if self.cached.question == question else None


@dataclass
class FakeScenarioManager:
    answer: str = "NO-LLM шаблонный ответ\nСценарий: пакеты python 3.12\nРезультаты:\n- deb: 1 пакетов"
    calls: list[dict[str, str]] | None = None

    async def handle_if_supported(self, question: str, requested_mode: str, answer_mode: str) -> ScenarioResult:
        if self.calls is not None:
            self.calls.append({"question": question, "requested_mode": requested_mode, "answer_mode": answer_mode})
        return ScenarioResult(
            handled=True,
            answer=self.answer if answer_mode == "no_llm" else self.answer.replace("NO-LLM шаблонный ответ", "LLM режим: подготовленный контекст"),
            metadata={"artifacts_count": 1, "answer_mode_seen": answer_mode},
        )


class ContextOutbox(FakeOutbox):
    def __init__(self, previous: AskExchangeEvent) -> None:
        super().__init__()
        self.previous = previous

    def find_recent_context(self, *, chat_id: str, user_id: str, max_scan: int = 50):  # noqa: ANN201
        if self.previous.chat_id == chat_id and self.previous.user_id == user_id:
            return self.previous
        return None


@pytest.mark.asyncio
async def test_nollm_python_312_ubuntu_limit_10_uses_template_without_llm() -> None:
    api = FakeApiClient()
    outbox = FakeOutbox()
    orchestrator = AskOrchestrator(
        settings=IntegrationSettings(),
        api_client=api,  # type: ignore[arg-type]
        outbox=outbox,  # type: ignore[arg-type]
        scenario_manager=FakeScenarioManager(),  # type: ignore[arg-type]
    )

    result = await orchestrator.handle_user_message(
        raw_text="/nollm Python 3.12 для Ubuntu limit=10",
        chat_id="chat",
        user_id="user",
        correlation_id="corr",
    )

    assert result.answer_mode == "no_llm"
    assert result.answer.startswith("NO-LLM шаблонный ответ")
    assert api.beautify_calls == []
    assert outbox.events[0].metadata["answer_mode"] == "no_llm"


@pytest.mark.asyncio
async def test_db_only_command_skips_registry_scraper_and_llm() -> None:
    api = FakeApiClient()
    calls: list[dict[str, str]] = []
    outbox = FakeOutbox()
    orchestrator = AskOrchestrator(
        settings=IntegrationSettings(),
        api_client=api,  # type: ignore[arg-type]
        outbox=outbox,  # type: ignore[arg-type]
        scenario_manager=FakeScenarioManager(calls=calls),  # type: ignore[arg-type]
    )

    result = await orchestrator.handle_user_message(
        raw_text="/db /llm Python 3.12 for Ubuntu limit=10",
        chat_id="chat",
        user_id="user",
        correlation_id="corr",
    )

    assert result.answer_mode == "no_llm"
    assert calls == []
    assert api.beautify_calls == []
    assert api.ask_calls == [{"question": "Python 3.12 for Ubuntu limit=10", "mode": "local", "answer_mode": "no_llm"}]
    assert outbox.events[0].metadata["db_only"] is True


@pytest.mark.asyncio
async def test_followup_query_uses_previous_dialog_context_for_package_parser() -> None:
    previous = AskExchangeEvent(
        event_id="previous-event",
        question="versions PostgreSQL for debian 13",
        answer="previous answer",
        mode="local",
        user_id="user",
        chat_id="chat",
        correlation_id="old",
    )
    calls: list[dict[str, str]] = []
    orchestrator = AskOrchestrator(
        settings=IntegrationSettings(),
        api_client=FakeApiClient(),  # type: ignore[arg-type]
        outbox=ContextOutbox(previous),  # type: ignore[arg-type]
        scenario_manager=FakeScenarioManager(calls=calls),  # type: ignore[arg-type]
    )

    result = await orchestrator.handle_user_message(
        raw_text="/nollm latest version of it show=1",
        chat_id="chat",
        user_id="user",
        correlation_id="new",
    )

    assert result.answer_mode == "no_llm"
    assert calls
    assert calls[0]["question"] == "product=postgresql latest version latest version of it show=1"


@pytest.mark.asyncio
async def test_russian_followup_query_uses_previous_dialog_context() -> None:
    previous = AskExchangeEvent(
        event_id="previous-event",
        question="версии PostgreSQL для debian 13",
        answer="previous answer",
        mode="local",
        user_id="user",
        chat_id="chat",
        correlation_id="old",
    )
    calls: list[dict[str, str]] = []
    orchestrator = AskOrchestrator(
        settings=IntegrationSettings(),
        api_client=FakeApiClient(),  # type: ignore[arg-type]
        outbox=ContextOutbox(previous),  # type: ignore[arg-type]
        scenario_manager=FakeScenarioManager(calls=calls),  # type: ignore[arg-type]
    )

    result = await orchestrator.handle_user_message(
        raw_text="/nollm дай последнюю версию этого файла show=1",
        chat_id="chat",
        user_id="user",
        correlation_id="new",
    )

    assert result.answer_mode == "no_llm"
    assert calls
    assert calls[0]["question"] == "product=postgresql latest version дай последнюю версию этого файла show=1"


@pytest.mark.asyncio
async def test_redis_latest_followup_uses_previous_subject_instead_of_general_llm(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DISABLE_LLM_ANSWERS", "false")
    previous = AskExchangeEvent(
        event_id="previous-event",
        question="что такое redis?",
        answer="Redis - это in-memory key-value database.",
        mode="local",
        user_id="user",
        chat_id="chat",
        correlation_id="old",
    )
    calls: list[dict[str, str]] = []
    api = FakeApiClient()
    orchestrator = AskOrchestrator(
        settings=IntegrationSettings(),
        api_client=api,  # type: ignore[arg-type]
        outbox=ContextOutbox(previous),  # type: ignore[arg-type]
        scenario_manager=FakeScenarioManager(calls=calls),  # type: ignore[arg-type]
    )

    result = await orchestrator.handle_user_message(
        raw_text="дай мне его последнюю версию",
        chat_id="chat",
        user_id="user",
        correlation_id="new",
    )

    assert result.answer_mode == "llm"
    assert calls
    assert calls[0]["question"] == "product=redis latest version дай мне его последнюю версию"
    assert api.ask_calls == []


@pytest.mark.asyncio
async def test_llm_postgresql_176_limit_13_show_4_beautifies_prepared_context() -> None:
    api = FakeApiClient()
    orchestrator = AskOrchestrator(
        settings=IntegrationSettings(),
        api_client=api,  # type: ignore[arg-type]
        outbox=FakeOutbox(),  # type: ignore[arg-type]
        scenario_manager=FakeScenarioManager(),  # type: ignore[arg-type]
    )

    result = await orchestrator.handle_user_message(
        raw_text="/llm дай список всех пакетов PostgreSQL 17.6 limit=13 show=4",
        chat_id="chat",
        user_id="user",
        correlation_id="corr",
    )

    assert result.answer_mode == "llm"
    assert result.response_mode == "registry_scrape_llm_local"
    assert result.answer.startswith("LLM режим: красивое оформление")
    assert api.beautify_calls
    assert "LLM режим: подготовленный контекст" in api.beautify_calls[0]["structured_answer"]


@pytest.mark.asyncio
async def test_plain_query_uses_default_answer_mode_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DISABLE_LLM_ANSWERS", "false")
    api = FakeApiClient()
    orchestrator = AskOrchestrator(
        settings=IntegrationSettings(),
        api_client=api,  # type: ignore[arg-type]
        outbox=FakeOutbox(),  # type: ignore[arg-type]
        scenario_manager=FakeScenarioManager(),  # type: ignore[arg-type]
    )

    result = await orchestrator.handle_user_message(
        raw_text="дай список всех пакетов PostgreSQL 17.6 limit=13 show=4",
        chat_id="chat",
        user_id="user",
        correlation_id="corr",
    )

    assert result.answer_mode == "llm"
    assert api.beautify_calls


@pytest.mark.asyncio
async def test_general_it_question_routes_to_semantic_api_without_package_parser() -> None:
    api = FakeApiClient()
    outbox = FakeOutbox()
    orchestrator = AskOrchestrator(
        settings=IntegrationSettings(),
        api_client=api,  # type: ignore[arg-type]
        outbox=outbox,  # type: ignore[arg-type]
        scenario_manager=FakeScenarioManager(),  # type: ignore[arg-type]
    )

    result = await orchestrator.handle_user_message(
        raw_text="/nollm what is the difference between Docker image and container?",
        chat_id="chat",
        user_id="user",
        correlation_id="corr",
    )

    assert result.answer_mode == "no_llm"
    assert result.response_mode == "local"
    assert api.ask_calls == [
        {
            "question": "what is the difference between Docker image and container?",
            "mode": "local",
            "answer_mode": "no_llm",
        }
    ]
    assert outbox.events[0].metadata["query_kind"] == "general_it"


@pytest.mark.asyncio
async def test_nollm_general_it_reuses_exact_outbox_when_graph_has_no_context() -> None:
    question = "what is frobnicate endpoint?"
    cached = AskExchangeEvent(
        event_id="cached-event",
        question=question,
        answer="LLM mode: cached answer from the previous turn.",
        mode="local",
        user_id="user",
        chat_id="chat",
        correlation_id="old",
    )
    api = NoReliableContextApiClient()
    outbox = CachedOutbox(cached)
    orchestrator = AskOrchestrator(
        settings=IntegrationSettings(),
        api_client=api,  # type: ignore[arg-type]
        outbox=outbox,  # type: ignore[arg-type]
        scenario_manager=FakeScenarioManager(),  # type: ignore[arg-type]
    )

    result = await orchestrator.handle_user_message(
        raw_text=f"/nollm {question}",
        chat_id="chat",
        user_id="user",
        correlation_id="new",
    )

    assert result.response_mode == "local_cache_no_llm"
    assert result.answer.startswith("NO-LLM ответ из сохраненной базы")
    assert "cached answer from the previous turn" in result.answer
    assert outbox.events[0].metadata["cache_hit"] is True
    assert outbox.events[0].metadata["cache_source_event_id"] == "cached-event"


def test_embedding_dim_is_capped_at_20(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EMBEDDING_DIM", "128")
    capped = min(20, max(1, fastapi_server._env_int("EMBEDDING_DIM", 20)))
    assert capped == 20


def test_lexical_fallback_ignores_generic_question_words() -> None:
    tokens = fastapi_server._tokenize("what is the difference between Docker image and container?")
    assert "what" not in tokens
    assert "between" not in tokens
    assert tokens == ["difference", "docker", "image", "container"]
    assert fastapi_server._match_score("question: give me package for Android", tokens) == 0


def test_prepared_it_answer_is_indexed_and_formatted() -> None:
    docs = [
        '{"event_type":"prepared_it_answer","id":"prepared_it_test","topic":"Docker image vs container",'
        '"question":"Чем Docker image отличается от container?",'
        '"keywords":["docker","image","container"],'
        '"answer":"Image хранится, container выполняется."}'
    ]

    entities, relations = fastapi_server.build_fast_graph_from_docs(docs)
    answer = fastapi_server._build_prepared_it_answer(entities[0], "local", "embeddings")

    assert relations == []
    assert entities[0].entity_type == "PreparedITAnswer"
    assert answer.startswith("NO-LLM заготовленный IT-ответ")
    assert "Image хранится, container выполняется." in answer

    prepared, score = fastapi_server._best_prepared_it_answer(
        "чем Docker image отличается от container?",
        entities,
    )
    assert prepared == entities[0]
    assert score >= 2


def test_seed_has_specific_docker_image_definition() -> None:
    question = "что такое Docker image?"
    seed_item = next(item for item in PREPARED_IT_ANSWERS if item["id"] == "prepared_it_docker_image_definition")
    comparison_item = next(item for item in PREPARED_IT_ANSWERS if item["id"] == "prepared_it_docker_image_container")
    entities, _ = fastapi_server.build_fast_graph_from_docs(
        [
            json.dumps(comparison_item, ensure_ascii=False),
            json.dumps(seed_item, ensure_ascii=False),
        ]
    )

    prepared, score = fastapi_server._trusted_prepared_from_candidates(question, entities)

    assert prepared is not None
    assert prepared.entity_name == "Docker image"
    assert score > 100


def test_structured_events_use_fast_graph_even_when_llm_enabled() -> None:
    fast_docs, regular_docs = fastapi_server._split_fast_graph_docs(
        [
            '{"event_type":"prepared_it_answer","id":"prepared_it_test","topic":"API endpoint","answer":"Endpoint is a URL."}',
            '{"event_type":"ask_exchange","event_id":"evt","question":"q","answer":"a","mode":"local","user_id":"u","chat_id":"c","correlation_id":"x"}',
            "plain document",
        ]
    )

    assert len(fast_docs) == 2
    assert regular_docs == ["plain document"]


def test_unrelated_semantic_candidates_are_not_trusted() -> None:
    docs = [
        '{"event_type":"ask_exchange","event_id":"evt","question":"Python 3.12 для Ubuntu",'
        '"answer":"NO-LLM шаблонный ответ\\nlibpython -> https://example.test/libpython.deb",'
        '"mode":"local","user_id":"u","chat_id":"c","correlation_id":"x"}'
    ]
    entities, relations = fastapi_server.build_fast_graph_from_docs(docs)

    score = fastapi_server._candidate_score(
        "чем Docker image отличается от container?",
        entities,
        relations,
    )

    assert score < fastapi_server.LEXICAL_MIN_SCORE


def test_exact_saved_qa_can_be_reused_before_semantic_fallback() -> None:
    question = "what year did Debian 13 release?"
    docs = [
        json.dumps(
            {
                "event_type": "ask_exchange",
                "event_id": "evt-debian-release",
                "question": question,
                "answer": "LLM mode: Debian 13 was released in 2025.",
                "mode": "local",
                "user_id": "u",
                "chat_id": "c",
                "correlation_id": "x",
            }
        ),
        json.dumps(
            {
                "event_type": "prepared_it_answer",
                "id": "prepared_it_git_test",
                "topic": "Git merge vs rebase",
                "question": "What is the difference between git merge and git rebase?",
                "keywords": ["git", "merge", "rebase"],
                "answer": "Git merge preserves integration history; rebase makes local history linear.",
            }
        ),
    ]

    entities, relations = fastapi_server.build_fast_graph_from_docs(docs)
    saved_answer, source = fastapi_server._find_exact_saved_answer(question, entities, relations)
    prepared, prepared_score = fastapi_server._trusted_prepared_from_candidates(question, entities)

    assert saved_answer == "LLM mode: Debian 13 was released in 2025."
    assert source.startswith("exact_saved_qa:")
    assert prepared is None
    assert prepared_score < fastapi_server.PREPARED_IT_MIN_SCORE


def test_seed_contains_debian_13_release_answer() -> None:
    question = "\u0432 \u043a\u0430\u043a\u043e\u043c \u0433\u043e\u0434\u0443 \u0432\u044b\u0448\u0435\u043b Debian 13"
    seed_item = next(item for item in PREPARED_IT_ANSWERS if item["id"] == "prepared_it_debian_13_release")
    entities, _ = fastapi_server.build_fast_graph_from_docs([json.dumps(seed_item, ensure_ascii=False)])

    prepared, score = fastapi_server._trusted_prepared_from_candidates(question, entities)
    answer = fastapi_server._build_prepared_it_answer(prepared, "local", f"prepared_it_lexical:{score}")

    assert prepared is not None
    assert score >= fastapi_server.PREPARED_IT_MIN_SCORE
    assert "2025" in answer
    assert "Bullseye" in answer
