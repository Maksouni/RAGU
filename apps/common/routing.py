from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

AskMode = Literal["local", "global"]
AnswerMode = Literal["auto", "llm", "no_llm"]

_COMMAND_RE = re.compile(r"^/(local|global)\b", re.IGNORECASE)
_FLAG_RE = re.compile(r"^(local|global)\s*[:\-]\s*", re.IGNORECASE)
_ANSWER_COMMAND_RE = re.compile(r"^/(llm|nollm|no_llm|no-llm)\b", re.IGNORECASE)
_ANSWER_FLAG_RE = re.compile(r"^(llm|nollm|no_llm|no-llm)\s*[:\-]\s*", re.IGNORECASE)
_ANSWER_WORD_PREFIX_RE = re.compile(r"^(llm|nollm|no_llm|no-llm)\s+", re.IGNORECASE)


@dataclass(frozen=True)
class RoutedQuestion:
    mode: AskMode
    question: str
    mode_explicit: bool = False
    answer_mode: AnswerMode = "auto"
    answer_mode_explicit: bool = False


def _normalize_answer_mode(value: str) -> AnswerMode:
    normalized = value.strip().lower().replace("-", "_")
    return "no_llm" if normalized in {"nollm", "no_llm"} else "llm"


def route_mode_and_question(
    text: str,
    default_mode: AskMode = "local",
    default_answer_mode: AnswerMode = "auto",
) -> RoutedQuestion:
    raw = (text or "").strip()
    if not raw:
        return RoutedQuestion(
            mode=default_mode,
            question="",
            mode_explicit=False,
            answer_mode=default_answer_mode,
            answer_mode_explicit=False,
        )

    mode: AskMode = default_mode
    answer_mode: AnswerMode = default_answer_mode
    mode_explicit = False
    answer_mode_explicit = False

    # Allow compact combinations such as:
    # /llm /global question, /global /nollm question, nollm global: question.
    while True:
        command_match = _COMMAND_RE.match(raw)
        if command_match:
            mode = command_match.group(1).lower()  # type: ignore[assignment]
            mode_explicit = True
            raw = raw[command_match.end() :].strip()
            continue

        answer_command_match = _ANSWER_COMMAND_RE.match(raw)
        if answer_command_match:
            answer_mode = _normalize_answer_mode(answer_command_match.group(1))
            answer_mode_explicit = True
            raw = raw[answer_command_match.end() :].strip()
            continue

        flag_match = _FLAG_RE.match(raw)
        if flag_match:
            mode = flag_match.group(1).lower()  # type: ignore[assignment]
            mode_explicit = True
            raw = raw[flag_match.end() :].strip()
            continue

        answer_flag_match = _ANSWER_FLAG_RE.match(raw)
        if answer_flag_match:
            answer_mode = _normalize_answer_mode(answer_flag_match.group(1))
            answer_mode_explicit = True
            raw = raw[answer_flag_match.end() :].strip()
            continue

        answer_word_prefix_match = _ANSWER_WORD_PREFIX_RE.match(raw)
        if answer_word_prefix_match:
            answer_mode = _normalize_answer_mode(answer_word_prefix_match.group(1))
            answer_mode_explicit = True
            raw = raw[answer_word_prefix_match.end() :].strip()
            continue

        break

    return RoutedQuestion(
        mode=mode,
        question=raw,
        mode_explicit=mode_explicit,
        answer_mode=answer_mode,
        answer_mode_explicit=answer_mode_explicit,
    )
