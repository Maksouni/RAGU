from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

AskMode = Literal["local", "global"]

_COMMAND_RE = re.compile(r"^/(local|global)\b", re.IGNORECASE)
_FLAG_RE = re.compile(r"^(local|global)\s*[:\-]\s*", re.IGNORECASE)
_WORD_PREFIX_RE = re.compile(r"^(local|global)\s+", re.IGNORECASE)


@dataclass(frozen=True)
class RoutedQuestion:
    mode: AskMode
    question: str
    mode_explicit: bool = False


def route_mode_and_question(text: str, default_mode: AskMode = "local") -> RoutedQuestion:
    raw = (text or "").strip()
    if not raw:
        return RoutedQuestion(mode=default_mode, question="", mode_explicit=False)

    command_match = _COMMAND_RE.match(raw)
    if command_match:
        mode = command_match.group(1).lower()
        question = raw[command_match.end() :].strip()
        return RoutedQuestion(mode=mode, question=question, mode_explicit=True)

    flag_match = _FLAG_RE.match(raw)
    if flag_match:
        mode = flag_match.group(1).lower()
        question = raw[flag_match.end() :].strip()
        return RoutedQuestion(mode=mode, question=question, mode_explicit=True)

    word_prefix_match = _WORD_PREFIX_RE.match(raw)
    if word_prefix_match:
        mode = word_prefix_match.group(1).lower()
        question = raw[word_prefix_match.end() :].strip()
        return RoutedQuestion(mode=mode, question=question, mode_explicit=True)

    return RoutedQuestion(mode=default_mode, question=raw, mode_explicit=False)
