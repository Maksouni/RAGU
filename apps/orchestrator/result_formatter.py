from __future__ import annotations

from collections import defaultdict
from typing import Callable

from apps.orchestrator.query_parser import ScenarioQuery
from apps.scraper.models import PackageArtifact


def _deduplicate(artifacts: list[PackageArtifact]) -> list[PackageArtifact]:
    seen = set()
    result = []
    for item in artifacts:
        key = (item.package_name, item.package_version, item.package_format, item.artifact_url)
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result


def _version_key(version: str) -> tuple[int, ...]:
    tokens = []
    for part in version.replace("-", ".").split("."):
        try:
            tokens.append(int(part))
        except ValueError:
            break
    return tuple(tokens)


def _apply_filters(query: ScenarioQuery, artifacts: list[PackageArtifact]) -> list[PackageArtifact]:
    items = _deduplicate(artifacts)
    if query.package_format:
        items = [i for i in items if i.package_format == query.package_format]
    if query.source_name:
        src = query.source_name.lower()
        items = [i for i in items if src in i.source_name.lower()]
    return items


def _sort_items(query: ScenarioQuery, items: list[PackageArtifact]) -> list[PackageArtifact]:
    key_fn: Callable[[PackageArtifact], object]
    reverse = False
    if query.sort_by == "name":
        key_fn = lambda i: (i.package_name.lower(), i.package_version)
    else:
        key_fn = lambda i: _version_key(i.package_version)
        reverse = query.sort_by == "newest"
    return sorted(items, key=key_fn, reverse=reverse)


def _style_label(answer_mode: str) -> str:
    return (
        "LLM режим: структурированный ответ по данным registry/scraper"
        if answer_mode == "llm"
        else "NO-LLM шаблонный ответ"
    )


def format_scenario_answer(
    query: ScenarioQuery,
    artifacts: list[PackageArtifact],
    max_lines: int = 60,
    answer_mode: str = "no_llm",
) -> str:
    items = _sort_items(query, _apply_filters(query, artifacts))
    label = _style_label(answer_mode)
    if query.limit:
        items = items[: query.limit]
    if not items:
        return (
            f"{label}\n"
            "Статус: по заданному сценарию пакеты не найдены в доступных источниках.\n"
            f"Фильтры: format={query.package_format or '*'}, source={query.source_name or '*'}, "
            f"sort={query.sort_by}, limit={query.limit}, show={query.show}"
        )

    if query.scenario_type == "versions_by_os":
        by_version = defaultdict(list)
        for item in items:
            by_version[item.package_version].append(item)
        versions = sorted(by_version.keys(), key=_version_key, reverse=query.sort_by != "oldest")
        lines = [
            label,
            f"Сценарий: версии {query.product} для {query.os} {query.os_version or '*'}",
            f"Найдено версий: {len(versions)}; пакетов: {len(items)}",
            f"Фильтры: format={query.package_format or '*'}, source={query.source_name or '*'}, sort={query.sort_by}, limit={query.limit}, show={query.show}",
            "",
            "Результаты:",
        ]
        for version in versions:
            examples = by_version[version][: query.show]
            lines.append(f"- {version}: пакетов {len(by_version[version])}")
            for ex in examples:
                lines.append(f"  {ex.package_name} -> {ex.artifact_url}")
            if len(lines) >= max_lines:
                lines.append("... ответ сокращен, данных больше.")
                break
        return "\n".join(lines)

    by_format = defaultdict(list)
    for item in items:
        by_format[item.package_format].append(item)
    formats = sorted(by_format.keys())
    lines = [
        label,
        f"Сценарий: пакеты {query.product} {query.package_version or '*'}",
        f"Форматы: {', '.join(formats)}",
        f"Найдено пакетов: {len(items)}",
        f"Фильтры: format={query.package_format or '*'}, source={query.source_name or '*'}, sort={query.sort_by}, limit={query.limit}, show={query.show}",
        "",
        "Результаты:",
    ]
    for fmt in formats:
        lines.append(f"- {fmt}: {len(by_format[fmt])} пакетов")
        for ex in by_format[fmt][: query.show]:
            lines.append(f"  {ex.package_name} {ex.package_version} -> {ex.artifact_url}")
        if len(lines) >= max_lines:
            lines.append("... ответ сокращен, данных больше.")
            break
    return "\n".join(lines)
