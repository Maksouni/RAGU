from __future__ import annotations

from collections import defaultdict
from typing import Callable

from apps.orchestrator.query_parser import ScenarioQuery
from apps.scraper.models import PackageArtifact

VERSION_EXAMPLES_PER_VERSION = 1


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


def _limit_visible_items(query: ScenarioQuery, items: list[PackageArtifact]) -> list[PackageArtifact]:
    if not query.show:
        return items
    return items[: query.show]


def _visible_versions(query: ScenarioQuery, by_version: dict[str, list[PackageArtifact]]) -> list[str]:
    versions = sorted(by_version.keys(), key=_version_key, reverse=query.sort_by != "oldest")
    if not query.show:
        return versions
    return versions[: query.show]


def _style_label(answer_mode: str) -> str:
    return (
        "LLM режим: подготовленный контекст для генеративного оформления"
        if answer_mode == "llm"
        else "NO-LLM шаблонный ответ"
    )


def _format_filter_line(query: ScenarioQuery) -> str:
    return (
        f"Фильтры: format={query.package_format or '*'}, source={query.source_name or '*'}, "
        f"sort={query.sort_by}, limit={query.limit}, show={query.show}"
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
            + _format_filter_line(query)
        )

    if query.scenario_type == "versions_by_os":
        by_version = defaultdict(list)
        for item in items:
            by_version[item.package_version].append(item)
        versions = sorted(by_version.keys(), key=_version_key, reverse=query.sort_by != "oldest")
        visible_versions = _visible_versions(query, by_version)
        if answer_mode == "llm":
            lines = [
                label,
                f"Краткая сводка: найдено {len(versions)} версий и {len(items)} пакетов для {query.product}.",
                f"Область поиска: {query.os or '*'} {query.os_version or '*'}; LLM получает только эти подготовленные данные.",
                _format_filter_line(query),
                "",
                "Данные для оформления:",
            ]
        else:
            lines = [
                label,
                f"Сценарий: версии {query.product} для {query.os} {query.os_version or '*'}",
                f"Найдено версий: {len(versions)}; пакетов: {len(items)}",
                _format_filter_line(query),
                "",
                "Результаты:",
            ]
        for version in visible_versions:
            examples = by_version[version][:VERSION_EXAMPLES_PER_VERSION]
            lines.append(f"- {version}: пакетов {len(by_version[version])}")
            for ex in examples:
                lines.append(f"  {ex.package_name} -> {ex.artifact_url}")
            hidden_count = len(by_version[version]) - len(examples)
            if hidden_count > 0:
                lines.append(f"  ... еще {hidden_count} пакетов этой версии скрыто.")
            if len(lines) >= max_lines:
                lines.append("... ответ сокращен, данных больше.")
                break
        return "\n".join(lines)

    visible_items = _limit_visible_items(query, items)
    by_format = defaultdict(list)
    for item in items:
        by_format[item.package_format].append(item)
    visible_by_format = defaultdict(list)
    for item in visible_items:
        visible_by_format[item.package_format].append(item)
    formats = sorted(by_format.keys())
    visible_formats = [fmt for fmt in formats if visible_by_format.get(fmt)]
    if answer_mode == "llm":
        lines = [
            label,
            f"Краткая сводка: найдено {len(items)} пакетов {query.product} {query.package_version or '*'}; форматы: {', '.join(formats)}.",
            "LLM не ищет источники сама: ниже только нормализованный результат registry/scraper.",
            _format_filter_line(query),
            "",
            "Данные для оформления:",
        ]
    else:
        lines = [
            label,
            f"Сценарий: пакеты {query.product} {query.package_version or '*'}",
            f"Форматы: {', '.join(formats)}",
            f"Найдено пакетов: {len(items)}",
            _format_filter_line(query),
            "",
            "Результаты:",
        ]
    for fmt in visible_formats:
        lines.append(f"- {fmt}: {len(by_format[fmt])} пакетов")
        for ex in visible_by_format[fmt]:
            lines.append(f"  {ex.package_name} {ex.package_version} -> {ex.artifact_url}")
        if len(lines) >= max_lines:
            lines.append("... ответ сокращен, данных больше.")
            break
    return "\n".join(lines)
