from __future__ import annotations

import logging

from pydantic import BaseModel, Field

from apps.orchestrator.query_parser import parse_scenario_query
from apps.orchestrator.result_formatter import format_scenario_answer
from apps.registry.repository import RegistryRepository
from apps.scraper.models import PackageArtifact
from apps.scraper.service import PackageScraperService

logger = logging.getLogger(__name__)


def _normalize_product(value: str) -> str:
    product = value.strip().lower()
    if product == "postgres":
        return "postgresql"
    return product


def _template_matches_product(template_product: str, requested_product: str) -> bool:
    hint = _normalize_product(template_product)
    requested = _normalize_product(requested_product)
    return requested == hint or requested in hint or hint in requested


def _filter_templates_for_query(templates: list, product: str, source_name: str | None) -> list:
    filtered = [
        template
        for template in templates
        if _template_matches_product(template.product_hint, product)
    ]
    if source_name:
        source = source_name.strip().lower()
        filtered = [
            template
            for template in filtered
            if source in template.source_name.lower() or source in template.template_id.lower()
        ]
    return filtered


class ScenarioResult(BaseModel):
    handled: bool
    answer: str = ""
    metadata: dict[str, object] = Field(default_factory=dict)


def _format_unsupported_sources_answer(question: str, product: str, os_name: str | None, package_format: str | None) -> str:
    return (
        "Источник данных не настроен для этого запроса.\n\n"
        f"Запрос: {question}\n"
        f"Распознано: product={product}, os={os_name or '*'}, format={package_format or '*'}.\n\n"
        "Сейчас в демо настроены источники для PostgreSQL/Python и серверных репозиториев "
        "Debian, Ubuntu, RHEL, Alpine и python.org.\n"
        "Для этого продукта, ОС или формата нет подключенного registry-шаблона, поэтому я не буду "
        "подбирать похожие старые ответы из графа."
    )


def _format_source_unavailable_answer(question: str, errors: dict[str, str]) -> str:
    lines = [
        "Источник данных временно недоступен или вернул ошибку.",
        "",
        f"Запрос: {question}",
        "Повторите запрос позже или выберите другой source/os/format.",
        "",
        "Ошибки источников:",
    ]
    for template_id, error in errors.items():
        lines.append(f"- {template_id}: {error}")
    return "\n".join(lines)


class ScenarioManager:
    def __init__(self, registry: RegistryRepository, scraper: PackageScraperService) -> None:
        self._registry = registry
        self._scraper = scraper

    async def handle_if_supported(
        self,
        question: str,
        requested_mode: str,
        answer_mode: str = "no_llm",
    ) -> ScenarioResult:
        scenario_query = parse_scenario_query(question)
        if not scenario_query:
            return ScenarioResult(handled=False)

        if scenario_query.scenario_type == "versions_by_os" and scenario_query.os:
            templates = self._registry.find_for_os(
                os_name=scenario_query.os,
                os_version=scenario_query.os_version or "",
            )
        elif requested_mode == "local":
            templates = self._registry.filter_templates(
                os_name=scenario_query.os,
                os_version=scenario_query.os_version,
                package_format=scenario_query.package_format,
            )
        else:
            templates = self._registry.list_all()
            if scenario_query.os:
                templates = [t for t in templates if (t.os or "").lower() == scenario_query.os.lower()]
            if scenario_query.os_version:
                osv = scenario_query.os_version.lower()
                templates = [t for t in templates if (t.os_version or "").lower() in {osv, "*", ""}]
            if scenario_query.package_format:
                templates = [t for t in templates if t.package_format == scenario_query.package_format]

        templates = _filter_templates_for_query(
            templates,
            product=scenario_query.product,
            source_name=scenario_query.source_name,
        )

        if not templates:
            return ScenarioResult(
                handled=True,
                answer=_format_unsupported_sources_answer(
                    scenario_query.raw_query,
                    scenario_query.product,
                    scenario_query.os,
                    scenario_query.package_format,
                ),
                metadata={
                    "scenario_type": scenario_query.scenario_type,
                    "templates_used": [],
                    "artifacts_count": 0,
                    "product": scenario_query.product,
                    "os": scenario_query.os,
                    "os_version": scenario_query.os_version,
                    "package_version": scenario_query.package_version,
                    "requested_mode": requested_mode,
                    "unsupported_sources": True,
                },
            )

        all_artifacts: list[PackageArtifact] = []
        for template in templates:
            artifacts = await self._scraper.fetch_from_template(
                template,
                product=scenario_query.product,
                requested_version=scenario_query.package_version,
            )
            all_artifacts.extend(artifacts)

        source_errors = {}
        if hasattr(self._scraper, "last_errors_for"):
            source_errors = self._scraper.last_errors_for([t.template_id for t in templates])
        if not all_artifacts and source_errors:
            return ScenarioResult(
                handled=True,
                answer=_format_source_unavailable_answer(scenario_query.raw_query, source_errors),
                metadata={
                    "scenario_type": scenario_query.scenario_type,
                    "templates_used": [t.template_id for t in templates],
                    "artifacts_count": 0,
                    "product": scenario_query.product,
                    "os": scenario_query.os,
                    "os_version": scenario_query.os_version,
                    "package_version": scenario_query.package_version,
                    "requested_mode": requested_mode,
                    "source_errors": source_errors,
                },
            )

        answer = format_scenario_answer(scenario_query, all_artifacts, answer_mode=answer_mode)
        return ScenarioResult(
            handled=True,
            answer=answer,
            metadata={
                "scenario_type": scenario_query.scenario_type,
                "templates_used": [t.template_id for t in templates],
                "artifacts_count": len(all_artifacts),
                "product": scenario_query.product,
                "os": scenario_query.os,
                "os_version": scenario_query.os_version,
                "package_version": scenario_query.package_version,
                "requested_mode": requested_mode,
            },
        )
