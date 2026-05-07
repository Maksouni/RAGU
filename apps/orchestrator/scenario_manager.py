from __future__ import annotations

import logging

from pydantic import BaseModel, Field

from apps.orchestrator.query_parser import parse_scenario_query
from apps.orchestrator.result_formatter import format_scenario_answer
from apps.registry.repository import RegistryRepository
from apps.scraper.models import PackageArtifact
from apps.scraper.service import PackageScraperService

logger = logging.getLogger(__name__)


class ScenarioResult(BaseModel):
    handled: bool
    answer: str = ""
    metadata: dict[str, object] = Field(default_factory=dict)


def _format_unsupported_sources_answer(question: str, product: str, os_name: str | None, package_format: str | None) -> str:
    return (
        "Источник данных не настроен для этого запроса.\n\n"
        f"Запрос: {question}\n"
        f"Распознано: product={product}, os={os_name or '*'}, format={package_format or '*'}.\n\n"
        "Сейчас в демо настроены источники для PostgreSQL/Python и форматов deb/rpm/apk/exe "
        "в серверных репозиториях Debian, Ubuntu, RHEL, Alpine и python.org.\n"
        "Android APK-источники вроде Google Play или сторонних APK-каталогов не подключены, "
        "поэтому я не буду подбирать похожие старые ответы из графа."
    )


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
