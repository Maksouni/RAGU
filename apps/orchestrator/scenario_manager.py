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
