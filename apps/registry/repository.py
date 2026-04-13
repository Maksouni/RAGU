from __future__ import annotations

import json
from pathlib import Path

from apps.registry.models import RepositoryTemplate


class RegistryRepository:
    def __init__(self, templates_dir: Path | None = None) -> None:
        self._templates_dir = templates_dir or (Path(__file__).resolve().parent / "templates")
        self._templates = self._load_templates()

    def _load_templates(self) -> list[RepositoryTemplate]:
        if not self._templates_dir.exists():
            return []
        templates: list[RepositoryTemplate] = []
        for path in sorted(self._templates_dir.glob("*.json")):
            data = json.loads(path.read_text(encoding="utf-8"))
            templates.append(RepositoryTemplate.model_validate(data))
        return [t for t in templates if t.enabled]

    def list_all(self) -> list[RepositoryTemplate]:
        return list(self._templates)

    def find_for_os(self, os_name: str, os_version: str) -> list[RepositoryTemplate]:
        os_norm = os_name.strip().lower()
        version_norm = os_version.strip().lower()
        return [
            t
            for t in self._templates
            if (t.os or "").lower() == os_norm
            and ((t.os_version or "").lower() in {version_norm, "*", ""})
        ]

    def filter_templates(
        self,
        *,
        os_name: str | None = None,
        os_version: str | None = None,
        package_format: str | None = None,
    ) -> list[RepositoryTemplate]:
        result = self.list_all()
        if os_name:
            os_norm = os_name.lower()
            result = [t for t in result if (t.os or "").lower() == os_norm]
        if os_version:
            version_norm = os_version.lower()
            result = [t for t in result if (t.os_version or "").lower() in {version_norm, "*", ""}]
        if package_format:
            fmt = package_format.lower()
            result = [t for t in result if t.package_format == fmt]
        return result
