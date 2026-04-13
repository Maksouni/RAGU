from __future__ import annotations

from pydantic import BaseModel


class PackageArtifact(BaseModel):
    source_name: str
    package_name: str
    package_version: str
    package_format: str
    artifact_url: str
    source_url: str
    os: str | None = None
    os_version: str | None = None

