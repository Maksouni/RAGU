from apps.orchestrator.query_parser import parse_scenario_query
from apps.orchestrator.result_formatter import format_scenario_answer
from apps.scraper.models import PackageArtifact


def _artifact(idx: int, fmt: str = "deb", version: str = "17.6") -> PackageArtifact:
    return PackageArtifact(
        source_name=f"source-{fmt}",
        package_name=f"pkg-{idx}",
        package_version=version,
        package_format=fmt,
        artifact_url=f"https://example.test/{fmt}/pkg-{idx}",
        source_url="https://example.test",
        os="ubuntu",
    )


def _package_lines(answer: str) -> list[str]:
    return [line for line in answer.splitlines() if " -> " in line and line.startswith("  ")]


def test_show_limits_total_packages_across_formats() -> None:
    query = parse_scenario_query("дай список всех пакетов PostgreSQL 17.6 limit=10 show=4")
    assert query is not None
    artifacts = [
        _artifact(1, "deb"),
        _artifact(2, "deb"),
        _artifact(3, "deb"),
        _artifact(4, "rpm"),
        _artifact(5, "rpm"),
        _artifact(6, "rpm"),
    ]

    answer = format_scenario_answer(query, artifacts)

    assert len(_package_lines(answer)) == 4


def test_show_limits_total_packages_across_versions() -> None:
    query = parse_scenario_query("найди для ubuntu все версии PostgreSQL limit=10 show=3")
    assert query is not None
    artifacts = [
        _artifact(1, "deb", "17.6"),
        _artifact(2, "deb", "17.6"),
        _artifact(3, "deb", "16.10"),
        _artifact(4, "deb", "16.10"),
        _artifact(5, "deb", "15.14"),
    ]

    answer = format_scenario_answer(query, artifacts)

    assert len(_package_lines(answer)) == 3
