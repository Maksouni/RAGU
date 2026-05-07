from apps.orchestrator.query_parser import parse_scenario_query


def test_parse_versions_by_os() -> None:
    q = parse_scenario_query("дай список всех версий PostgreSQL для debian 13")
    assert q is not None
    assert q.scenario_type == "versions_by_os"
    assert q.product == "postgresql"
    assert q.os == "debian"
    assert q.os_version == "13"


def test_parse_formats_by_version() -> None:
    q = parse_scenario_query("дай список всех пакетов PostgreSQL 17.6")
    assert q is not None
    assert q.scenario_type == "formats_by_version"
    assert q.package_version == "17.6"


def test_parse_python_for_ubuntu() -> None:
    q = parse_scenario_query("Python 3.12 для Ubuntu limit=10")
    assert q is not None
    assert q.scenario_type == "formats_by_version"
    assert q.product == "python"
    assert q.package_version == "3.12"
    assert q.os == "ubuntu"
    assert q.limit == 10
    assert q.show == 5


def test_parse_with_filters_and_sort() -> None:
    q = parse_scenario_query(
        "дай список всех пакетов PostgreSQL 17.6 format=exe source=python sort=oldest limit=12 show=9"
    )
    assert q is not None
    assert q.package_format == "exe"
    assert q.source_name == "python"
    assert q.sort_by == "oldest"
    assert q.limit == 12
    assert q.show == 9


def test_parse_android_apk_request_without_version() -> None:
    q = parse_scenario_query("дай мне Geometry Dash Lite для Android format=apk")
    assert q is not None
    assert q.scenario_type == "formats_by_version"
    assert q.product == "geometry dash lite"
    assert q.os == "android"
    assert q.package_format == "apk"
