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


def test_parse_unsupported_source_request_without_version() -> None:
    q = parse_scenario_query("скачай Foo Package для Solaris format=deb")
    assert q is not None
    assert q.scenario_type == "formats_by_version"
    assert q.product == "foo package"
    assert q.os == "solaris"
    assert q.package_format == "deb"


def test_parse_free_word_order_with_filters_in_middle() -> None:
    q = parse_scenario_query("покажи для Ubuntu Python limit=10 3.12 show=4 format=deb")
    assert q is not None
    assert q.scenario_type == "formats_by_version"
    assert q.product == "python"
    assert q.package_version == "3.12"
    assert q.os == "ubuntu"
    assert q.package_format == "deb"
    assert q.limit == 10
    assert q.show == 4


def test_parse_versions_when_os_comes_before_product() -> None:
    q = parse_scenario_query("найди для debian 13 все версии PostgreSQL limit=10")
    assert q is not None
    assert q.scenario_type == "versions_by_os"
    assert q.product == "postgresql"
    assert q.os == "debian"
    assert q.os_version == "13"


def test_parse_natural_language_filters() -> None:
    q = parse_scenario_query("дай последнюю версию PostgreSQL для debian 13 с ресурса pgdg в количестве 10 штук покажи 5")
    assert q is not None
    assert q.scenario_type == "versions_by_os"
    assert q.product == "postgresql"
    assert q.os == "debian"
    assert q.os_version == "13"
    assert q.source_name == "pgdg"
    assert q.sort_by == "newest"
    assert q.limit == 10
    assert q.show == 5


def test_parse_postgresql_for_ubuntu_without_format() -> None:
    q = parse_scenario_query("postgresql для ubuntu")
    assert q is not None
    assert q.scenario_type == "versions_by_os"
    assert q.product == "postgresql"
    assert q.os == "ubuntu"
    assert q.package_format is None


def test_parse_ubuntu24_postgresql_versions_with_natural_show() -> None:
    q = parse_scenario_query("покажи последние версии postgresql для ubuntu 24 выведи 5")
    assert q is not None
    assert q.scenario_type == "versions_by_os"
    assert q.product == "postgresql"
    assert q.os == "ubuntu"
    assert q.os_version == "24"
    assert q.sort_by == "newest"
    assert q.show == 5


def test_parse_expanded_products_for_ubuntu24() -> None:
    cases = [
        ("redis для ubuntu 24", "redis"),
        ("Go для ubuntu 24", "golang"),
        ("java для ubuntu 24", "openjdk"),
        ("sqlite для ubuntu 24", "sqlite3"),
        ("nodejs для ubuntu 24", "nodejs"),
        ("rust для ubuntu 24", "rustc"),
    ]
    for text, product in cases:
        q = parse_scenario_query(text)
        assert q is not None
        assert q.scenario_type == "versions_by_os"
        assert q.product == product
        assert q.os == "ubuntu"
        assert q.os_version == "24"


def test_parse_install_file_wording_does_not_treat_installation_as_os() -> None:
    q = parse_scenario_query("дай файл для установки go для ubuntu 24 show=3")
    assert q is not None
    assert q.scenario_type == "versions_by_os"
    assert q.product == "golang"
    assert q.os == "ubuntu"
    assert q.os_version == "24"
    assert q.show == 3


def test_parse_english_install_file_for_go_on_ubuntu() -> None:
    q = parse_scenario_query("give install file for go on ubuntu 24 show=3")
    assert q is not None
    assert q.scenario_type == "versions_by_os"
    assert q.product == "golang"
    assert q.os == "ubuntu"
    assert q.os_version == "24"
    assert q.show == 3


def test_parse_oldest_and_name_sort_without_params() -> None:
    q_old = parse_scenario_query("покажи старые версии postgresql для ubuntu 24")
    assert q_old is not None
    assert q_old.sort_by == "oldest"

    q_name = parse_scenario_query("покажи версии postgresql для ubuntu 24 по имени")
    assert q_name is not None
    assert q_name.sort_by == "name"


def test_parse_context_rewritten_latest_product_version() -> None:
    q = parse_scenario_query("product=redis latest version дай мне его последнюю версию show=1")
    assert q is not None
    assert q.scenario_type == "formats_by_version"
    assert q.product == "redis"
    assert q.package_version is None
    assert q.sort_by == "newest"
    assert q.show == 1
