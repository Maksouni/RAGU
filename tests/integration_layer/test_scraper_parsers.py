from apps.scraper.parsers import (
    parse_deb_packages_index,
    parse_deb_html_listing,
    parse_exe_html_listing,
    parse_rpm_html_listing,
)


def test_parse_deb_html_listing() -> None:
    html = """
    <a href="postgresql-17_17.6-1.pgdg13+1_amd64.deb">pkg</a>
    <a href="postgresql-client-17_17.6-1.pgdg13+1_amd64.deb">pkg</a>
    """
    artifacts = parse_deb_html_listing(
        html=html,
        base_url="https://example.org/deb/",
        source_name="deb-source",
        source_url="https://example.org",
        product="postgresql",
        os_name="debian",
        os_version="13",
    )
    assert len(artifacts) == 2
    assert artifacts[0].package_format == "deb"


def test_parse_deb_packages_index_filters_by_package_names() -> None:
    data = b"""
Package: redis-server
Version: 5:7.0.15-1ubuntu0.24.04.1
Architecture: amd64
Filename: pool/universe/r/redis/redis-server_7.0.15-1ubuntu0.24.04.1_amd64.deb

Package: redis-doc
Version: 5:7.0.15-1ubuntu0.24.04.1
Architecture: all
Filename: pool/universe/r/redis/redis-doc_7.0.15-1ubuntu0.24.04.1_all.deb
"""
    artifacts = parse_deb_packages_index(
        data=data,
        base_url="https://archive.ubuntu.com/ubuntu/",
        source_name="ubuntu-noble",
        source_url="https://archive.ubuntu.com/ubuntu/dists/noble/universe/binary-amd64/",
        product="redis",
        os_name="ubuntu",
        os_version="24.04",
        package_names=["redis-server"],
    )
    assert len(artifacts) == 1
    assert artifacts[0].package_name == "redis-server"
    assert artifacts[0].package_version == "5:7.0.15-1ubuntu0.24.04.1"
    assert artifacts[0].artifact_url == "https://archive.ubuntu.com/ubuntu/pool/universe/r/redis/redis-server_7.0.15-1ubuntu0.24.04.1_amd64.deb"


def test_parse_deb_packages_index_allows_globs_and_source_package() -> None:
    data = b"""
Package: cargo-1.91
Source: rustc-1.91
Version: 1.91.1+dfsg~24.04-0ubuntu0.24.04.3
Architecture: amd64
Filename: pool/universe/r/rustc-1.91/cargo-1.91_1.91.1+dfsg~24.04-0ubuntu0.24.04.3_amd64.deb

Package: rustc-1.91
Version: 1.91.1+dfsg~24.04-0ubuntu0.24.04.3
Architecture: amd64
Filename: pool/universe/r/rustc-1.91/rustc-1.91_1.91.1+dfsg~24.04-0ubuntu0.24.04.3_amd64.deb

Package: rust-doc
Source: rustc
Version: 1.75.0+dfsg0ubuntu1-0ubuntu7.4
Architecture: all
Filename: pool/universe/r/rustc/rust-doc_1.75.0+dfsg0ubuntu1-0ubuntu7.4_all.deb
"""
    artifacts = parse_deb_packages_index(
        data=data,
        base_url="https://archive.ubuntu.com/ubuntu/",
        source_name="ubuntu-noble-updates",
        source_url="https://archive.ubuntu.com/ubuntu/dists/noble-updates/universe/binary-amd64/",
        product="rustc",
        os_name="ubuntu",
        os_version="24.04",
        package_names=["rustc-*", "cargo-*"],
    )
    assert [item.package_name for item in artifacts] == ["rustc-1.91", "cargo-1.91"]


def test_parse_rpm_html_listing() -> None:
    html = """
    <a href="postgresql17-17.6-1PGDG.rhel9.x86_64.rpm">rpm</a>
    <a href="postgresql17-server-17.6-1PGDG.rhel9.x86_64.rpm">rpm</a>
    """
    artifacts = parse_rpm_html_listing(
        html=html,
        base_url="https://example.org/rpm/",
        source_name="rpm-source",
        source_url="https://example.org",
        product="postgresql",
        os_name="rhel",
        os_version="9",
    )
    assert len(artifacts) == 2
    assert all(a.package_format == "rpm" for a in artifacts)


def test_parse_exe_html_listing() -> None:
    html = """
    <a href="/downloads/release/python-3123/Python-3.12.3-amd64.exe">exe</a>
    <a href="/downloads/release/python-3123/python-3.12.3.exe">exe</a>
    """
    artifacts = parse_exe_html_listing(
        html=html,
        base_url="https://www.python.org",
        source_name="python-org-windows",
        source_url="https://www.python.org/downloads/windows/",
        product="python",
        os_name="windows",
        os_version="*",
        requested_version="3.12",
    )
    assert len(artifacts) == 2
    assert all(a.package_format == "exe" for a in artifacts)
