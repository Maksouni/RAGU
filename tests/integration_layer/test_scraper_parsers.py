import tarfile
from io import BytesIO

from apps.scraper.parsers import (
    parse_apk_index,
    parse_deb_html_listing,
    parse_exe_html_listing,
    parse_rpm_html_listing,
)


def _build_apk_index_tar_gz(text: str) -> bytes:
    buffer = BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        content = text.encode("utf-8")
        info = tarfile.TarInfo(name="APKINDEX")
        info.size = len(content)
        tar.addfile(info, BytesIO(content))
    return buffer.getvalue()


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


def test_parse_apk_index() -> None:
    text = "P:postgresql17\nV:17.6-r0\n\nP:postgresql17-client\nV:17.6-r0\n"
    tar_gz = _build_apk_index_tar_gz(text)
    artifacts = parse_apk_index(
        apkindex_tar_gz=tar_gz,
        base_url="https://example.org/apk/",
        source_name="apk-source",
        source_url="https://example.org",
        product="postgresql",
        os_name="alpine",
        os_version="3.20",
    )
    assert len(artifacts) == 2
    assert all(a.package_format == "apk" for a in artifacts)


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
