from apps.registry.repository import RegistryRepository


def test_registry_loads_templates() -> None:
    repo = RegistryRepository()
    all_templates = repo.list_all()
    assert len(all_templates) >= 4
    assert any(t.package_format == "deb" for t in all_templates)
    assert any(t.package_format == "rpm" for t in all_templates)
    assert any(t.package_format == "exe" for t in all_templates)


def test_registry_filter_debian13() -> None:
    repo = RegistryRepository()
    templates = repo.find_for_os("debian", "13")
    assert len(templates) >= 1
    assert all(t.os == "debian" and t.os_version == "13" for t in templates)


def test_registry_matches_ubuntu24_major_version_to_2404_template() -> None:
    repo = RegistryRepository()
    templates = repo.find_for_os("ubuntu", "24")
    assert any(t.product_hint == "postgresql" and t.os_version == "24.04" for t in templates)


def test_registry_matches_ubuntu_without_version_to_configured_templates() -> None:
    repo = RegistryRepository()
    templates = repo.find_for_os("ubuntu", "")
    assert any(t.product_hint == "postgresql" and t.os_version == "24.04" for t in templates)
