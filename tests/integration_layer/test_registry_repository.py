from apps.registry.repository import RegistryRepository


def test_registry_loads_templates() -> None:
    repo = RegistryRepository()
    all_templates = repo.list_all()
    assert len(all_templates) >= 5
    assert any(t.package_format == "deb" for t in all_templates)
    assert any(t.package_format == "rpm" for t in all_templates)
    assert any(t.package_format == "apk" for t in all_templates)
    assert any(t.package_format == "exe" for t in all_templates)


def test_registry_filter_debian13() -> None:
    repo = RegistryRepository()
    templates = repo.find_for_os("debian", "13")
    assert len(templates) >= 1
    assert all(t.os == "debian" and t.os_version == "13" for t in templates)
