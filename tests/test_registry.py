import pytest

from toolkit.core.registry import Registry, register_builtin_plugins


def test_registry_register_and_create():
    r = Registry()
    r.register("x", lambda a=0: {"a": a})
    assert r.create("x", a=2) == {"a": 2}


def test_registry_unknown_plugin_raises_keyerror():
    r = Registry()
    with pytest.raises(KeyError):
        r.create("missing")


def test_register_builtin_plugins_registers_present_plugins():
    r = Registry()

    register_builtin_plugins(registry_obj=r)

    plugins = r.list_plugins()
    assert "ckan" in plugins
    assert "sdmx" in plugins
    assert "http_file" in plugins
    assert "local_file" in plugins
