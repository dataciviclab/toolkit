import importlib
import logging

import pytest

from toolkit.core.registry import PluginRegistrationError, Registry, register_builtin_plugins


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
    assert "http_file" in plugins
    assert "local_file" in plugins


def test_register_builtin_plugins_warns_for_optional_missing_plugin_in_non_strict(
    monkeypatch, caplog, capsys
):
    r = Registry()
    real_import = importlib.import_module

    def _fake_import(name: str, package=None):
        if name == "toolkit.plugins.html_table":
            raise ImportError("missing optional dependency")
        return real_import(name, package)

    monkeypatch.setattr("toolkit.core.registry.importlib.import_module", _fake_import)

    with caplog.at_level(logging.WARNING, logger="toolkit.core.registry"):
        register_builtin_plugins(registry_obj=r, strict=False)

    captured = caplog.text + capsys.readouterr().out
    assert "DCLPLUGIN001" in captured
    assert "html_table" in captured
    assert "html_table" not in r.list_plugins()
    assert "http_file" in r.list_plugins()


def test_register_builtin_plugins_errors_for_optional_missing_plugin_in_strict(monkeypatch):
    r = Registry()
    real_import = importlib.import_module

    def _fake_import(name: str, package=None):
        if name == "toolkit.plugins.html_table":
            raise ImportError("missing optional dependency")
        return real_import(name, package)

    monkeypatch.setattr("toolkit.core.registry.importlib.import_module", _fake_import)

    with pytest.raises(PluginRegistrationError) as exc:
        register_builtin_plugins(registry_obj=r, strict=True)

    assert "DCLPLUGIN001" in str(exc.value)
    assert "html_table" in str(exc.value)
