# toolkit/core/registry.py
from __future__ import annotations

import importlib
import logging
from collections.abc import Callable
from typing import Any

logger = logging.getLogger("toolkit.core.registry")


class PluginRegistrationError(RuntimeError):
    pass


class Registry:
    def __init__(self):
        self._plugins: dict[str, Callable[..., Any]] = {}

    def register(self, name: str, factory: Callable[..., Any], *, overwrite: bool = False) -> None:
        if not overwrite and name in self._plugins:
            raise ValueError(f"Plugin già registrato: '{name}'")
        self._plugins[name] = factory

    def decorator(self, name: str, *, overwrite: bool = False):
        def _wrap(factory: Callable[..., Any]):
            self.register(name, factory, overwrite=overwrite)
            return factory
        return _wrap

    def create(self, name: str, **kwargs):
        if name not in self._plugins:
            available = ", ".join(sorted(self._plugins.keys())) or "(none)"
            raise KeyError(f"Plugin sconosciuto: '{name}'. Disponibili: {available}")
        return self._plugins[name](**kwargs)

    def list_plugins(self) -> list[str]:
        return sorted(self._plugins.keys())

    def clear(self) -> None:
        self._plugins.clear()


registry = Registry()

_BUILTIN_PLUGINS: tuple[dict[str, Any], ...] = (
    {
        "name": "ckan",
        "module": "toolkit.plugins.ckan",
        "class_name": "CkanSource",
        "optional": False,
        "factory": lambda cls: (lambda **client: cls(**client)),
    },
    {
        "name": "sdmx",
        "module": "toolkit.plugins.sdmx",
        "class_name": "SdmxSource",
        "optional": False,
        "factory": lambda cls: (lambda **client: cls(**client)),
    },
    {
        "name": "http_file",
        "module": "toolkit.plugins.http_file",
        "class_name": "HttpFileSource",
        "optional": False,
        "factory": lambda cls: (lambda **client: cls(**client)),
    },
    {
        "name": "local_file",
        "module": "toolkit.plugins.local_file",
        "class_name": "LocalFileSource",
        "optional": False,
        "factory": lambda cls: (lambda **client: cls()),
    },
)


def register_builtin_plugins(
    *,
    strict: bool = False,
    registry_obj: Registry | None = None,
) -> None:
    target = registry_obj or registry

    for spec in _BUILTIN_PLUGINS:
        if spec["name"] in target.list_plugins():
            continue

        try:
            module = importlib.import_module(spec["module"])
            plugin_class = getattr(module, spec["class_name"])
            target.register(spec["name"], spec["factory"](plugin_class))
        except Exception as exc:
            raise PluginRegistrationError(
                f"Required built-in plugin '{spec['name']}' failed to register: {exc}"
            ) from exc
