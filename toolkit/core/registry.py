# toolkit/core/registry.py
from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import Any


class PluginRegistrationError(RuntimeError):
    pass


class Registry:
    def __init__(self):
        self._plugins: dict[str, Callable[..., Any]] = {}

    def register(self, name: str, factory: Callable[..., Any], *, overwrite: bool = False) -> None:
        if not overwrite and name in self._plugins:
            raise ValueError(f"Plugin già registrato: '{name}'")
        self._plugins[name] = factory

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


def register_builtin_plugins(
    *,
    registry_obj: Registry | None = None,
) -> None:
    """Registra tutti i plugin del catalogo ``core.source_types``.

    I built-in senza classe plugin (es. ``script``) sono skippati: il loro
    fetch adapter gira senza istanza.
    """
    from toolkit.core.source_types import SOURCE_TYPES

    target = registry_obj or registry

    for spec in SOURCE_TYPES.values():
        if spec.module is None or spec.class_name is None or spec.factory is None:
            continue
        if spec.name in target.list_plugins():
            continue

        try:
            module = importlib.import_module(spec.module)
            plugin_class = getattr(module, spec.class_name)
            target.register(spec.name, spec.factory(plugin_class))
        except Exception as exc:
            raise PluginRegistrationError(
                f"Required built-in plugin '{spec.name}' failed to register: {exc}"
            ) from exc
