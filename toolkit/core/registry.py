# toolkit/core/registry.py
from __future__ import annotations

from collections.abc import Callable
from typing import Any


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


registry = Registry()