"""Compat shim: re-export TypedDict contracts from ``types.py``.

``toolkit.mcp.contracts`` was renamed to ``toolkit.mcp.types`` to avoid
naming collision with ``toolkit.contracts`` (path contract API).

This shim preserves backward compatibility for any downstream consumer
that imports from the old path.
"""
from __future__ import annotations

from toolkit.mcp.types import *  # noqa: F401, F403
