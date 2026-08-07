"""Wrapper MCP per la lettura degli artifact registry committati.

Speculare al comando CLI ``toolkit registry`` (backend condiviso in
``toolkit.domain.registry_ops``).
"""

from __future__ import annotations

from typing import Any

from lab_connectors.mcp.errors import ErrorCode

from toolkit.domain.registry_ops import list_registries as _list_registries
from toolkit.domain.registry_ops import show_registry as _show_registry
from toolkit.mcp.errors import ToolkitClientError


def mcp_registry_list() -> dict[str, Any]:
    """Elenca gli artifact registry committati nei repo del workspace."""
    try:
        return _list_registries()
    except Exception as exc:
        raise ToolkitClientError(f"Errore lettura registry: {exc}", code=ErrorCode.UNEXPECTED)


def mcp_registry_show(
    repo: str,
    artifact: str,
    slug: str | None = None,
) -> dict[str, Any]:
    """Legge un artifact registry di un repo del workspace."""
    try:
        return _show_registry(repo, artifact, slug=slug)
    except FileNotFoundError as exc:
        raise ToolkitClientError(str(exc), code=ErrorCode.PARQUET_NOT_FOUND) from exc
    except Exception as exc:
        raise ToolkitClientError(f"Errore lettura registry: {exc}", code=ErrorCode.UNEXPECTED)
