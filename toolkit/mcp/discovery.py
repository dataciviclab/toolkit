"""Discovery: elenca dataset nel workspace.

Deprecato: usa ``toolkit_find(source="workspace")`` via ``CatalogResolver``
in ``toolkit.cli.catalog_ops``. ``list_candidates`` qui e' mantenuto come
delega per backward compatibility.
"""

from __future__ import annotations

from typing import Any, Literal

from toolkit.domain.catalog import CatalogResolver
from toolkit.mcp.errors import ToolkitClientError
from toolkit.mcp.path_safety import WORKSPACE_ROOT  # noqa: F401 — ri-esportato per backward compat test


def list_candidates(
    stage: Literal["candidates", "support", "all"] = "all",
    status_filter: str | None = None,
) -> list[dict[str, Any]]:
    """Elenca dataset nel workspace (deprecato: usa toolkit_find source='workspace').

    Delega a ``CatalogResolver.list_datasets(source='workspace')``.
    Usa ``discovery.WORKSPACE_ROOT`` (monkeypatch-safe per test).
    """
    from lab_connectors.mcp.errors import ErrorCode

    resolver = CatalogResolver(include_local=True, workspace=WORKSPACE_ROOT)
    try:
        result = resolver.list_datasets(
            source="workspace", stage=stage, status_filter=status_filter
        )
    except ValueError as exc:
        raise ToolkitClientError(str(exc), code=ErrorCode.INVALID_PARAMS) from exc
    return result["datasets"]
