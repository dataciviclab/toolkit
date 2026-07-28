"""Discovery: elenca dataset nel workspace.

Deprecato: usa ``toolkit_find(source="workspace")`` via ``CatalogResolver``
in ``toolkit.cli.catalog_ops``. ``list_candidates`` qui e' mantenuto come
delega per backward compatibility.
"""

from __future__ import annotations

from typing import Any, Literal

from toolkit.cli.catalog_ops import CatalogResolver
from toolkit.mcp.path_safety import WORKSPACE_ROOT as WORKSPACE_ROOT  # noqa: F401 — re-export per test backward compat


def list_candidates(
    stage: Literal["candidates", "support", "all"] = "all",
    status_filter: str | None = None,
) -> list[dict[str, Any]]:
    """Elenca dataset nel workspace (deprecato: usa toolkit_find source='workspace').

    Delega a ``CatalogResolver.list_datasets(source='workspace')``.
    """
    resolver = CatalogResolver(include_local=True)
    result = resolver.list_datasets(source="workspace", stage=stage, status_filter=status_filter)
    # Torna solo la lista per backward compat
    return result["datasets"]
