"""MCP wrapper per catalog ops.

Chiama il backend condiviso ``toolkit.cli.catalog_ops`` e traduce
eccezioni in ``ToolkitClientError`` per il server MCP.
"""

from __future__ import annotations

from typing import Any

from lab_connectors.mcp.errors import ErrorCode

from toolkit.domain.catalog import CatalogResolver
from toolkit.mcp.errors import ToolkitClientError

_resolver: CatalogResolver | None = None


def _get_resolver() -> CatalogResolver:
    global _resolver
    if _resolver is None:
        _resolver = CatalogResolver()
    return _resolver


def mcp_find(
    query: str = "",
    layer: str | None = None,
    limit: int = 15,
    source: str = "all",
    stage: str = "all",
    status_filter: str | None = None,
    metric_only: bool = False,
) -> dict[str, Any]:
    """Cerca dataset nel manifest GCS e/o workspace locale."""
    try:
        resolver = _get_resolver()
        return resolver.list_datasets(
            query=query,
            layer=layer,
            limit=limit,
            source=source,
            stage=stage,
            status_filter=status_filter,
            metric_only=metric_only,
        )
    except (FileNotFoundError, TimeoutError) as exc:
        raise ToolkitClientError(
            f"Manifest GCS non raggiungibile: {exc}",
            code=ErrorCode.GCS_UNAVAILABLE,
        ) from exc
    except ValueError as exc:
        raise ToolkitClientError(
            f"Errori parametri: {exc}",
            code=ErrorCode.INVALID_PARAMS,
        ) from exc


def mcp_dataset_overview(
    slug: str,
    layer: str = "clean",
    year: int | None = None,
    source: str = "all",
    profile: bool = False,
) -> dict[str, Any]:
    """Overview di un dataset: schema, row count e preview."""
    try:
        resolver = _get_resolver()
        return resolver.describe_slug(slug, layer=layer, year=year, source=source, profile=profile)
    except FileNotFoundError as exc:
        raise ToolkitClientError(
            str(exc),
            code=ErrorCode.PARQUET_NOT_FOUND,
        ) from exc
    except RuntimeError as exc:
        raise ToolkitClientError(
            str(exc),
            code=ErrorCode.UNEXPECTED,
        ) from exc
