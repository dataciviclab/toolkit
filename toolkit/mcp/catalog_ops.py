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


def reset_resolver() -> None:
    """Resetta il resolver (utile nei test)."""
    global _resolver
    _resolver = None


def mcp_find(
    query: str = "",
    layer: str | None = None,
    limit: int = 15,
    source: str = "all",
    stage: str = "all",
    status_filter: str | None = None,
) -> dict[str, Any]:
    """Cerca dataset nel manifest GCS e/o workspace locale.

    Args:
        query: Testo da cercare nello slug (case-insensitive).
        layer: ``"clean"``, ``"mart"`` o ``None`` (entrambi).
        limit: Max risultati (default 15). Usa ``0`` per nessun limite.
        source: ``"gcs"`` (pubblicati), ``"workspace"`` (in sviluppo),
                ``"all"`` (default, unione).
        stage: Filtro workspace: ``"candidates"``, ``"support"``, ``"all"``.
        status_filter: Filtro run status (es. ``"SUCCESS"``).

    Returns:
        Dict con ``datasets``, ``total_count``, ``truncated``.

    Raises:
        ToolkitClientError: se manifest GCS irraggiungibile o parametri invalidi.
    """
    try:
        resolver = _get_resolver()
        return resolver.list_datasets(
            query=query,
            layer=layer,
            limit=limit,
            source=source,
            stage=stage,
            status_filter=status_filter,
        )
    except (FileNotFoundError, TimeoutError) as exc:
        raise ToolkitClientError(
            f"Manifest GCS non raggiungibile: {exc}",
            code=ErrorCode.GCS_UNAVAILABLE,
        ) from exc
    except ValueError as exc:
        raise ToolkitClientError(
            f"Errore parametri: {exc}",
            code=ErrorCode.INVALID_PARAMS,
        ) from exc


def mcp_dataset_overview(
    slug: str,
    layer: str = "clean",
    year: int | None = None,
    source: str = "all",
) -> dict[str, Any]:
    """Overview di un dataset: schema, row count e preview.

    Args:
        slug: Slug del dataset.
        layer: ``"clean"`` (default) o ``"mart"``.
        year: Anno specifico o ``None`` (ultimo disponibile).
        source: ``"gcs"``, ``"workspace"``, ``"all"`` (default).

    Returns:
        Dict con slug, year, layer, columns, row_count, preview.

    Raises:
        ToolkitClientError: se slug non trovato o errore DuckDB.
    """
    try:
        resolver = _get_resolver()
        return resolver.describe_slug(slug, layer=layer, year=year, source=source)
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
