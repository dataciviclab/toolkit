"""MCP wrapper per catalog ops.

Chiama il backend condiviso ``toolkit.cli.catalog_ops`` e traduce
eccezioni in ``ToolkitClientError`` per il server MCP.

Pattern identico a ``aggregate_ops.py`` che wrappa ``cli.layer_ops``.
"""

from __future__ import annotations

from typing import Any

from lab_connectors.mcp.errors import ErrorCode

from toolkit.cli.catalog_ops import CatalogResolver
from toolkit.mcp.errors import ToolkitClientError

# Istanza condivisa del resolver (lazy init, cache MCP-lifetime)
_resolver: CatalogResolver | None = None


def _get_resolver() -> CatalogResolver:
    global _resolver
    if _resolver is None:
        _resolver = CatalogResolver()
    return _resolver


def reset_resolver() -> None:
    """Resetta il resolver (utile nei test per ricaricare il manifest)."""
    global _resolver
    _resolver = None


def mcp_find(query: str = "", layer: str | None = None, limit: int = 15) -> dict[str, Any]:
    """Cerca dataset nel manifest GCS.

    Args:
        query: Testo da cercare nello slug (case-insensitive).
        layer: ``"clean"``, ``"mart"`` o ``None`` (entrambi).
        limit: Max risultati (default 15). Usa ``0`` per nessun limite.

    Returns:
        Dict con ``datasets`` (lista), ``total_count`` (int totale prima del taglio),
        e ``truncated`` (bool).

    Raises:
        ToolkitClientError: se il manifest non e' raggiungibile.
    """
    try:
        resolver = _get_resolver()
        return resolver.list_datasets(query=query, layer=layer, limit=limit)
    except (FileNotFoundError, TimeoutError) as exc:
        raise ToolkitClientError(
            f"Manifest GCS non raggiungibile: {exc}",
            code=ErrorCode.GCS_UNAVAILABLE,
        ) from exc
    except ValueError as exc:
        raise ToolkitClientError(
            f"Errore nel manifest GCS: {exc}",
            code=ErrorCode.INVALID_PARAMS,
        ) from exc


def mcp_dataset_overview(
    slug: str,
    layer: str = "clean",
    year: int | None = None,
) -> dict[str, Any]:
    """Overview di un dataset: schema, row count e preview.

    Args:
        slug: Slug del dataset.
        layer: ``"clean"`` (default) o ``"mart"``.
        year: Anno specifico o ``None`` (ultimo disponibile).

    Returns:
        Dict con slug, year, layer, columns, row_count, preview.

    Raises:
        ToolkitClientError: se slug non trovato o errore DuckDB.
    """
    try:
        resolver = _get_resolver()
        return resolver.describe_slug(slug, layer=layer, year=year)
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
