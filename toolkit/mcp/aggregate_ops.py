"""Consolidated MCP tools for layer query and dataset status.

Questi tool aggregati delegano al backend condiviso ``toolkit.cli.layer_ops``.
CLI e MCP condividono la stessa implementazione.

Backward compat: i tool granulari esistenti (toolkit_inspect_schema,
toolkit_clean_preview, toolkit_summary, ecc.) restano registrati.
"""

from __future__ import annotations

from typing import Any

from lab_connectors.mcp.errors import ErrorCode

from toolkit.cli.layer_ops import layer_query as _layer_query_core
from toolkit.mcp.errors import ToolkitClientError


def _get_impls():
    """Lazy import per evitare dipendenze circolari con server.py."""
    from toolkit.mcp.cli_adapter import inspect_paths as _inspect_paths
    from toolkit.mcp.schema_ops import (
        dataset_info as _dataset_info,
        review_readiness as _review_readiness,
        run_summary as _run_summary,
        summary as _summary,
    )

    return (
        _inspect_paths,
        _dataset_info,
        _review_readiness,
        _run_summary,
        _summary,
    )


# ---------------------------------------------------------------------------
# toolkit_layer — layer query unificato
# ---------------------------------------------------------------------------


def layer_query(
    config_path: str,
    layer: str = "clean",
    mode: str = "schema",
    year: int | None = None,
    limit: int = 20,
    sql: str | None = None,
    mart_index: int = 0,
) -> dict[str, Any]:
    """Query unificata su un layer (RAW/CLEAN/MART).

    Delega al backend condiviso ``toolkit.cli.layer_ops`` — stessa
    implementazione della CLI ``toolkit layer``.

    Args:
        config_path: Path a dataset.yml o slug del dataset.
        layer: ``"raw"``, ``"clean"`` (default) o ``"mart"``.
        mode: Cosa restituire:
            - ``"schema"`` (default): colonne + tipi.
            - ``"preview"``: schema + prime N righe.
            - ``"profile"``: profilo diagnostico RAW (solo layer=raw).
            - ``"sql"``: SQL arbitrario sul parquet (solo clean/mart).
        year: Anno del dataset. Se omesso usa l'ultimo anno configurato.
        limit: Max righe in preview (default 20, solo mode=preview/sql).
        sql: Query SQL. Il parquet e' disponibile come tabella ``data``.
            (solo mode=sql).
        mart_index: Indice della tabella mart (default 0, solo layer=mart).

    Returns:
        Dict con schema, preview o profilo a seconda del mode.

    Raises:
        ToolkitClientError: se layer/mode non validi, o file non trovato.
    """
    from pathlib import Path
    from toolkit.mcp.path_safety import _safe_path

    try:
        resolved_path: Path = _safe_path(config_path)
    except ToolkitClientError:
        resolved_path = Path(config_path)

    try:
        return _layer_query_core(
            str(resolved_path),
            layer=layer,
            mode=mode,
            year=year,
            limit=limit,
            sql=sql,
            mart_index=mart_index,
        )
    except ValueError as exc:
        raise ToolkitClientError(str(exc), code=ErrorCode.INVALID_PARAMS) from exc
    except FileNotFoundError as exc:
        raise ToolkitClientError(str(exc), code=ErrorCode.PARQUET_NOT_FOUND) from exc


# ---------------------------------------------------------------------------
# toolkit_status — stato dataset unificato
# ---------------------------------------------------------------------------


def dataset_status(
    config_path: str,
    year: int | None = None,
) -> dict[str, Any]:
    """Stato completo di un dataset: percorso, run, validation, info.

    Aggrega in una chiamata: paths_info + summary + readiness + run_stats + info.

    Args:
        config_path: Path a dataset.yml o slug del dataset.
        year: Anno del dataset. Se omesso usa l'ultimo anno configurato.

    Returns:
        Dict con sezioni: paths_info, summary, readiness, run_stats, info.
    """
    (
        _inspect_paths,
        _dataset_info,
        _review_readiness,
        _run_summary,
        _summary,
    ) = _get_impls()

    return {
        "paths_info": _inspect_paths(config_path, year=year),
        "summary": _summary(config_path, year=year),
        "readiness": _review_readiness(config_path, year=year),
        "run_stats": _run_summary(config_path, year=year),
        "info": _dataset_info(config_path),
    }
