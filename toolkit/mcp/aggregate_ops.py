"""Consolidated MCP tools for layer query and dataset status.

Questi tool aggregati delegano alle implementazioni esistenti (schema_ops,
scout_ops, cli_adapter, discovery) — zero logica nuova, solo routing.

Backward compat: i tool granulari esistenti (toolkit_inspect_schema,
toolkit_clean_preview, toolkit_summary, ecc.) restano registrati.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from lab_connectors.mcp.errors import ErrorCode

from toolkit.mcp.errors import ToolkitClientError
from toolkit.mcp.path_safety import _load_cfg


def _get_impls():
    """Lazy import per evitare dipendenze circolari con server.py."""
    from toolkit.mcp.cli_adapter import inspect_paths as _inspect_paths
    from toolkit.mcp.schema_ops import (
        clean_preview as _clean_preview,
        dataset_info as _dataset_info,
        raw_preview as _raw_preview,
        raw_profile as _raw_profile,
        review_readiness as _review_readiness,
        run_summary as _run_summary,
        show_schema as _show_schema,
        summary as _summary,
    )

    return (
        _inspect_paths,
        _clean_preview,
        _dataset_info,
        _raw_preview,
        _raw_profile,
        _review_readiness,
        _run_summary,
        _show_schema,
        _summary,
    )


# ---------------------------------------------------------------------------
# toolkit_layer — layer query unificato
# ---------------------------------------------------------------------------

VALID_LAYERS = {"raw", "clean", "mart"}
VALID_MODES = {"schema", "preview", "profile", "sql"}


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
    safe_layer = layer.strip().lower()
    safe_mode = mode.strip().lower()

    if safe_layer not in VALID_LAYERS:
        raise ToolkitClientError(
            f"layer deve essere uno tra: {', '.join(sorted(VALID_LAYERS))} (ricevuto: {layer})",
            code=ErrorCode.INVALID_PARAMS,
        )
    if safe_mode not in VALID_MODES:
        raise ToolkitClientError(
            f"mode deve essere uno tra: {', '.join(sorted(VALID_MODES))} (ricevuto: {mode})",
            code=ErrorCode.INVALID_PARAMS,
        )
    if safe_mode == "profile" and safe_layer != "raw":
        raise ToolkitClientError(
            f"mode=profile e' valido solo per layer=raw (ricevuto: layer={layer})",
            code=ErrorCode.INVALID_PARAMS,
        )
    if safe_mode == "sql" and safe_layer == "raw":
        raise ToolkitClientError(
            "mode=sql non e' supportato per layer=raw (usa mode=schema o mode=preview)",
            code=ErrorCode.INVALID_PARAMS,
        )
    if safe_mode == "sql" and not sql:
        raise ToolkitClientError(
            "mode=sql richiede il parametro sql (es. sql='SELECT * FROM data')",
            code=ErrorCode.INVALID_PARAMS,
        )

    (
        _inspect_paths,
        _clean_preview,
        _dataset_info,
        _raw_preview,
        _raw_profile,
        _review_readiness,
        _run_summary,
        _show_schema,
        _summary,
    ) = _get_impls()

    # Schema mode
    if safe_mode == "schema":
        return _show_schema(config_path, layer=safe_layer, year=year)

    # Profile mode (raw only)
    if safe_mode == "profile":
        return _raw_profile(config_path, year=year)

    # Preview mode
    if safe_mode == "preview":
        if safe_layer == "raw":
            return _raw_preview(config_path, year=year, limit=limit)
        return _clean_preview(
            config_path, layer=safe_layer, mart_index=mart_index, year=year, limit=limit
        )

    # SQL mode — sql non puo' essere None qui (guardato sopra)
    if safe_mode == "sql":
        assert sql is not None  # mypy narrowing
        return _layer_sql(
            config_path, layer=safe_layer, year=year, limit=limit, sql=sql, mart_index=mart_index
        )

    raise RuntimeError(f"mode non gestito: {safe_mode}")


def _layer_sql(
    config_path: str,
    layer: str,
    year: int | None,
    limit: int,
    sql: str,
    mart_index: int,
) -> dict[str, Any]:
    """Esegue SQL arbitrario sul parquet risolto da config_path + layer.

    Logica di risoluzione path identica a clean_preview/raw_preview.
    """
    config, cfg = _load_cfg(config_path)
    from toolkit.mcp.cli_adapter import inspect_paths as _inspect_paths

    paths = _inspect_paths(str(config), year)
    resolved_year = paths.get("year", year)

    if layer == "clean":
        parquet_str = paths["paths"]["clean"].get("output")
        if not parquet_str:
            raise ToolkitClientError(
                "Nessun output clean configurato", code=ErrorCode.PARQUET_NOT_FOUND
            )
        parquet_path = Path(parquet_str)
    else:
        outputs = paths["paths"]["mart"].get("outputs") or []
        if not outputs:
            raise ToolkitClientError(
                "Nessun output mart configurato", code=ErrorCode.PARQUET_NOT_FOUND
            )
        if mart_index < 0 or mart_index >= len(outputs):
            raise ToolkitClientError(
                f"Indice mart {mart_index} non valido: {len(outputs)} output disponibili",
                code=ErrorCode.INVALID_PARAMS,
            )
        parquet_path = Path(outputs[mart_index])

    if not parquet_path.exists():
        raise ToolkitClientError(
            f"Parquet {layer} non trovato: {parquet_path}. "
            f"Esegui 'toolkit run all -c {config_path}' per generarlo.",
            code=ErrorCode.PARQUET_NOT_FOUND,
        )

    from toolkit.core.duckdb_shape import parquet_preview

    result = parquet_preview(parquet_path, limit=limit, sql=sql)
    result.update(
        {
            "dataset": paths.get("dataset"),
            "year": resolved_year,
            "layer": layer,
            "config_path": str(config),
            "mode": "sql",
        }
    )
    return result


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
        _clean_preview,
        _dataset_info,
        _raw_preview,
        _raw_profile,
        _review_readiness,
        _run_summary,
        _show_schema,
        _summary,
    ) = _get_impls()

    return {
        "paths_info": _inspect_paths(config_path, year=year),
        "summary": _summary(config_path, year=year),
        "readiness": _review_readiness(config_path, year=year),
        "run_stats": _run_summary(config_path, year=year),
        "info": _dataset_info(config_path),
    }
