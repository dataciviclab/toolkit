"""Backend condiviso per query su layer RAW/CLEAN/MART.

Usato da:
- CLI ``toolkit inspect config`` (via ``config_ops.py``)
- MCP ``toolkit_layer`` (via ``aggregate_ops.py``)

Le funzioni qui NON gestiscono errori MCP (ToolkitClientError) — quelle
vanno aggiunte nei wrapper MCP. Le funzioni qui sollevano eccezioni
Python standard (ValueError, FileNotFoundError).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from toolkit.core.config import load_config
from toolkit.core.duckdb_shape import parquet_preview
from toolkit.core.io import read_yaml
from toolkit.core.paths import RAW_PROFILE, RAW_SUGGESTED_READ
from toolkit.cli.inspect._helpers import _payload_for_year

# ---------------------------------------------------------------------------
# Schemi validi
# ---------------------------------------------------------------------------

VALID_LAYERS: set[str] = {"raw", "clean", "mart"}
VALID_MODES: set[str] = {"schema", "preview", "profile", "sql"}

# ---------------------------------------------------------------------------
# Helper path
# ---------------------------------------------------------------------------


def _resolve_clean_path(cfg: Any, year: int) -> Path:
    """Risolve il path al parquet clean."""
    paths = _payload_for_year(cfg, year)
    parquet_str = paths["paths"]["clean"].get("output")
    if not parquet_str:
        raise FileNotFoundError("Nessun output clean configurato")
    return Path(parquet_str)


def _resolve_mart_path(cfg: Any, year: int, mart_index: int = 0) -> Path:
    """Risolve il path al parquet mart (indice)."""
    paths = _payload_for_year(cfg, year)
    outputs = paths["paths"]["mart"].get("outputs") or []
    if not outputs:
        raise FileNotFoundError("Nessun output mart configurato")
    if mart_index < 0 or mart_index >= len(outputs):
        raise ValueError(f"Indice mart {mart_index} non valido: {len(outputs)} output disponibili")
    return Path(outputs[mart_index])


def _resolve_raw_dir(cfg: Any, year: int) -> tuple[Path, dict[str, Any]]:
    """Risolve la directory raw e i path info."""
    paths = _payload_for_year(cfg, year)
    raw_dir = Path(paths["paths"]["raw"]["dir"])
    return raw_dir, paths


# ---------------------------------------------------------------------------
# Schema mode
# ---------------------------------------------------------------------------


def show_schema(config_path: str, layer: str = "clean", year: int | None = None) -> dict[str, Any]:
    """Mostra lo schema (colonne + tipi) di raw, clean o mart.

    Args:
        config_path: path al dataset.yml.
        layer: ``"raw"``, ``"clean"`` (default), o ``"mart"``.
        year: anno. Se ``None`` per dataset multi-year usa l'ultimo.

    Returns:
        Dict con schema del layer richiesto.
    """
    # Riutilizza l'implementazione già condivisa in CLI
    from toolkit.cli.inspect.schema_ops import show_schema as _cli_show_schema

    return _cli_show_schema(config_path, layer=layer, year=year)


# ---------------------------------------------------------------------------
# Profile mode (raw only)
# ---------------------------------------------------------------------------


def raw_profile(config_path: str, year: int | None = None) -> dict[str, Any]:
    """Legge il profilo raw (raw_profile.json o suggested_read.yml).

    Args:
        config_path: path al dataset.yml.
        year: anno del dataset. Per multi-year, se omesso usa l'ultimo.

    Returns:
        Dict con encoding, delim, colonne, mapping_suggestions.

    Raises:
        FileNotFoundError: se profilo non trovato.
    """
    from toolkit.cli.inspect._helpers import _payload_for_year

    cfg = load_config(config_path)
    if year is None:
        year = max(cfg.years) if cfg.years else 0
    paths = _payload_for_year(cfg, year)
    raw_dir = Path(paths["paths"]["raw"]["dir"])
    profile_path = raw_dir / "_profile"
    raw_profile_json = profile_path / RAW_PROFILE
    suggested_read_yml = profile_path / RAW_SUGGESTED_READ

    if raw_profile_json.exists():
        profile = json.loads(raw_profile_json.read_text(encoding="utf-8"))
    elif suggested_read_yml.exists():
        raw_yaml = read_yaml(suggested_read_yml)
        clean_section = raw_yaml.get("clean", {}) if isinstance(raw_yaml, dict) else {}
        read_section = clean_section.get("read", {}) if isinstance(clean_section, dict) else {}
        profile = {
            "dataset": None,
            "year": None,
            "encoding_suggested": read_section.get("encoding"),
            "delim_suggested": read_section.get("delim"),
            "decimal_suggested": read_section.get("decimal"),
            "skip_suggested": read_section.get("skip"),
            "robust_read_suggested": None,
            "columns_raw": None,
            "columns_norm": None,
            "missingness_top": [],
            "mapping_suggestions": {},
            "warnings": [],
        }
    else:
        raise FileNotFoundError(
            f"Profilo raw non trovato in {profile_path}. "
            "Nessun file raw_profile.json ne suggested_read.yml."
        )

    return {
        "dataset": profile.get("dataset"),
        "year": profile.get("year"),
        "config_path": str(config_path),
        "profile_path": str(profile_path),
        "file_used": profile.get("file_used"),
        "read_hints": {
            "encoding": profile.get("encoding_suggested"),
            "delimiter": profile.get("delim_suggested"),
            "decimal": profile.get("decimal_suggested"),
            "skip": profile.get("skip_suggested"),
            "robust": profile.get("robust_read_suggested"),
        },
        "header_line": profile.get("header_line"),
        "columns": {
            "raw": profile.get("columns_raw") or [],
            "normalized": profile.get("columns_norm") or [],
            "count": len(profile.get("columns_raw") or []),
        },
        "missingness_top": profile.get("missingness_top", []),
        "mapping_suggestions": profile.get("mapping_suggestions", {}),
        "warnings": profile.get("warnings", []),
        "profile_exists": True,
    }


# ---------------------------------------------------------------------------
# Preview mode
# ---------------------------------------------------------------------------


def _read_parquet_preview(parquet_path: Path, limit: int = 10) -> dict[str, Any]:
    """Legge schema + prime N righe da un parquet.

    Returns:
        Dict con columns (lista di {name, type}), row_count, preview.
    """
    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet non trovato: {parquet_path}")
    if parquet_path.suffix not in (".parquet",):
        raise ValueError(f"Formato non supportato: {parquet_path.suffix}. Solo .parquet.")

    import duckdb

    with duckdb.connect() as conn:
        describe = conn.execute(f"DESCRIBE SELECT * FROM '{parquet_path}'").fetchall()
        columns = [{"name": str(row[0]), "type": str(row[1])} for row in describe]

        row_count_row = conn.execute(f"SELECT COUNT(*) FROM '{parquet_path}'").fetchone()
        row_count = int(row_count_row[0]) if row_count_row else None

        preview_rows = conn.execute(f"SELECT * FROM '{parquet_path}' LIMIT {int(limit)}").fetchall()
        col_names = [c["name"] for c in columns]
        preview = [dict(zip(col_names, row)) for row in preview_rows]

    return {
        "columns": columns,
        "column_count": len(columns),
        "row_count": row_count,
        "preview": preview,
        "truncated": row_count is not None and row_count > limit,
    }


def clean_preview(
    config_path: str,
    layer: str = "clean",
    mart_index: int = 0,
    year: int | None = None,
    limit: int = 10,
) -> dict[str, Any]:
    """Preview dati da un parquet clean o mart.

    Args:
        config_path: path a dataset.yml.
        layer: ``"clean"`` (default) o ``"mart"``.
        mart_index: indice output mart (default 0).
        year: anno. Se None per multi-year usa l'ultimo.
        limit: righe massime (default 10).

    Returns:
        Schema + preview rows del parquet.
    """
    from toolkit.cli.inspect._helpers import _payload_for_year

    cfg = load_config(config_path)
    if year is None:
        year = max(cfg.years) if cfg.years else 0
    paths = _payload_for_year(cfg, year)

    if layer == "clean":
        parquet_path = _resolve_clean_path(cfg, year)
    elif layer == "mart":
        parquet_path = _resolve_mart_path(cfg, year, mart_index)
    else:
        raise ValueError(f"layer deve essere 'clean' o 'mart', non '{layer}'")

    result = _read_parquet_preview(parquet_path, limit=limit)
    result.update(
        {
            "dataset": paths.get("dataset"),
            "year": paths.get("year"),
            "layer": layer,
            "config_path": str(config_path),
            "mart_name": parquet_path.stem if layer == "mart" else None,
        }
    )
    return result


def raw_preview(
    config_path: str,
    year: int | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    """Preview del raw file primario di un dataset (CSV/TSV o XLSX).

    Args:
        config_path: path a dataset.yml.
        year: anno. Se None per multi-year usa l'ultimo.
        limit: righe massime (default 20).

    Returns:
        Dict con preview del raw file.
    """

    cfg = load_config(config_path)
    if year is None:
        year = max(cfg.years) if cfg.years else 0
    raw_dir, paths = _resolve_raw_dir(cfg, year)

    primary_file = (paths.get("raw_hints") or {}).get("primary_output_file")
    if not primary_file:
        raise FileNotFoundError("Nessun primary_output_file nel manifest raw")
    raw_file = raw_dir / primary_file
    if not raw_file.exists():
        raise FileNotFoundError(f"Raw file non trovato: {raw_file}")

    suffix = raw_file.suffix.lower()
    if suffix in (".csv", ".tsv", ".txt"):
        # Riutilizza csv_preview da CLI (già condiviso)
        from toolkit.cli.inspect.profile_ops import csv_preview as _csv_preview

        return _csv_preview(str(raw_file), limit=limit)
    elif suffix in (".xlsx", ".xls"):
        return {
            "path": str(raw_file),
            "format": "xlsx",
            "note": "File binario XLSX. Usa mode='schema' per lo schema colonne.",
            "dataset": paths.get("dataset"),
            "year": paths.get("year"),
        }
    else:
        return {
            "path": str(raw_file),
            "format": suffix.lstrip("."),
            "note": f"Formato '{suffix}' non supportato per preview raw.",
            "dataset": paths.get("dataset"),
            "year": paths.get("year"),
        }


# ---------------------------------------------------------------------------
# SQL mode
# ---------------------------------------------------------------------------


def layer_sql(
    config_path: str,
    layer: str,
    year: int | None = None,
    limit: int = 20,
    sql: str | None = None,
    mart_index: int = 0,
) -> dict[str, Any]:
    """Esegue SQL arbitrario sul parquet risolto da config_path + layer.

    Args:
        config_path: path a dataset.yml.
        layer: ``"clean"`` o ``"mart"``.
        year: anno. Se None per multi-year usa l'ultimo.
        limit: righe massime (default 20).
        sql: query SQL. Il parquet è disponibile come tabella ``data``.
        mart_index: indice tabella mart (default 0).

    Returns:
        Risultato della query SQL.
    """
    if not sql:
        raise ValueError("mode=sql richiede il parametro sql")

    cfg = load_config(config_path)
    if year is None:
        year = max(cfg.years) if cfg.years else 0

    if layer == "clean":
        parquet_path = _resolve_clean_path(cfg, year)
    elif layer == "mart":
        parquet_path = _resolve_mart_path(cfg, year, mart_index)
    else:
        raise ValueError(f"layer deve essere 'clean' o 'mart', non '{layer}'")

    if not parquet_path.exists():
        raise FileNotFoundError(
            f"Parquet {layer} non trovato: {parquet_path}. "
            f"Esegui 'toolkit run all -c {config_path}' per generarlo."
        )

    result = parquet_preview(parquet_path, limit=limit, sql=sql)
    result.update(
        {
            "dataset": cfg.dataset,
            "year": year,
            "layer": layer,
            "config_path": str(config_path),
            "mode": "sql",
        }
    )
    return result


# ---------------------------------------------------------------------------
# Router principale
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

    Args:
        config_path: Path a dataset.yml.
        layer: ``"raw"``, ``"clean"`` (default) o ``"mart"``.
        mode: Cosa restituire:
            - ``"schema"`` (default): colonne + tipi.
            - ``"preview"``: schema + prime N righe.
            - ``"profile"``: profilo diagnostico RAW (solo layer=raw).
            - ``"sql"``: SQL arbitrario sul parquet (solo clean/mart).
        year: Anno del dataset. Se omesso usa l'ultimo anno configurato.
        limit: Max righe in preview (default 20, solo mode=preview/sql).
        sql: Query SQL per mode=sql. Il parquet e' disponibile come tabella ``data``.
        mart_index: Indice della tabella mart (default 0, solo layer=mart).

    Returns:
        Dict con schema, preview o profilo a seconda del mode.

    Raises:
        ValueError: se layer/mode non validi, o file non trovato.
    """
    safe_layer = layer.strip().lower()
    safe_mode = mode.strip().lower() if isinstance(mode, str) else mode

    if safe_layer not in VALID_LAYERS:
        raise ValueError(
            f"layer deve essere uno tra: {', '.join(sorted(VALID_LAYERS))} (ricevuto: {layer})"
        )
    if safe_mode not in VALID_MODES:
        raise ValueError(
            f"mode deve essere uno tra: {', '.join(sorted(VALID_MODES))} (ricevuto: {mode})"
        )
    if safe_mode == "profile" and safe_layer != "raw":
        raise ValueError(f"mode=profile e' valido solo per layer=raw (ricevuto: layer={layer})")
    if safe_mode == "sql" and safe_layer == "raw":
        raise ValueError("mode=sql non e' supportato per layer=raw")
    if safe_mode == "sql" and not sql:
        raise ValueError("mode=sql richiede il parametro sql (es. sql='SELECT * FROM data')")

    # Schema mode
    if safe_mode == "schema":
        return show_schema(config_path, layer=safe_layer, year=year)

    # Profile mode (raw only)
    if safe_mode == "profile":
        return raw_profile(config_path, year=year)

    # Preview mode
    if safe_mode == "preview":
        if safe_layer == "raw":
            return raw_preview(config_path, year=year, limit=limit)
        return clean_preview(
            config_path, layer=safe_layer, mart_index=mart_index, year=year, limit=limit
        )

    # SQL mode
    if safe_mode == "sql":
        return layer_sql(
            config_path, layer=safe_layer, year=year, limit=limit, sql=sql, mart_index=mart_index
        )

    raise RuntimeError(f"mode non gestito: {safe_mode}")
