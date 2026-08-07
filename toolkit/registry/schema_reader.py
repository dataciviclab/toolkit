"""Schema colonne per gli artifact registry.

Lo schema dei parquet clean è letto da ``toolkit.core.duckdb_shape.parquet_schema``
(il reader runtime del toolkit); qui solo l'arricchimento di catalogo:
mappatura tipo DuckDB→catalogo, role (dimension/metric) e semantic_type.

La mappatura tipi replica quella di ``dataset-incubator/scripts/build_clean_catalog.py``:
il parquet locale è lo stesso file che verrà pushato su GCS.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from toolkit.core.duckdb_shape import parquet_schema
from toolkit.domain.path_resolver import payload_for_year

from toolkit.registry.layout import DatasetManifest

# Mappa tipi DuckDB → tipo catalogo (preserva BIGINT vs INTEGER)
DUCKDB_TO_CATALOG: dict[str, str] = {
    "integer": "INTEGER",
    "int32": "INTEGER",
    "int": "INTEGER",
    "bigint": "BIGINT",
    "int64": "BIGINT",
    "smallint": "INTEGER",
    "tinyint": "INTEGER",
    "hugeint": "BIGINT",
    "float": "DOUBLE",
    "real": "DOUBLE",
    "double": "DOUBLE",
    "decimal": "DOUBLE",
    "numeric": "DOUBLE",
    "varchar": "VARCHAR",
    "text": "VARCHAR",
    "char": "VARCHAR",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "timestamp_s": "TIMESTAMP",
    "timestamp_ms": "TIMESTAMP",
    "timestamp_ns": "TIMESTAMP",
    "time": "TIME",
    "boolean": "BOOLEAN",
    "bool": "BOOLEAN",
}

_DIMENSION_TYPES = {"VARCHAR", "DATE", "BOOLEAN"}


def load_semantic_types(path: Path | None) -> dict[str, str]:
    """Carica alias_map da un semantic_types.yaml (alias.lower → semantic_type).

    Nessun partial/substring match — solo match esatto. Il file è opzionale:
    senza, le colonne non ricevono semantic_type.
    """
    if path is None or not path.is_file():
        return {}
    import yaml

    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    alias_map: dict[str, str] = {}
    for stype, info in (data.get("types") or {}).items():
        for alias in info.get("aliases", []) if isinstance(info, dict) else []:
            alias_lower = str(alias).lower()
            if alias_lower not in alias_map:
                alias_map[alias_lower] = stype
    return alias_map


def _assign_semantic_type(col_name: str, alias_map: dict[str, str]) -> str | None:
    return alias_map.get(col_name.lower())


def clean_parquet_path(cfg: Any, year: int) -> Path | None:
    """Path del parquet clean locale per anno (path resolver del toolkit).

    Returns:
        Path se il file esiste, altrimenti None.
    """
    try:
        payload = payload_for_year(cfg, year)
        output = (payload.get("paths") or {}).get("clean", {}).get("output")
    except Exception:
        return None
    if not output:
        return None
    path = Path(output)
    return path if path.is_file() else None


def parquet_columns(
    parquet_path: Path | None,
    alias_map: dict[str, str] | None = None,
) -> list[dict[str, Any]] | None:
    """Schema del parquet (via reader runtime) → colonne del catalogo."""
    if parquet_path is None:
        return None
    alias_map = alias_map or {}
    try:
        rows = parquet_schema(parquet_path)
    except Exception:
        return None
    if not rows:
        return None

    columns: list[dict[str, Any]] = []
    for row in rows:
        col_name = str(row.get("name", ""))
        raw_type = str(row.get("type", "")).lower()
        bq_type = DUCKDB_TO_CATALOG.get(raw_type, "VARCHAR")
        role = "dimension" if bq_type in _DIMENSION_TYPES else "metric"
        col_entry: dict[str, Any] = {
            "name": col_name,
            "type": bq_type,
            "role": role,
            "description": "",
        }
        semantic_type = _assign_semantic_type(col_name, alias_map)
        if semantic_type:
            col_entry["semantic_type"] = semantic_type
        columns.append(col_entry)
    return columns


def latest_clean_columns(
    manifest: DatasetManifest,
    alias_map: dict[str, str] | None = None,
) -> tuple[list[dict[str, Any]] | None, int | None]:
    """Schema del parquet clean più recente presente in locale.

    Returns:
        Tuple (columns, latest_year) — (None, None) se nessun parquet locale.
    """
    if not manifest.years:
        return None, None
    for year in sorted(manifest.years, reverse=True):
        parquet = clean_parquet_path(manifest.cfg, year)
        if parquet is not None:
            return parquet_columns(parquet, alias_map), year
    return None, None
