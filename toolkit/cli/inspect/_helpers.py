"""Shared helpers used by inspect subcommands (paths, schema_diff).

Not part of the public API — internal utility module.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from toolkit.cli.common import load_layer_profile_summaries
from toolkit.core.config import ensure_dict
from toolkit.core.metadata import read_layer_metadata
from toolkit.core.paths import (
    METADATA,
    RAW_PROFILE,
    RAW_PROFILE_DIR,
    RAW_SUGGESTED_READ,
    RAW_VALIDATION,
    CLEAN_VALIDATION,
    MART_VALIDATION,
    layer_year_dir,
)
from toolkit.core.run_records import get_run_dir, latest_run
from toolkit.core.support import resolve_support_payloads
from toolkit.core.io import read_json_or_none
from toolkit.profile.raw import sniff_source_file


def _raw_primary_file(raw_dir: Path, metadata: dict[str, Any]) -> Path | None:
    primary_output_file = metadata.get("primary_output_file")
    if isinstance(primary_output_file, str):
        candidate = raw_dir / primary_output_file
        if candidate.exists():
            return candidate
    return None


def _raw_schema_payload(cfg, year: int) -> dict[str, Any]:
    root = Path(cfg.root)
    raw_dir = layer_year_dir(root, "raw", cfg.dataset, year)
    raw_meta = read_layer_metadata(raw_dir)
    primary_file = _raw_primary_file(raw_dir, raw_meta)

    profile_hints = raw_meta.get("profile_hints") or {}
    profile_source = "metadata" if profile_hints else None
    sniff_error: str | None = None

    if not profile_hints and primary_file is not None:
        try:
            profile_hints = sniff_source_file(primary_file)
            profile_source = "sniff"
        except Exception as exc:
            sniff_error = f"{type(exc).__name__}: {exc}"

    # For binary files (XLSX/XLS), sniff_hints columns_preview is empty.
    # Read actual columns from raw_profile.json if available.
    columns_preview = list(profile_hints.get("columns_preview") or [])
    if not columns_preview and profile_hints.get("is_binary_file"):
        raw_profile_path = raw_dir / RAW_PROFILE_DIR / RAW_PROFILE
        if raw_profile_path.exists():
            raw_profile_data = read_json_or_none(raw_profile_path)
            if raw_profile_data:
                columns_preview = (
                    raw_profile_data.get("columns_norm")
                    or raw_profile_data.get("columns_raw")
                    or []
                )

    warnings = list(profile_hints.get("warnings") or [])
    if sniff_error is not None:
        warnings.append(f"profile_hint_fallback_failed: {sniff_error}")

    return {
        "year": year,
        "raw_dir": str(raw_dir),
        "raw_exists": raw_dir.exists(),
        "primary_output_file": raw_meta.get("primary_output_file"),
        "file_used": profile_hints.get("file_used"),
        "profile_source": profile_source,
        "is_binary_file": profile_hints.get("is_binary_file"),
        "encoding": profile_hints.get("encoding_suggested"),
        "delim": profile_hints.get("delim_suggested"),
        "decimal": profile_hints.get("decimal_suggested"),
        "skip": profile_hints.get("skip_suggested"),
        "header_line": profile_hints.get("header_line"),
        "columns_count": len(columns_preview),
        "columns_preview": columns_preview,
        "warnings": warnings,
    }


def _compare_schema_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    comparisons: list[dict[str, Any]] = []
    for previous, current in zip(entries, entries[1:]):
        previous_columns = set(previous.get("columns_preview") or [])
        current_columns = set(current.get("columns_preview") or [])
        comparisons.append(
            {
                "from_year": previous["year"],
                "to_year": current["year"],
                "from_columns_count": previous.get("columns_count") or 0,
                "to_columns_count": current.get("columns_count") or 0,
                "added_columns": sorted(current_columns - previous_columns),
                "removed_columns": sorted(previous_columns - current_columns),
                "changed": previous_columns != current_columns,
            }
        )
    return comparisons


def _raw_output_paths(root: Path, dataset: str, year: int) -> dict[str, str]:
    raw_dir = layer_year_dir(root, "raw", dataset, year)
    return {
        "dir": str(raw_dir),
        "metadata": str(raw_dir / METADATA),
        "validation": str(raw_dir / RAW_VALIDATION),
    }


def _clean_output_path(root: Path, dataset: str, year: int) -> Path:
    return layer_year_dir(root, "clean", dataset, year) / f"{dataset}_{year}_clean.parquet"


def _clean_paths(root: Path, dataset: str, year: int) -> dict[str, str]:
    clean_dir = layer_year_dir(root, "clean", dataset, year)
    return {
        "dir": str(clean_dir),
        "output": str(_clean_output_path(root, dataset, year)),
        "metadata": str(clean_dir / METADATA),
        "validation": str(clean_dir / CLEAN_VALIDATION),
    }


def _mart_output_paths(root: Path, year_dir: Path, tables: list[Any]) -> list[Path]:  # noqa: ARG001
    result: list[Path] = []
    for table in tables:
        if isinstance(table, dict):
            name = table.get("name")
        elif hasattr(table, "name"):
            name = table.name
        else:
            continue
        if name:
            result.append(year_dir / f"{name}.parquet")
    return result


def _mart_paths(
    root: Path, dataset: str, year: int, tables: list[dict[str, Any]]
) -> dict[str, Any]:
    mart_dir = layer_year_dir(root, "mart", dataset, year)
    return {
        "dir": str(mart_dir),
        "outputs": [str(path) for path in _mart_output_paths(root, mart_dir, tables)],
        "metadata": str(mart_dir / METADATA),
        "validation": str(mart_dir / MART_VALIDATION),
    }


def _payload_for_year(cfg, year: int) -> dict[str, Any]:
    root = Path(cfg.root)
    run_dir = get_run_dir(root, cfg.dataset, year)
    mart_tables = cfg.mart.tables
    raw_dir = layer_year_dir(root, "raw", cfg.dataset, year)
    raw_meta = read_layer_metadata(raw_dir)
    suggested_read_path = raw_dir / RAW_PROFILE_DIR / RAW_SUGGESTED_READ
    profile_hints = raw_meta.get("profile_hints") or {}

    run_files = sorted(run_dir.glob("*.json")) if run_dir.exists() else []
    years_seen = (
        sorted({p.parent.name for p in run_dir.parent.glob("*/*.json") if p.parent.name.isdigit()})
        if run_dir.parent.exists()
        else []
    )

    latest_payload: dict[str, Any] | None = None
    try:
        latest_record = latest_run(run_dir)
        latest_payload = {
            "run_id": latest_record.get("run_id"),
            "status": latest_record.get("status"),
            "started_at": latest_record.get("started_at"),
            "path": str(run_dir / f"{latest_record.get('run_id')}.json"),
        }
    except FileNotFoundError:
        latest_payload = None

    return {
        "dataset": cfg.dataset,
        "year": year,
        "config_path": str(cfg.base_dir / "dataset.yml"),
        "root": str(root),
        "paths": {
            "raw": _raw_output_paths(root, cfg.dataset, year),
            "clean": _clean_paths(root, cfg.dataset, year),
            "mart": _mart_paths(root, cfg.dataset, year, mart_tables),
            "support": resolve_support_payloads(ensure_dict(cfg.support), require_exists=False),
            "run_dir": str(run_dir),
        },
        "run_file_count": len(run_files),
        "years_seen": years_seen,
        "raw_hints": {
            "primary_output_file": raw_meta.get("primary_output_file"),
            "suggested_read_path": str(suggested_read_path),
            "suggested_read_exists": suggested_read_path.exists(),
            "encoding": profile_hints.get("encoding_suggested"),
            "delim": profile_hints.get("delim_suggested"),
            "decimal": profile_hints.get("decimal_suggested"),
            "skip": profile_hints.get("skip_suggested"),
            "warnings": profile_hints.get("warnings") or [],
        },
        "layer_profiles": load_layer_profile_summaries(root, cfg.dataset, year),
        "latest_run": latest_payload,
    }


# ---------------------------------------------------------------------------
# DuckDB helpers — schema, row count, preview da parquet e CSV
# Condivise da cli/inspect e mcp (MCP wrappa con ToolkitClientError).
# ---------------------------------------------------------------------------
# Le implementazioni reali sono in toolkit.core.parquet.
# Questi wrapper servono solo per chiudere errori con
# FileNotFoundError/RuntimeError invece di return silenziosi.


def _schema_from_parquet(parquet_path: Path) -> dict[str, Any]:
    from toolkit.core.duckdb_shape import parquet_schema

    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet non trovato: {parquet_path}")
    cols = parquet_schema(parquet_path)
    if not cols:
        raise RuntimeError(f"Lettura schema parquet fallita per {parquet_path}")
    return {"path": str(parquet_path), "column_count": len(cols), "columns": cols}


def _read_parquet_row_count(parquet_path: Path | None) -> int | None:
    from toolkit.core.duckdb_shape import parquet_row_count

    if parquet_path is None:
        return None
    return parquet_row_count(parquet_path)


def _read_parquet_preview(parquet_path: Path, limit: int = 10) -> dict[str, Any]:
    from toolkit.core.duckdb_shape import parquet_preview

    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet non trovato: {parquet_path}")
    result = parquet_preview(parquet_path, limit=limit)
    if not result["columns"]:
        raise RuntimeError(f"Lettura schema parquet fallita per {parquet_path}")
    return result


def _exists(path: str | None) -> bool:
    """Return True if path is a real file/directory."""
    if not path:
        return False
    return Path(path).exists()


def _read_validation_content(path: str | None) -> dict[str, Any] | None:
    """Read a validation JSON file and return its content, or None if missing."""
    if not path or not _exists(path):
        return None
    return read_json_or_none(Path(path))


def _check_run_record_coherence(
    run_record: dict[str, Any] | None,
    layers: dict[str, Any],
) -> list[dict[str, str]]:
    """Verifica che i layer marcati SUCCESS nel run record abbiano output reali.

    Ritorna una lista di hint (dict con code, severity, message).
    """
    hints: list[dict[str, str]] = []
    if not run_record:
        return hints

    layers_map = run_record.get("layers") or {}
    for layer_name, layer_detail in layers_map.items():
        layer_status = (
            layer_detail.get("status") if isinstance(layer_detail, dict) else layer_detail
        )
        if layer_status != "SUCCESS":
            continue

        layer_info = layers.get(layer_name, {})

        if layer_name == "clean" and not layer_info.get("output_exists"):
            hints.append(
                {
                    "code": "run_says_clean_success_but_output_missing",
                    "severity": "blocker",
                    "message": "run record dice clean SUCCESS ma output file manca",
                }
            )
        elif layer_name == "mart":
            out_count = layer_info.get("output_count", 0) or 0
            exists_count = layer_info.get("output_exists_count", 0) or 0
            if exists_count == 0 and out_count > 0:
                hints.append(
                    {
                        "code": "run_says_mart_success_but_outputs_missing",
                        "severity": "blocker",
                        "message": "run record dice mart SUCCESS ma nessun output file presente",
                    }
                )

    return hints


def _validation_summary_for_layer(
    layer_dir: Path, validation_filename: str
) -> dict[str, Any] | None:
    """Extract summary from a layer's validation JSON.

    Adds: ok, errors_count, warnings_count, row_count, col_count,
    raw_row_count, clean_row_count.
    Reads row/col counts from summary.stats (clean) or summary.row_counts (mart).
    Falls back to sections.stats for layers that use that path.
    Returns None if the validation file does not exist.
    """
    validation_path = layer_dir / validation_filename
    content = _read_validation_content(str(validation_path))
    if not content:
        return None

    result = {
        "ok": content.get("ok"),
        "errors_count": len(content.get("errors", [])),
        "warnings_count": len(content.get("warnings", [])),
        "row_count": None,
        "col_count": None,
    }

    # Extract stats from summary (clean layer: summary.stats.clean_rows/clean_cols)
    summary = content.get("summary", {})
    stats = summary.get("stats", {})
    result["row_count"] = stats.get("clean_rows") or stats.get("row_count")
    result["col_count"] = stats.get("clean_cols")

    # Fallback: sections.stats (mart layer uses sections differently)
    sections = content.get("sections", {})
    if result["row_count"] is None and "stats" in sections:
        result["row_count"] = sections["stats"].get("row_count")
        result["col_count"] = sections["stats"].get("col_count")

    # Extract transition metadata (clean validation)
    if "transition" in sections:
        t = sections["transition"]
        if "clean_cols" in t:
            result["col_count"] = t.get("clean_cols")
        if "raw_row_count" in t:
            result["raw_row_count"] = t.get("raw_row_count")
        if "clean_row_count" in t:
            result["clean_row_count"] = t.get("clean_row_count")

    # Extract row_counts from mart summary (mart layer)
    if result["row_count"] is None:
        row_counts = summary.get("row_counts", {})
        if row_counts:
            first_key = next(iter(row_counts), None)
            if first_key:
                result["row_count"] = row_counts[first_key]

    return result
