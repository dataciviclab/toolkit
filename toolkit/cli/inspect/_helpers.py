"""Shared helpers used by inspect subcommands (paths, schema_diff).

Not part of the public API — internal utility module.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from toolkit.cli.common import load_layer_profile_summaries
from toolkit.core.metadata import read_layer_metadata
from toolkit.core.paths import layer_year_dir
from toolkit.core.run_context import get_run_dir, latest_run
from toolkit.core.support import resolve_support_payloads
from toolkit.profile.raw import build_profile_hints


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
            profile_hints = build_profile_hints(primary_file)
            profile_source = "sniff"
        except Exception as exc:
            sniff_error = f"{type(exc).__name__}: {exc}"

    columns_preview = profile_hints.get("columns_preview") or []
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
        "metadata": str(raw_dir / "metadata.json"),
        "manifest": str(raw_dir / "manifest.json"),
        "validation": str(raw_dir / "raw_validation.json"),
    }


def _clean_output_path(root: Path, dataset: str, year: int) -> Path:
    return layer_year_dir(root, "clean", dataset, year) / f"{dataset}_{year}_clean.parquet"


def _clean_paths(root: Path, dataset: str, year: int) -> dict[str, str]:
    clean_dir = layer_year_dir(root, "clean", dataset, year)
    return {
        "dir": str(clean_dir),
        "output": str(_clean_output_path(root, dataset, year)),
        "metadata": str(clean_dir / "metadata.json"),
        "manifest": str(clean_dir / "manifest.json"),
        "validation": str(clean_dir / "_validate" / "clean_validation.json"),
    }


def _mart_output_paths(root: Path, year_dir: Path, tables: list[dict[str, Any]]) -> list[Path]:
    return [
        year_dir / f"{table['name']}.parquet"
        for table in tables
        if isinstance(table, dict) and table.get("name")
    ]


def _mart_paths(
    root: Path, dataset: str, year: int, tables: list[dict[str, Any]]
) -> dict[str, Any]:
    mart_dir = layer_year_dir(root, "mart", dataset, year)
    return {
        "dir": str(mart_dir),
        "outputs": [str(path) for path in _mart_output_paths(root, mart_dir, tables)],
        "metadata": str(mart_dir / "metadata.json"),
        "manifest": str(mart_dir / "manifest.json"),
        "validation": str(mart_dir / "_validate" / "mart_validation.json"),
    }


def _payload_for_year(cfg, year: int) -> dict[str, Any]:
    root = Path(cfg.root)
    run_dir = get_run_dir(root, cfg.dataset, year)
    mart_tables = cfg.mart.get("tables") or []
    raw_dir = layer_year_dir(root, "raw", cfg.dataset, year)
    raw_meta = read_layer_metadata(raw_dir)
    suggested_read_path = raw_dir / "_profile" / "suggested_read.yml"
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
            "support": resolve_support_payloads(cfg.support, require_exists=False),
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