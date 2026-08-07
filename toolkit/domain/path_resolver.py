"""Path resolver — costruisce path e payload diagnostici per dataset/year.

Estratto da ``toolkit.cli.inspect._helpers`` per eliminare la dipendenza
``domain → cli/``. Funzioni di pura logica di dominio.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from toolkit.core.config import ensure_dict
from toolkit.core.metadata import read_layer_metadata
from toolkit.core.paths import (
    METADATA,
    RAW_PROFILE_DIR,
    RAW_SUGGESTED_READ,
    layer_dataset_dir,
    layer_year_dir,
)
from toolkit.core.run_records import get_run_dir, latest_run
from toolkit.core.support import resolve_support_payloads


def _raw_output_paths(root: Path, dataset: str, year: int) -> dict[str, str | None]:
    raw_dir = layer_year_dir(root, "raw", dataset, year)
    return {
        "dir": str(raw_dir),
        "metadata": str(raw_dir / METADATA),
        "validation": None,
    }


def _clean_output_path(root: Path, dataset: str, year: int) -> Path:
    return layer_year_dir(root, "clean", dataset, year) / f"{dataset}_{year}_clean.parquet"


def _clean_paths(root: Path, dataset: str, year: int) -> dict[str, str | None]:
    clean_dir = layer_year_dir(root, "clean", dataset, year)
    return {
        "dir": str(clean_dir),
        "output": str(_clean_output_path(root, dataset, year)),
        "metadata": str(clean_dir / METADATA),
        "validation": None,
    }


def _mart_output_paths(root: Path, year_dir: Path, dataset: str, tables: list[Any]) -> list[Path]:
    result: list[Path] = []
    # Le tabelle multi-year (mart.tables[].years) vengono scritte a livello
    # dataset (data/mart/{dataset}/{name}.parquet), NON nel dir per-anno:
    # il path deve rifletterlo, altrimenti readiness/summary segnalano
    # mart_outputs_missing anche quando gli output esistono (issue #445).
    dataset_mart_dir = layer_dataset_dir(root, "mart", dataset)
    for table in tables:
        if isinstance(table, dict):
            name = table.get("name")
            is_multi_year = bool(table.get("years"))
        elif hasattr(table, "name"):
            name = table.name
            is_multi_year = bool(getattr(table, "years", None))
        else:
            continue
        if not name:
            continue
        if is_multi_year:
            result.append(dataset_mart_dir / f"{name}.parquet")
        else:
            result.append(year_dir / f"{name}.parquet")
    return result


def _mart_paths(
    root: Path, dataset: str, year: int, tables: list[dict[str, Any]]
) -> dict[str, Any]:
    mart_dir = layer_year_dir(root, "mart", dataset, year)
    return {
        "dir": str(mart_dir),
        "outputs": [str(path) for path in _mart_output_paths(root, mart_dir, dataset, tables)],
        "metadata": str(mart_dir / METADATA),
        "validation": None,
    }


def payload_for_year(cfg, year: int) -> dict[str, Any]:
    """Costruisce il payload diagnostico per un dataset/year."""
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
            "support": resolve_support_payloads(
                ensure_dict(cfg.support), require_exists=False, root=Path(root)
            ),
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
        "latest_run": latest_payload,
    }
