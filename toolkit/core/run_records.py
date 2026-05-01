"""Storage and query layer for run records.

Provides:
- Run record path resolution (get_run_dir, get_run_dir_dataset, _run_record_path)
- Write with Windows-safe retry (write_run_record)
- Read with portability migration (read_run_record)
- Query with filters (list_runs, latest_run)
"""

from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from toolkit.core.run_record_portability import _load_run_record


# --- Path resolution ----------------------------------------------------------


def get_run_dir(root: Path, dataset: str, year: int) -> Path:
    return root / "data" / "_runs" / dataset / str(year)


def get_run_dir_dataset(root: Path, dataset: str) -> Path:
    """Return the dataset-level run directory (above year). Used for cross-year queries."""
    return root / "data" / "_runs" / dataset


def _run_record_path(run_dir: Path, run_id: str) -> Path:
    return run_dir / f"{run_id}.json"


# --- Write -------------------------------------------------------------------


def write_run_record(run_dir: Path, run_id: str, payload: dict) -> Path:
    run_dir.mkdir(parents=True, exist_ok=True)
    path = _run_record_path(run_dir, run_id)
    tmp = run_dir / f".{run_id}.json.tmp"
    tmp.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    last_error: PermissionError | None = None
    _RUN_RECORD_RENAME_RETRY_DELAYS_SECONDS = (0.05, 0.1, 0.2)
    for attempt in range(len(_RUN_RECORD_RENAME_RETRY_DELAYS_SECONDS) + 1):
        try:
            tmp.replace(path)
            return path
        except PermissionError as exc:
            # On Windows, AV/indexing can transiently hold the tmp/target handle.
            last_error = exc
            if attempt >= len(_RUN_RECORD_RENAME_RETRY_DELAYS_SECONDS):
                raise
            time.sleep(_RUN_RECORD_RENAME_RETRY_DELAYS_SECONDS[attempt])

    if last_error is not None:
        raise last_error
    return path


# --- Query helpers -----------------------------------------------------------


def _is_cross_year_run_dir(run_dir: Path) -> bool:
    """Detect if run_dir is dataset-level (cross-year) vs year-level.

    True when run_dir is data/_runs/{dataset} (no year subdir).
    False when run_dir is data/_runs/{dataset}/{year}.
    """
    parts = run_dir.parts
    try:
        runs_idx = parts.index("_runs")
    except ValueError:
        return False
    # dataset-level: data/_runs/{dataset} with no year subdir after
    if len(parts) <= runs_idx + 2:
        return True
    after_dataset = parts[runs_idx + 2]
    return not after_dataset.isdigit()


def list_runs(
    run_dir: Path,
    *,
    since: datetime | None = None,
    until: datetime | None = None,
    status: Literal["SUCCESS", "FAILED", "RUNNING", "DRY_RUN"] | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """List run records from a run directory, with optional filters.

    Args:
        run_dir: directory containing run JSON files (data/_runs/{dataset}/{year})
        since: only runs started at or after this datetime (UTC)
        until: only runs started at or before this datetime (UTC)
        status: filter by run status
        limit: maximum number of runs to return (most recent first)
    """
    if not run_dir.exists():
        return []

    all_records: list[dict[str, Any]] = []
    # cross_year=True: run_dir is dataset-level (data/_runs/{dataset}),
    # need rglob to find JSON in year subdirectories
    pattern = "**/*.json" if _is_cross_year_run_dir(run_dir) else "*.json"
    for path in sorted(run_dir.glob(pattern)):
        try:
            record = _load_run_record(path)
        except Exception:
            continue
        started = record.get("started_at", "")
        if started:
            try:
                started_dt = datetime.fromisoformat(started.replace("Z", "+00:00"))
                if started_dt.tzinfo is None:
                    started_dt = started_dt.replace(tzinfo=timezone.utc)
            except ValueError:
                started_dt = None
        else:
            started_dt = None

        if since and started_dt and started_dt < since:
            continue
        if until and started_dt and started_dt > until:
            continue
        if status and record.get("status") != status:
            continue

        all_records.append(record)

    # Sort descending by started_at
    all_records.sort(key=lambda r: r.get("started_at", ""), reverse=True)

    if limit is not None:
        all_records = all_records[:limit]

    return all_records


def read_run_record(run_dir: Path, run_id: str) -> dict:
    path = _run_record_path(run_dir, run_id)
    if not path.exists():
        raise FileNotFoundError(f"Run record not found: {path}")
    return _load_run_record(path)


def latest_run(run_dir: Path) -> dict:
    runs = list_runs(run_dir)
    if not runs:
        dataset = run_dir.parent.name if run_dir.parent != run_dir else "(unknown)"
        year = run_dir.name
        raise FileNotFoundError(f"No run records found for dataset={dataset} year={year}")
    return runs[0]
