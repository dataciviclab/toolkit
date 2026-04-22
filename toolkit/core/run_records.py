"""Storage and query layer for run records.

Provides:
- Run record path resolution (get_run_dir, _run_record_path)
- Write with Windows-safe retry (write_run_record)
- Read with portability migration (read_run_record)
- Query (list_runs, latest_run)
"""

from __future__ import annotations

import json
import time
from pathlib import Path

from toolkit.core.run_record_portability import _load_run_record


# --- Path resolution ----------------------------------------------------------


def get_run_dir(root: Path, dataset: str, year: int) -> Path:
    return root / "data" / "_runs" / dataset / str(year)


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


# --- Query ------------------------------------------------------------------


def list_runs(run_dir: Path) -> list[Path]:
    if not run_dir.exists():
        return []
    return sorted(run_dir.glob("*.json"))


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
    latest = max(
        runs,
        key=lambda path: (
            _load_run_record(path).get("started_at", ""),
            path.stat().st_mtime,
        ),
    )
    return _load_run_record(latest)
