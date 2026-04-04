from __future__ import annotations

import json
import re
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path, PurePath, PurePosixPath, PureWindowsPath
from typing import Any, Dict, Optional

from toolkit.core.paths import to_root_relative

_LAYER_NAMES = ("raw", "clean", "mart")
_WINDOWS_ABS_RE = re.compile(r"^[A-Za-z]:[\\/]")
_RUN_RECORD_RENAME_RETRY_DELAYS_SECONDS = (0.05, 0.1, 0.2)
_PORTABLE_RUN_PATH_FIELDS: set[tuple[str, ...]] = {
    ("layers", "raw", "artifact_path"),
    ("layers", "clean", "artifact_path"),
    ("layers", "mart", "artifact_path"),
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _duration_seconds(started: Optional[str], finished: Optional[str]) -> Optional[float]:
    if started is None or finished is None:
        return None
    try:
        s = datetime.fromisoformat(started)
        f = datetime.fromisoformat(finished)
        return round((f - s).total_seconds(), 3)
    except Exception:
        return None


def _empty_layer_metrics() -> Dict[str, Any]:
    return {"output_rows": None, "output_bytes": None, "tables_count": None}


def get_run_dir(root: Path, dataset: str, year: int) -> Path:
    return root / "data" / "_runs" / dataset / str(year)


def _run_record_path(run_dir: Path, run_id: str) -> Path:
    return run_dir / f"{run_id}.json"


def _root_from_run_dir(run_dir: Path) -> Path:
    return run_dir.parents[3]


def _to_pure_path(path: str) -> PurePath:
    if "\\" in path or _WINDOWS_ABS_RE.match(path):
        return PureWindowsPath(path)
    return PurePosixPath(path)


def _is_absolute_path_string(value: str) -> bool:
    return value.startswith("/") or value.startswith("\\\\") or _WINDOWS_ABS_RE.match(value) is not None


def _migrate_path_value(value: str, root: Path) -> tuple[str, bool]:
    if not _is_absolute_path_string(value):
        return value, False

    try:
        relative = to_root_relative(_to_pure_path(value), _to_pure_path(str(root)))
        return relative, True
    except Exception:
        return value, False


def _migrate_whitelisted_path_fields(payload: dict[str, Any], root: Path, warnings: list[str]) -> dict[str, Any]:
    migrated = json.loads(json.dumps(payload))

    for field_path in _PORTABLE_RUN_PATH_FIELDS:
        current: Any = migrated
        for token in field_path[:-1]:
            if not isinstance(current, dict) or token not in current:
                current = None
                break
            current = current[token]

        if not isinstance(current, dict):
            continue

        leaf = field_path[-1]
        value = current.get(leaf)
        if not isinstance(value, str):
            continue

        normalized, portable = _migrate_path_value(value, root)
        if portable:
            current[leaf] = normalized
        elif _is_absolute_path_string(value):
            warnings.append(value)

    return migrated


def _load_run_record(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    run_dir = path.parent
    root = _root_from_run_dir(run_dir)
    warnings: list[str] = []
    migrated = _migrate_whitelisted_path_fields(payload, root, warnings)
    migrated["_portability"] = {
        "portable": not warnings,
        "warnings": warnings,
    }
    return migrated


def write_run_record(run_dir: Path, run_id: str, payload: dict[str, Any]) -> Path:
    run_dir.mkdir(parents=True, exist_ok=True)
    path = _run_record_path(run_dir, run_id)
    tmp = run_dir / f".{run_id}.json.tmp"
    tmp.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    last_error: PermissionError | None = None
    for attempt in range(len(_RUN_RECORD_RENAME_RETRY_DELAYS_SECONDS) + 1):
        try:
            tmp.replace(path)
            return path
        except PermissionError as exc:
            # On Windows, AV/indexing can transiently hold the tmp/target handle.
            # Retrying keeps run tracking resilient without changing record format.
            last_error = exc
            if attempt >= len(_RUN_RECORD_RENAME_RETRY_DELAYS_SECONDS):
                raise
            time.sleep(_RUN_RECORD_RENAME_RETRY_DELAYS_SECONDS[attempt])

    if last_error is not None:
        raise last_error
    return path


def list_runs(run_dir: Path) -> list[Path]:
    if not run_dir.exists():
        return []
    return sorted(run_dir.glob("*.json"))


def read_run_record(run_dir: Path, run_id: str) -> dict[str, Any]:
    path = _run_record_path(run_dir, run_id)
    if not path.exists():
        raise FileNotFoundError(f"Run record not found: {path}")
    return _load_run_record(path)


def latest_run(run_dir: Path) -> dict[str, Any]:
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


class RunContext:
    def __init__(
        self,
        dataset: str,
        year: int,
        *,
        root: Path | str,
        resumed_from: str | None = None,
    ) -> None:
        self.dataset = dataset
        self.year = year
        self.root = Path(root)
        self.resumed_from = resumed_from
        self.run_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
        self.started_at = _now_iso()
        self.finished_at: str | None = None
        self.status = "RUNNING"
        self.layers = {
            layer: {"status": "PENDING", "started_at": None, "finished_at": None, "metrics": _empty_layer_metrics()}
            for layer in _LAYER_NAMES
        }
        self.validations = {layer: {} for layer in _LAYER_NAMES}
        self.error: str | None = None
        self._runs_dir = get_run_dir(self.root, self.dataset, self.year)
        self._path = self._runs_dir / f"{self.run_id}.json"
        self.save()

    @property
    def path(self) -> Path:
        return self._path

    def to_dict(self) -> Dict[str, Any]:
        layers_out = {}
        for layer, info in self.layers.items():
            layers_out[layer] = {
                **info,
                "duration_seconds": _duration_seconds(info.get("started_at"), info.get("finished_at")),
            }
        return {
            "dataset": self.dataset,
            "year": self.year,
            "run_id": self.run_id,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_seconds": _duration_seconds(self.started_at, self.finished_at),
            "status": self.status,
            "layers": layers_out,
            "validations": self.validations,
            "resumed_from": self.resumed_from,
            "error": self.error,
        }

    def _layer(self, layer: str) -> Dict[str, Any]:
        if layer not in self.layers:
            raise ValueError(f"Unknown layer: {layer}")
        return self.layers[layer]

    def start_layer(self, layer: str) -> None:
        info = self._layer(layer)
        now = _now_iso()
        if info["started_at"] is None:
            info["started_at"] = now
        info["status"] = "PENDING"
        self.save()

    def complete_layer(self, layer: str) -> None:
        info = self._layer(layer)
        now = _now_iso()
        if info["started_at"] is None:
            info["started_at"] = now
        info["finished_at"] = now
        info["status"] = "SUCCESS"
        self.save()

    def fail_layer(self, layer: str, error_msg: str) -> None:
        info = self._layer(layer)
        now = _now_iso()
        if info["started_at"] is None:
            info["started_at"] = now
        info["finished_at"] = now
        info["status"] = "FAILED"
        self.error = error_msg
        self.save()

    def set_validation(self, layer: str, summary: Dict[str, Any]) -> None:
        if layer not in self.validations:
            raise ValueError(f"Unknown validation layer: {layer}")
        self.validations[layer] = summary
        self.save()

    def set_layer_metrics(
        self,
        layer: str,
        *,
        output_rows: Optional[int] = None,
        output_bytes: Optional[int] = None,
        tables_count: Optional[int] = None,
    ) -> None:
        info = self._layer(layer)
        info["metrics"] = {
            "output_rows": output_rows,
            "output_bytes": output_bytes,
            "tables_count": tables_count,
        }
        self.save()

    def complete_run(self, *, success_with_warnings: bool = False) -> None:
        self.finished_at = _now_iso()
        if self.status != "FAILED":
            self.status = "SUCCESS_WITH_WARNINGS" if success_with_warnings else "SUCCESS"
        self.save()

    def fail_run(self, error_msg: str) -> None:
        self.finished_at = _now_iso()
        self.status = "FAILED"
        self.error = error_msg
        self.save()

    def mark_dry_run(self) -> None:
        self.finished_at = _now_iso()
        self.status = "DRY_RUN"
        self.save()

    def save(self) -> None:
        self._path = write_run_record(self._runs_dir, self.run_id, self.to_dict())
