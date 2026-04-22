"""Run context lifecycle management.

This module is the public facade for run lifecycle. Storage/query and portability
have been migrated to dedicated sub-modules:
- run_records: path resolution, write, read, query
- run_record_portability: absolute-to-relative path migration
"""

from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from toolkit.core.run_records import get_run_dir, write_run_record
from toolkit.core.paths import to_root_relative
from toolkit.version import __version__ as _toolkit_version


# --- Backward-compat re-exports (consumers import from run_context) ---
# ruff: noqa: F401
from toolkit.core.run_records import latest_run, list_runs, read_run_record


# --- Module-level constants ---
_LAYER_NAMES = ("raw", "clean", "mart")


# --- Lifecycle helpers (remain here, tight coupling with RunContext) ---


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
    return {"output_rows": None, "output_bytes": None, "col_count": None, "tables_count": None}


# --- RunContext ---------------------------------------------------------------


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
        self.source_urls: list[str] = []
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
            "toolkit_version": _toolkit_version,
            "run_id": self.run_id,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_seconds": _duration_seconds(self.started_at, self.finished_at),
            "status": self.status,
            "layers": layers_out,
            "validations": self.validations,
            "source_urls": self.source_urls,
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
        col_count: Optional[int] = None,
        tables_count: Optional[int] = None,
        source_urls: Optional[list[str]] = None,
    ) -> None:
        info = self._layer(layer)
        info["metrics"] = {
            "output_rows": output_rows,
            "output_bytes": output_bytes,
            "col_count": col_count,
            "tables_count": tables_count,
        }
        if source_urls:
            self.source_urls = list(dict.fromkeys(self.source_urls + source_urls))
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
