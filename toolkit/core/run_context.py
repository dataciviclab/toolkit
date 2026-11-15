"""Run context lifecycle management.

This module is the public facade for run lifecycle. Storage/query and portability
have been migrated to dedicated sub-modules:
- run_records: path resolution, write, read, query
- run_record_portability: absolute-to-relative path migration
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from toolkit.core.run_records import get_run_dir, write_run_record
from toolkit.version import __version__ as _toolkit_version


# --- Module-level constants ---
_LAYER_NAMES = ("raw", "clean", "mart")


# --- Lifecycle helpers (remain here, tight coupling with RunContext) ---


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _duration_seconds(started: str | None, finished: str | None) -> float | None:
    if started is None or finished is None:
        return None
    try:
        s = datetime.fromisoformat(started)
        f = datetime.fromisoformat(finished)
        return round((f - s).total_seconds(), 3)
    except Exception:
        return None


def _empty_layer_metrics() -> dict[str, Any]:
    return {"output_rows": None, "output_bytes": None, "col_count": None, "tables_count": None}


_ORPHAN_RUN_AGE_SECONDS = 3600


def _close_orphan_runs(runs_dir: Path, logger=None) -> None:
    """Chiude i run record rimasti RUNNING (processo ucciso, macchina spenta).

    Senza questo un run interrotto resta RUNNING per sempre e falsa lo stato
    del dataset. Si toccano solo i record piu' vecchi di un'ora, per non
    interferire con un run concorrente legittimo.
    """
    if not runs_dir.is_dir():
        return
    now = datetime.now(timezone.utc)
    for path in runs_dir.glob("*.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if payload.get("status") != "RUNNING":
            continue
        started_raw = payload.get("started_at")
        try:
            started = datetime.fromisoformat(str(started_raw))
        except (TypeError, ValueError):
            started = None
        if started is not None and (now - started).total_seconds() < _ORPHAN_RUN_AGE_SECONDS:
            continue
        payload["status"] = "ABORTED"
        payload["error"] = payload.get("error") or "run interrotto: processo terminato"
        payload["finished_at"] = payload.get("finished_at") or now.isoformat()
        try:
            path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        except OSError:
            continue
        if logger is not None:
            logger.warning("Run record orfano chiuso come ABORTED: %s", path.name)


# --- RunContext ---------------------------------------------------------------


class RunContext:
    def __init__(
        self,
        dataset: str,
        year: int,
        *,
        root: Path | str,
        smoke: bool = False,
    ) -> None:
        self.dataset = dataset
        self.year = year
        self.root = Path(root)
        self.smoke = smoke
        self.run_id = (
            f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
        )
        self.started_at = _now_iso()
        self.finished_at: str | None = None
        self.status = "RUNNING"
        self.layers = {
            layer: {
                "status": "PENDING",
                "started_at": None,
                "finished_at": None,
                "metrics": _empty_layer_metrics(),
            }
            for layer in _LAYER_NAMES
        }
        self.validations: dict[str, dict[str, Any]] = {layer: {} for layer in _LAYER_NAMES}
        self.source_urls: list[str] = []
        self.error: str | None = None
        self._runs_dir = get_run_dir(self.root, self.dataset, self.year)
        self._path = self._runs_dir / f"{self.run_id}.json"
        _close_orphan_runs(self._runs_dir)
        self.save()

    @property
    def path(self) -> Path:
        return self._path

    def to_dict(self) -> dict[str, Any]:
        layers_out = {}
        for layer, info in self.layers.items():
            layers_out[layer] = {
                **info,
                "duration_seconds": _duration_seconds(
                    info.get("started_at"),  # type: ignore[arg-type]
                    info.get("finished_at"),  # type: ignore[arg-type]
                ),
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
            "smoke": self.smoke,
            "error": self.error,
        }

    def _layer(self, layer: str) -> dict[str, Any]:
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

    def set_validation(self, layer: str, summary: dict[str, Any]) -> None:
        if layer not in self.validations:
            raise ValueError(f"Unknown validation layer: {layer}")
        self.validations[layer] = summary
        self.save()

    def set_layer_metrics(
        self,
        layer: str,
        *,
        output_rows: int | None = None,
        output_bytes: int | None = None,
        col_count: int | None = None,
        tables_count: int | None = None,
        source_urls: list[str] | None = None,
        skipped_sources: list[dict[str, Any]] | None = None,
    ) -> None:
        info = self._layer(layer)
        info["metrics"] = {
            "output_rows": output_rows,
            "output_bytes": output_bytes,
            "col_count": col_count,
            "tables_count": tables_count,
        }
        # Sorgenti `optional` saltate: senza questo, un run che ha perso una
        # fonte sarebbe indistinguibile da uno che ha caricato tutto.
        if skipped_sources:
            info["metrics"]["skipped_sources"] = list(skipped_sources)
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
