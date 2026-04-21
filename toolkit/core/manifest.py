from __future__ import annotations

from pathlib import Path
from typing import Any

from toolkit.core.io import read_json, write_json_atomic
from toolkit.core.metadata import _read_metadata, merge_layer_manifest


def write_raw_manifest(folder: Path, payload: dict[str, Any]) -> Path:
    path = folder / "manifest.json"
    write_json_atomic(path, payload)

    outputs = payload.get("outputs")
    summary = payload.get("summary")
    validation = payload.get("validation")
    ok = summary.get("ok") if isinstance(summary, dict) else None
    errors_count = summary.get("errors_count") if isinstance(summary, dict) else None
    warnings_count = summary.get("warnings_count") if isinstance(summary, dict) else None

    merge_layer_manifest(
        folder,
        metadata_path="metadata.json",
        validation_path=validation,
        outputs=outputs,
        ok=ok,
        errors_count=errors_count,
        warnings_count=warnings_count,
    )

    primary_output_file = payload.get("primary_output_file")
    sources = payload.get("sources")
    if primary_output_file or sources:
        meta = _read_metadata(folder) or {}
        changed = False
        if primary_output_file and not meta.get("primary_output_file"):
            meta["primary_output_file"] = primary_output_file
            changed = True
        if sources and not meta.get("sources"):
            meta["sources"] = sources
            changed = True
        if changed:
            write_json_atomic(folder / "metadata.json", meta)

    return path


def read_raw_manifest(folder: Path) -> dict[str, Any] | None:
    path = folder / "manifest.json"
    if not path.exists():
        return None
    return read_json(path)
