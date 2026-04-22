from __future__ import annotations

from pathlib import Path
from typing import Any

from toolkit.core.io import read_json, write_json_atomic
from toolkit.core.metadata import merge_layer_manifest


def write_raw_manifest(folder: Path, payload: dict[str, Any]) -> Path:
    path = folder / "manifest.json"
    write_json_atomic(path, payload)

    summary = payload.get("summary")
    merge_layer_manifest(
        folder,
        metadata_path="metadata.json",
        validation_path=payload.get("validation"),
        outputs=payload.get("outputs"),
        ok=summary.get("ok") if isinstance(summary, dict) else None,
        errors_count=summary.get("errors_count") if isinstance(summary, dict) else None,
        warnings_count=summary.get("warnings_count") if isinstance(summary, dict) else None,
        primary_output_file=payload.get("primary_output_file"),
        sources=payload.get("sources"),
    )

    return path


def read_raw_manifest(folder: Path) -> dict[str, Any] | None:
    path = folder / "manifest.json"
    if not path.exists():
        return None
    return read_json(path)
