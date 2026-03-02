from __future__ import annotations

from pathlib import Path
from typing import Any

from toolkit.core.io import read_json, write_json_atomic


def write_raw_manifest(folder: Path, payload: dict[str, Any]) -> Path:
    path = folder / "manifest.json"
    write_json_atomic(path, payload)
    return path


def read_raw_manifest(folder: Path) -> dict[str, Any] | None:
    path = folder / "manifest.json"
    if not path.exists():
        return None
    return read_json(path)
