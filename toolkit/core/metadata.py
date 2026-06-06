from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from typing import cast
from datetime import datetime, timezone
import hashlib
from pathlib import Path
from typing import Any

from toolkit.core.io import read_json_or_none, write_json_atomic
from toolkit.core.paths import METADATA
from toolkit.version import __version__


def _jsonable(x: Any) -> Any:
    if is_dataclass(x):
        return asdict(cast(Any, x))
    if isinstance(x, Path):
        return str(x)
    return x


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def file_record(path: Path, *, origin: str | None = None) -> dict[str, Any]:
    payload = path.read_bytes()
    record: dict[str, Any] = {
        "file": path.name,
        "sha256": sha256_bytes(payload),
        "bytes": len(payload),
    }
    if origin is not None:
        record["origin"] = origin
    return record


def config_hash_for_year(base_dir: Path | None, year: int) -> str | None:
    if base_dir is None:
        return None

    path = Path(base_dir).resolve() / "dataset.yml"
    if path.exists():
        rendered = path.read_text(encoding="utf-8").replace("{year}", str(year))
        return sha256_bytes(rendered.encode("utf-8"))
    return None


def write_metadata(
    folder: Path,
    data: dict[str, Any],
    filename: str = METADATA,
) -> Path:
    meta = {
        "metadata_schema_version": 1,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "toolkit_version": __version__,
        **data,
    }

    out = folder / filename
    payload = json.loads(json.dumps(meta, ensure_ascii=False, default=_jsonable))
    write_json_atomic(out, payload)
    return out


def _read_metadata(folder: Path, filename: str = METADATA) -> dict[str, Any] | None:
    path = folder / filename
    if not path.exists():
        return None
    return read_json_or_none(path)


def read_layer_metadata(layer_dir: Path) -> dict[str, Any]:
    """Read ``metadata.json`` from a layer directory.

    ``metadata.json`` is the single source of truth — it contains runtime
    metadata from the pipeline run and validation fields written by
    ``merge_layer_manifest()``.
    """
    return _read_metadata(layer_dir) or {}


# ---------------------------------------------------------------------------
# merge_layer_manifest — merge validation fields into metadata.json
# ---------------------------------------------------------------------------


def merge_layer_manifest(
    folder: Path,
    *,
    metadata_path: str = METADATA,
    validation_path: str | None = None,
    outputs: list[dict[str, Any]] | None = None,
    ok: bool | None = None,
    errors_count: int | None = None,
    warnings_count: int | None = None,
    primary_output_file: str | None = None,
    sources: list[Any] | None = None,
) -> Path:
    meta = _read_metadata(folder, metadata_path) or {}
    if outputs is not None:
        meta["outputs"] = outputs
    if validation_path is not None:
        meta["validation"] = validation_path
    if ok is not None or errors_count is not None or warnings_count is not None:
        summary: dict[str, Any] = {}
        if ok is not None:
            summary["ok"] = ok
        if errors_count is not None:
            summary["errors_count"] = errors_count
        if warnings_count is not None:
            summary["warnings_count"] = warnings_count
        meta["summary"] = summary
    if primary_output_file and not meta.get("primary_output_file"):
        meta["primary_output_file"] = primary_output_file
    if sources and not meta.get("sources"):
        meta["sources"] = sources
    out = folder / metadata_path
    write_json_atomic(out, meta)
    return out
