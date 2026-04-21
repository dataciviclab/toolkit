from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
import hashlib
from pathlib import Path
from typing import Any

from toolkit.core.io import write_json_atomic
from toolkit.version import __version__


def _jsonable(x: Any) -> Any:
    if is_dataclass(x):
        return asdict(x)
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
    filename: str = "metadata.json",
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


def _read_metadata(folder: Path, filename: str = "metadata.json") -> dict[str, Any] | None:
    path = folder / filename
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None


def read_layer_metadata(layer_dir: Path) -> dict[str, Any]:
    """
    Read layer metadata as the canonical source, with fallback to manifest.json
    for fields that may not yet be migrated.

    Migration path:
    - metadata.json is the source of truth for: outputs, validation, summary,
      primary_output_file (raw), sources (raw), and all standard metadata fields
    - manifest.json is kept as a compat alias on write and as a fallback for
      read when metadata.json does not yet contain the field
    """
    meta = _read_metadata(layer_dir) or {}

    manifest = None
    if not meta.get("primary_output_file"):
        manifest = _read_json(layer_dir / "manifest.json") if manifest is None else manifest
        pof = manifest.get("primary_output_file") if manifest else None
        if isinstance(pof, str):
            meta["primary_output_file"] = pof

    if not meta.get("outputs"):
        manifest = _read_json(layer_dir / "manifest.json") if manifest is None else manifest
        outputs = manifest.get("outputs") if manifest else None
        if isinstance(outputs, list):
            meta["outputs"] = outputs

    if not meta.get("summary"):
        manifest = _read_json(layer_dir / "manifest.json") if manifest is None else manifest
        summary = manifest.get("summary") if manifest else None
        if isinstance(summary, dict):
            meta["summary"] = summary

    if not meta.get("validation"):
        manifest = _read_json(layer_dir / "manifest.json") if manifest is None else manifest
        validation = manifest.get("validation") if manifest else None
        if isinstance(validation, str):
            meta["validation"] = validation

    return meta


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None


def merge_layer_manifest(
    folder: Path,
    *,
    metadata_path: str = "metadata.json",
    validation_path: str | None = None,
    outputs: list[dict[str, Any]] | None = None,
    ok: bool | None = None,
    errors_count: int | None = None,
    warnings_count: int | None = None,
) -> Path:
    meta = _read_metadata(folder, metadata_path) or {}
    if outputs is not None:
        meta["outputs"] = outputs
    if validation_path is not None:
        meta["validation"] = validation_path
    if ok is not None or errors_count is not None or warnings_count is not None:
        meta["summary"] = {
            "ok": ok,
            "errors_count": errors_count,
            "warnings_count": warnings_count,
        }
    out = folder / metadata_path
    write_json_atomic(out, meta)
    return out


def write_layer_manifest(
    folder: Path,
    *,
    metadata_path: str = "metadata.json",
    validation_path: str | None = None,
    outputs: list[dict[str, Any]] | None = None,
    ok: bool | None = None,
    errors_count: int | None = None,
    warnings_count: int | None = None,
    filename: str = "manifest.json",
) -> Path:
    merge_layer_manifest(
        folder,
        metadata_path=metadata_path,
        validation_path=validation_path,
        outputs=outputs,
        ok=ok,
        errors_count=errors_count,
        warnings_count=warnings_count,
    )
    return write_manifest_alias(folder, filename, metadata_path, validation_path, outputs, ok, errors_count, warnings_count)


def write_manifest_alias(
    folder: Path,
    filename: str,
    metadata_path: str,
    validation_path: str | None,
    outputs: list[dict[str, Any]] | None,
    ok: bool | None,
    errors_count: int | None,
    warnings_count: int | None,
) -> Path:
    payload = {
        "metadata": metadata_path,
        "validation": validation_path,
        "summary": {
            "ok": ok,
            "errors_count": errors_count,
            "warnings_count": warnings_count,
        },
        "outputs": outputs,
    }
    out = folder / filename
    write_json_atomic(out, payload)
    return out
