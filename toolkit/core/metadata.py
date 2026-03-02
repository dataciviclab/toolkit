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


def write_metadata(folder: Path, data: dict[str, Any], filename: str = "metadata.json") -> Path:
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


def write_layer_manifest(
    folder: Path,
    *,
    metadata_path: str,
    validation_path: str,
    outputs: list[dict[str, Any]],
    ok: bool | None,
    errors_count: int | None,
    warnings_count: int | None,
    filename: str = "manifest.json",
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
