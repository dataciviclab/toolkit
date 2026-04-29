from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any


def _json_safe_default(o: Any) -> Any:
    """Handle non-standard types that json.dumps cannot serialize.

    - pandas.NaT     → "NaT" string
    - other unknown types → raise TypeError
    """
    # pandas.NaTType is a singleton; check via type name to avoid import cycle
    if type(o).__name__ == "NaTType":
        return "NaT"
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")


def _preprocess_for_json(obj: Any) -> Any:
    """Recursively replace non-JSON-compliant values with JSON-safe representations.

    json.dumps raises ValueError for nan/inf BEFORE calling the default handler,
    so we must pre-process the data structure before serialization.

    Handles:
    - float nan/inf   → "nan" / "inf" / "-inf" strings
    - pandas.NaT      → "NaT" string
    - pandas.Timestamp → ISO 8601 string
    """
    if isinstance(obj, dict):
        return {k: _preprocess_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_preprocess_for_json(item) for item in obj]
    elif isinstance(obj, float):
        if math.isnan(obj):
            return "nan"
        if math.isinf(obj):
            return "inf" if obj > 0 else "-inf"
        return obj
    else:
        # pandas.NaTType / pandas.Timestamp — check by type name to avoid import cycle
        type_name = type(obj).__name__
        if type_name == "NaTType":
            return "NaT"
        if type_name == "Timestamp":
            return obj.isoformat()
        return obj


def write_json_atomic(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    preprocessed = _preprocess_for_json(data)
    tmp.write_text(
        json.dumps(
            preprocessed,
            ensure_ascii=False,
            indent=2,
            allow_nan=False,
            default=_json_safe_default,
        ),
        encoding="utf-8",
    )
    tmp.replace(path)


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))
