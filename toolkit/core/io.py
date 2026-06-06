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


def normalize_encoding(enc: str | None) -> str | None:
    """Normalize encoding name to DuckDB canonical form.

    Mappa alias comuni (``latin1``, ``utf8``, ``win1252``, ecc.)
    alla forma canonica attesa da DuckDB (``latin-1``, ``utf-8``, ``CP1252``).
    """
    if enc is None:
        return None
    e = enc.strip()
    if e.lower() == "latin1":
        return "latin-1"
    if e.lower() == "utf8":
        return "utf-8"
    if e.lower() in {"win1252", "windows1252"}:
        return "CP1252"
    if e.lower() in {"iso-8859-1", "iso8859-1"}:
        return "latin-1"
    if e.lower() == "ascii":
        return "us-ascii"
    return e


def read_json(path: Path) -> dict[str, Any]:
    """Read JSON file.

    Deprecated: prefer :func:`read_json_or_none` che gestisce I/O ed errori
    di parsing senza eccezioni. ``read_json`` non è usata in produzione;
    potrebbe essere rimossa in una versione futura.
    """
    return json.loads(path.read_text(encoding="utf-8"))


def read_json_or_none(path: Path) -> dict[str, Any] | None:
    """Read JSON file, returning None on any parse or I/O error.

    Use for optional config files where absence is a valid state.
    """
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None


# ---------------------------------------------------------------------------
# YAML helpers — centralise yaml dependency here so callers don't import yaml
# ---------------------------------------------------------------------------

def read_yaml(path: Path) -> Any:
    """Read and parse a YAML file.

    Args:
        path: Path to the YAML file.

    Returns:
        Parsed content (dict, list, or scalar).

    Raises:
        ValueError: if the file cannot be parsed as YAML.
        OSError: if the file cannot be read.
    """
    import yaml as _yaml

    try:
        return _yaml.safe_load(path.read_text(encoding="utf-8"))
    except _yaml.YAMLError as exc:
        raise ValueError(f"YAML parse error in {path}: {exc}") from exc


def read_yaml_or_none(path: Path) -> Any:
    """Read and parse a YAML file, returning None on any error.

    Use for optional files where absence or malformation is a valid state.
    """
    import yaml as _yaml

    try:
        return _yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, _yaml.YAMLError, UnicodeDecodeError):
        return None


def write_yaml(data: Any, path: Path) -> None:
    """Serialize data to a YAML file (safe_dump, block style).

    Args:
        data: Data to serialize (typically dict or list).
        path: Destination path. Parent directories are created if needed.
    """
    import yaml as _yaml

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        _yaml.safe_dump(data, default_flow_style=False, sort_keys=False),
        encoding="utf-8",
    )


def yaml_dumps(data: Any) -> str:
    """Serialize data to a YAML string (safe_dump, block style).

    Args:
        data: Data to serialize.

    Returns:
        YAML-formatted string.
    """
    import yaml as _yaml

    return _yaml.safe_dump(data, default_flow_style=False, sort_keys=False)
