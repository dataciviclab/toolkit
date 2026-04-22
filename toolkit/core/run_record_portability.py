"""Portability layer for run records.

Handles:
- Conversion between absolute and portable (relative) paths in run records
- Path normalization for cross-platform compatibility
- Migration of legacy absolute paths to relative paths
- Loading run records with portability metadata attached
"""

from __future__ import annotations

import json
import re
from pathlib import Path, PurePath, PurePosixPath, PureWindowsPath
from typing import Any

from toolkit.core.paths import to_root_relative


_WINDOWS_ABS_RE = re.compile(r"^[A-Za-z]:[\\/]")

_PORTABLE_RUN_PATH_FIELDS: set[tuple[str, ...]] = {
    ("layers", "raw", "artifact_path"),
    ("layers", "clean", "artifact_path"),
    ("layers", "mart", "artifact_path"),
}


def _root_from_run_dir(run_dir: Path) -> Path:
    return run_dir.parents[3]


def _to_pure_path(path: str) -> PurePath:
    if "\\" in path or _WINDOWS_ABS_RE.match(path):
        return PureWindowsPath(path)
    return PurePosixPath(path)


def _is_absolute_path_string(value: str) -> bool:
    return value.startswith("/") or value.startswith("\\\\") or _WINDOWS_ABS_RE.match(value) is not None


def _migrate_path_value(value: str, root: Path) -> tuple[str, bool]:
    if not _is_absolute_path_string(value):
        return value, False

    try:
        relative = to_root_relative(_to_pure_path(value), _to_pure_path(str(root)))
        return relative, True
    except Exception:
        return value, False


def _migrate_whitelisted_path_fields(
    payload: dict[str, Any],
    root: Path,
    warnings: list[str],
) -> dict[str, Any]:
    migrated = json.loads(json.dumps(payload))

    for field_path in _PORTABLE_RUN_PATH_FIELDS:
        current: Any = migrated
        for token in field_path[:-1]:
            if not isinstance(current, dict) or token not in current:
                current = None
                break
            current = current[token]

        if not isinstance(current, dict):
            continue

        leaf = field_path[-1]
        value = current.get(leaf)
        if not isinstance(value, str):
            continue

        normalized, portable = _migrate_path_value(value, root)
        if portable:
            current[leaf] = normalized
        elif _is_absolute_path_string(value):
            warnings.append(value)

    return migrated


def _load_run_record(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    run_dir = path.parent
    root = _root_from_run_dir(run_dir)
    warnings: list[str] = []
    migrated = _migrate_whitelisted_path_fields(payload, root, warnings)
    migrated["_portability"] = {
        "portable": not warnings,
        "warnings": warnings,
    }
    return migrated
