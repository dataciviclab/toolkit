"""Path resolution utilities for dataset.yml config normalization.

Handles {year}-templated paths, relative-to-base_dir resolution,
section-specific whitelist-based normalization, and managed output root
detection.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any


_MANAGED_OUTPUT_ROOTS = {"_smoke_out", "_test_out"}


def _err(msg: str, *, path: Path) -> ValueError:
    return ValueError(f"{msg} (file: {path})")


def _require_map(data: dict[str, Any], key: str, *, path: Path) -> dict[str, Any]:
    val = data.get(key)
    if not isinstance(val, dict):
        raise _err(f"Campo '{key}' mancante o non valido (deve essere una mappa).", path=path)
    return val


def _resolve_path_value(value: Any, *, base_dir: Path) -> Any:
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not text:
        return value
    if "{year}" in text:
        sentinel = "__DCL_YEAR_PLACEHOLDER__"
        templated = text.replace("{year}", sentinel)
        path = Path(templated).expanduser()
        if path.is_absolute():
            return str(path.resolve()).replace(sentinel, "{year}")
        return str((base_dir / path).resolve()).replace(sentinel, "{year}")
    path = Path(text).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (base_dir / path).resolve()


_SECTION_PATH_WHITELIST: dict[str, tuple[tuple[str, ...], ...]] = {
    "raw": (
        ("source", "args", "path"),
        ("sources", "*", "args", "path"),
    ),
    "clean": (("sql",),),
    "mart": (("tables", "*", "sql"),),
    "support": (("*", "config"),),
    "cross_year": (("tables", "*", "sql"),),
}


def _path_tokens_to_str(tokens: tuple[str, ...]) -> str:
    out: list[str] = []
    for token in tokens:
        if token == "*":
            if out:
                out[-1] = f"{out[-1]}[*]"
            else:
                out.append("[*]")
        elif token.isdigit():
            if out:
                out[-1] = f"{out[-1]}[{token}]"
            else:
                out.append(f"[{token}]")
        else:
            out.append(token)
    return ".".join(out)


def _set_nested_value(container: Any, tokens: tuple[str, ...], value: Any) -> Any:
    if not tokens:
        return value

    head, *tail = tokens
    if isinstance(container, dict):
        updated = dict(container)
        updated[head] = _set_nested_value(updated.get(head), tuple(tail), value)
        return updated

    if isinstance(container, list):
        index = int(head)
        updated = list(container)
        updated[index] = _set_nested_value(updated[index], tuple(tail), value)
        return updated

    raise TypeError(f"Cannot set nested value at {tokens!r} on {type(container).__name__}")


def _iter_matching_tokens(
    container: Any,
    pattern: tuple[str, ...],
    prefix: tuple[str, ...] = (),
) -> list[tuple[str, ...]]:
    if not pattern:
        return [prefix]

    head, *tail = pattern
    tail_tuple = tuple(tail)

    if head == "*":
        if not isinstance(container, list):
            return []
        matches: list[tuple[str, ...]] = []
        for index, item in enumerate(container):
            matches.extend(_iter_matching_tokens(item, tail_tuple, prefix + (str(index),)))
        return matches

    if not isinstance(container, dict) or head not in container:
        return []

    return _iter_matching_tokens(container[head], tail_tuple, prefix + (head,))


def _get_nested_value(container: Any, tokens: tuple[str, ...]) -> Any:
    current = container
    for token in tokens:
        if isinstance(current, dict):
            current = current[token]
        elif isinstance(current, list):
            current = current[int(token)]
        else:
            raise TypeError(f"Cannot traverse token {token!r} on {type(current).__name__}")
    return current


def _normalize_section_paths(
    section_name: str,
    section: Any,
    *,
    base_dir: Path,
) -> tuple[Any, list[tuple[str, Path]]]:
    if isinstance(section, dict):
        normalized: Any = dict(section)
    elif isinstance(section, list):
        normalized = list(section)
    else:
        normalized = section
    changes: list[tuple[str, Path]] = []

    for pattern in _SECTION_PATH_WHITELIST.get(section_name, ()):
        for tokens in _iter_matching_tokens(section, pattern):
            raw_value = _get_nested_value(section, tokens)
            resolved = _resolve_path_value(raw_value, base_dir=base_dir)
            if resolved is raw_value:
                continue
            normalized = _set_nested_value(normalized, tokens, resolved)
            changes.append((f"{section_name}.{_path_tokens_to_str(tokens)}", resolved))

    return normalized, changes


def _is_managed_output_root(root: str) -> bool:
    raw = root.strip()
    if not raw:
        return False
    path = Path(raw)
    if path.is_absolute():
        return False
    return path.name in _MANAGED_OUTPUT_ROOTS


def _resolve_root(root: Any, *, base_dir: Path) -> tuple[Path, str]:
    if root is None:
        env_root = os.environ.get("DCL_ROOT")
        if env_root:
            return Path(env_root).expanduser().resolve(), "env:DCL_ROOT"
        return base_dir, "base_dir_fallback"

    if not isinstance(root, str):
        raise ValueError("root must be a string path or null")

    if not root.strip():
        env_root = os.environ.get("DCL_ROOT")
        if env_root:
            return Path(env_root).expanduser().resolve(), "env:DCL_ROOT"
        return base_dir, "base_dir_fallback"

    managed_outdir = os.environ.get("TOOLKIT_OUTDIR") or os.environ.get("DCL_OUTDIR")
    if managed_outdir and _is_managed_output_root(root):
        source = "env:TOOLKIT_OUTDIR" if os.environ.get("TOOLKIT_OUTDIR") else "env:DCL_OUTDIR"
        return Path(managed_outdir).expanduser().resolve(), source
    return _resolve_path_value(root, base_dir=base_dir), "yml"


def _ensure_root_within_repo(root: Path, *, repo_root: Path, path: Path) -> Path:
    try:
        root.relative_to(repo_root)
    except ValueError as exc:
        raise _err(
            f"root resolves outside repo_root: root={root} repo_root={repo_root}",
            path=path,
        ) from exc
    return root
