"""Shared utilities and models used across all config layers."""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, field_validator, model_validator


logger = logging.getLogger("toolkit.core.config")
_MANAGED_OUTPUT_ROOTS = {"_smoke_out", "_test_out"}
_SAFE_SQL_IDENTIFIER_RE = r"^[A-Za-z_][A-Za-z0-9_]*$"


@dataclass(frozen=True)
class ConfigDeprecation:
    code: str
    legacy: str
    replacement: str
    status: str
    message: str


_CONFIG_DEPRECATIONS: dict[str, ConfigDeprecation] = {
    "unknown.top_level": ConfigDeprecation(
        code="DCL009",
        legacy="unknown top-level keys",
        replacement="remove unsupported keys",
        status="ignored",
        message="unknown top-level config keys detected",
    ),
    "unknown.raw": ConfigDeprecation(
        code="DCL010",
        legacy="raw.* unknown keys",
        replacement="remove unsupported raw keys",
        status="ignored",
        message="unknown raw config keys detected",
    ),
    "unknown.clean": ConfigDeprecation(
        code="DCL011",
        legacy="clean.* unknown keys",
        replacement="remove unsupported clean keys",
        status="ignored",
        message="unknown clean config keys detected",
    ),
    "unknown.mart": ConfigDeprecation(
        code="DCL012",
        legacy="mart.* unknown keys",
        replacement="remove unsupported mart keys",
        status="ignored",
        message="unknown mart config keys detected",
    ),
    "unknown.cross_year": ConfigDeprecation(
        code="DCL013",
        legacy="cross_year.* unknown keys",
        replacement="remove unsupported cross_year keys",
        status="ignored",
        message="unknown cross_year config keys detected",
    ),
}


def parse_bool(value: Any, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in {0, 1}:
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y"}:
            return True
        if normalized in {"false", "0", "no", "n"}:
            return False
    raise ValueError(f"{field_name} must be a boolean-like value: true/false, 1/0, yes/no")


def ensure_str_list(value: Any, field_name: str) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        if not all(isinstance(item, str) for item in value):
            raise ValueError(f"{field_name} must be a string or a list of strings")
        return list(value)
    raise ValueError(f"{field_name} must be a string or a list of strings")


class TimeCoverage(BaseModel):
    """Optional metadata per dichiarare la copertura temporale reale dei dati."""

    model_config = ConfigDict(extra="forbid")

    mode: Literal["full_series"] = "full_series"
    start_year: int
    end_year: int

    @model_validator(mode="after")
    def _validate_year_range(self) -> "TimeCoverage":
        if self.end_year < self.start_year:
            raise ValueError("dataset.time_coverage.end_year must be >= start_year")
        return self


class DatasetBlock(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    years: list[int]
    time_coverage: TimeCoverage | None = None


class SupportDatasetConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    config: Path
    years: list[int]

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        text = value.strip()
        if not text:
            raise ValueError("support[].name must not be empty")
        if not re.fullmatch(_SAFE_SQL_IDENTIFIER_RE, text):
            raise ValueError(
                "support[].name must be a safe identifier "
                "(letters, numbers, underscore; cannot start with a number)"
            )
        return text

    @field_validator("years")
    @classmethod
    def _validate_years(cls, value: list[int]) -> list[int]:
        if not value:
            raise ValueError("support[].years must not be empty")
        return value


class OutputConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    artifacts: Literal["minimal", "standard", "debug"] = "standard"
    legacy_aliases: bool = True


class GlobalValidationConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    fail_on_error: bool = True


class ConfigPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    strict: bool = False


class RangeRuleConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    min: float | None = None
    max: float | None = None


# --- Path resolution utilities ------------------------------------------------


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


def _declared_model_keys(model_cls: type[BaseModel]) -> set[str]:
    keys: set[str] = set()
    for field_name, field_info in model_cls.model_fields.items():
        keys.add(field_name)
        if field_info.alias:
            keys.add(str(field_info.alias))
    return keys


_TOP_LEVEL_ALLOWED_KEYS = {
    "schema_version",
    "root",
    "dataset",
    "raw",
    "clean",
    "mart",
    "support",
    "cross_year",
    "config",
    "validation",
    "output",
}


def _normalize_legacy_payload(
    data: dict[str, Any],
    *,
    path: Path,
    strict_config: bool,
) -> dict[str, Any]:
    normalized = dict(data)

    raw = normalized.get("raw")
    if isinstance(raw, dict):
        normalized["raw"] = dict(raw)

    clean = normalized.get("clean")
    if isinstance(clean, dict):
        normalized["clean"] = dict(clean)

    mart = normalized.get("mart")
    if isinstance(mart, dict):
        normalized["mart"] = dict(mart)

    return normalized


def _emit_deprecation_notice(
    key: str,
    *,
    strict_config: bool,
    path: Path,
) -> None:
    notice = _CONFIG_DEPRECATIONS[key]
    message = f"{notice.code} {notice.message}"
    logger.warning(message)
    if strict_config:
        raise _err(f"{notice.code} {notice.message}", path=path)


def _emit_unknown_keys_notice(
    key: str,
    extras: list[str],
    *,
    strict_config: bool,
    path: Path,
) -> None:
    notice = _CONFIG_DEPRECATIONS[key]
    formatted = ", ".join(sorted(extras))
    message = f"{notice.code} {notice.message}: {formatted}"
    logger.warning(message)
    if strict_config:
        raise _err(message, path=path)


def _warn_or_reject_unknown_keys(
    data: dict[str, Any],
    *,
    path: Path,
    strict_config: bool,
) -> dict[str, Any]:
    normalized = dict(data)

    top_level_extras = [key for key in normalized.keys() if key not in _TOP_LEVEL_ALLOWED_KEYS]
    if "bq" in top_level_extras:
        raise _err("bq is no longer supported; remove field", path=path)
    if top_level_extras:
        _emit_unknown_keys_notice(
            "unknown.top_level",
            top_level_extras,
            strict_config=strict_config,
            path=path,
        )
        if not strict_config:
            normalized = {k: v for k, v in normalized.items() if k in _TOP_LEVEL_ALLOWED_KEYS}

    from toolkit.core.config_models.raw import RawConfig
    from toolkit.core.config_models.clean import CleanConfig
    from toolkit.core.config_models.mart import MartConfig
    from toolkit.core.config_models.cross_year import CrossYearConfig

    for section_name, allowed_keys, notice_key in (
        ("raw", _declared_model_keys(RawConfig), "unknown.raw"),
        ("clean", _declared_model_keys(CleanConfig), "unknown.clean"),
        ("mart", _declared_model_keys(MartConfig), "unknown.mart"),
        ("cross_year", _declared_model_keys(CrossYearConfig), "unknown.cross_year"),
    ):
        section = normalized.get(section_name)
        if not isinstance(section, dict):
            continue
        extras = [k for k in section if k not in allowed_keys]
        # Unconditional rejections for legacy forms that are no longer supported.
        if section_name == "raw" and "source" in extras:
            raise _err("raw.source is no longer supported; use raw.sources", path=path)
        if section_name == "clean" and "sql_path" in extras:
            raise _err("clean.sql_path is no longer supported; use clean.sql", path=path)
        if section_name == "mart" and "sql_dir" in extras:
            raise _err("mart.sql_dir is no longer supported; use mart.tables[].sql", path=path)
        if extras:
            _emit_unknown_keys_notice(
                notice_key,
                extras,
                strict_config=strict_config,
                path=path,
            )

    return normalized
