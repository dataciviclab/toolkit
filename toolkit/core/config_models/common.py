"""Shared utilities and models used across all config layers.

Responsibilities are split into dedicated sub-modules:
- path_normalization: path resolution and section normalization
- coercion: parse_bool, ensure_str_list
- policy: unknown-key detection, deprecation notices, legacy normalization
- shared_models: TimeCoverage, DatasetBlock, SupportDatasetConfig, etc.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from toolkit.core.config_models.path_normalization import (
    _err as _err_pn,
    _MANAGED_OUTPUT_ROOTS as _MANAGED_OUTPUT_ROOTS_PN,
    _require_map as _require_map_pn,
    _ensure_root_within_repo as _ensure_root_within_repo_pn,
    _is_managed_output_root as _is_managed_output_root_pn,
    _iter_matching_tokens as _iter_matching_tokens_pn,
    _normalize_section_paths as _normalize_section_paths_pn,
    _path_tokens_to_str as _path_tokens_to_str_pn,
    _resolve_root as _resolve_root_pn,
    _set_nested_value as _set_nested_value_pn,
    _get_nested_value as _get_nested_value_pn,
    _resolve_path_value as _resolve_path_value_pn,
)

# Re-export path_normalization functions for backward compat via __init__.py
# (consumers import from common, not from path_normalization directly)
_err = _err_pn
_MANAGED_OUTPUT_ROOTS = _MANAGED_OUTPUT_ROOTS_PN
_require_map = _require_map_pn
_ensure_root_within_repo = _ensure_root_within_repo_pn
_is_managed_output_root = _is_managed_output_root_pn
_iter_matching_tokens = _iter_matching_tokens_pn
_normalize_section_paths = _normalize_section_paths_pn
_path_tokens_to_str = _path_tokens_to_str_pn
_resolve_root = _resolve_root_pn
_set_nested_value = _set_nested_value_pn
_get_nested_value = _get_nested_value_pn
_resolve_path_value = _resolve_path_value_pn


logger = logging.getLogger("toolkit.core.config")
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
