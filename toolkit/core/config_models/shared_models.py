"""Shared data models and coercion utilities for dataset.yml config.

Contains:
- Shared Pydantic models (TimeCoverage, DatasetBlock, etc.)
- ConfigDeprecation dataclass and deprecation registry
- Coercion helpers (parse_bool, ensure_str_list)
- Constants used by policy and validation (_SAFE_SQL_IDENTIFIER_RE)
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, field_validator, model_validator


_SAFE_SQL_IDENTIFIER_RE = r"^[A-Za-z_][A-Za-z0-9_]*$"


# --- Deprecation registry ------------------------------------------------------


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


# --- Coercion helpers ----------------------------------------------------------


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


# --- Shared Pydantic models ---------------------------------------------------


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
