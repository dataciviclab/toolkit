"""Pydantic models for the clean layer configuration."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from toolkit.core.config_models.common import (
    RangeRuleConfig,
    ensure_str_list,
    normalize_columns_spec,
    parse_bool,
)


class CleanReadConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: Literal["auto", "config_only"] = "auto"
    delim: str | None = None
    header: bool = True
    encoding: str | None = None
    decimal: str | None = None
    skip: int | None = None
    auto_detect: bool | None = None
    quote: str | None = None
    escape: str | None = None
    comment: str | None = None
    ignore_errors: bool | None = None
    strict_mode: bool | None = None
    null_padding: bool | None = None
    parallel: bool | None = None
    nullstr: str | list[str] | None = None
    columns: dict[str, str] | None = None
    normalize_rows_to_columns: bool = False
    trim_whitespace: bool = True
    sample_size: int | None = None
    sheet_name: str | int | None = None
    mode: Literal["explicit", "latest", "largest", "all"] | None = None
    glob: str = "*"
    prefer_from_raw_run: bool = True
    allow_ambiguous: bool = False
    include: list[str] | None = None

    @field_validator("columns", mode="before")
    @classmethod
    def _normalize_columns(cls, value: Any) -> dict[str, str] | None:
        return normalize_columns_spec(value)

    @field_validator("normalize_rows_to_columns", mode="before")
    @classmethod
    def _normalize_rows_to_columns(cls, value: Any) -> bool:
        return parse_bool(value, "clean.read.normalize_rows_to_columns")

    @field_validator("include", mode="before")
    @classmethod
    def _normalize_include(cls, value: Any) -> list[str] | None:
        if value is None:
            return None
        return ensure_str_list(value, "clean.read.include")


class CleanValidateConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    primary_key: list[str] = Field(default_factory=list)
    not_null: list[str] = Field(default_factory=list)
    ranges: dict[str, RangeRuleConfig] = Field(default_factory=dict)
    max_null_pct: dict[str, float] = Field(default_factory=dict)
    min_rows: int | None = None

    @field_validator("primary_key", "not_null", mode="before")
    @classmethod
    def _normalize_lists(cls, value: Any, info) -> list[str]:
        return ensure_str_list(value, f"clean.validate.{info.field_name}")


class CleanConfig(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    sql: Path | None = None
    read_mode: Literal["strict", "fallback", "robust"] = "fallback"
    read_source: Literal["auto", "config_only"] | None = None
    read: CleanReadConfig | None = None
    required_columns: list[str] = Field(default_factory=list)
    validate_config: CleanValidateConfig = Field(
        default_factory=CleanValidateConfig,
        alias="validate",
    )

    @field_validator("required_columns", mode="before")
    @classmethod
    def _normalize_required_columns(cls, value: Any) -> list[str]:
        return ensure_str_list(value, "clean.required_columns")

    @property
    def validate(self) -> CleanValidateConfig:
        return self.validate_config


class CleanValidationSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    required_columns: list[str] = Field(default_factory=list)
    validate_config: CleanValidateConfig = Field(
        default_factory=CleanValidateConfig,
        alias="validate",
    )

    @field_validator("required_columns", mode="before")
    @classmethod
    def _normalize_required_columns(cls, value: Any) -> list[str]:
        return ensure_str_list(value, "clean.required_columns")

    @property
    def validate(self) -> CleanValidateConfig:
        return self.validate_config
