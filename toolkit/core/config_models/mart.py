"""Pydantic models for the mart layer configuration."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from toolkit.core.config_models.common import (
    RangeRuleConfig,
    _SAFE_SQL_IDENTIFIER_RE,
    ensure_str_list,
    parse_bool,
)


class MartTableConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    sql: Path

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        text = value.strip()
        if not text:
            raise ValueError("mart.tables[].name must not be empty")
        if not re.fullmatch(_SAFE_SQL_IDENTIFIER_RE, text):
            raise ValueError(
                "mart.tables[].name must be a safe SQL identifier "
                "(letters, numbers, underscore; cannot start with a number)"
            )
        return text


class MartTableRuleConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    required_columns: list[str] = Field(default_factory=list)
    not_null: list[str] = Field(default_factory=list)
    primary_key: list[str] = Field(default_factory=list)
    ranges: dict[str, RangeRuleConfig] = Field(default_factory=dict)
    min_rows: int | None = None

    @field_validator("required_columns", "not_null", "primary_key", mode="before")
    @classmethod
    def _normalize_lists(cls, value: Any, info) -> list[str]:
        return ensure_str_list(value, f"mart.validate.table_rules.*.{info.field_name}")


class TransitionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_row_drop_pct: float | None = None
    warn_removed_columns: bool = True

    @field_validator("warn_removed_columns", mode="before")
    @classmethod
    def _parse_warn_removed_columns(cls, value: Any) -> bool:
        return parse_bool(value, "mart.validate.transition.warn_removed_columns")


class MartValidateConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    table_rules: dict[str, MartTableRuleConfig] = Field(default_factory=dict)
    transition: TransitionConfig = Field(default_factory=TransitionConfig)


class MartConfig(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    tables: list[MartTableConfig] = Field(default_factory=list)
    required_tables: list[str] = Field(default_factory=list)
    validate_config: MartValidateConfig = Field(
        default_factory=MartValidateConfig,
        alias="validate",
    )

    @field_validator("required_tables", mode="before")
    @classmethod
    def _normalize_required_tables(cls, value: Any) -> list[str]:
        return ensure_str_list(value, "mart.required_tables")

    @property
    def validate(self) -> MartValidateConfig:
        return self.validate_config


class MartValidationSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    required_tables: list[str] = Field(default_factory=list)
    validate_config: MartValidateConfig = Field(
        default_factory=MartValidateConfig,
        alias="validate",
    )

    @field_validator("required_tables", mode="before")
    @classmethod
    def _normalize_required_tables(cls, value: Any) -> list[str]:
        return ensure_str_list(value, "mart.required_tables")

    @property
    def validate(self) -> MartValidateConfig:
        return self.validate_config
