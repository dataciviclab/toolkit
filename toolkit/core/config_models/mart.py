"""Pydantic models for the mart layer configuration."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from typing import Literal

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
    years: list[int] | None = None
    source_layer: Literal["clean", "mart"] = "clean"
    source_table: str | None = None

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
    max_null_pct: dict[str, float] = Field(default_factory=dict)
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


class HierarchyLevel(BaseModel):
    """Un livello della gerarchia mart (es. comune, provincia, regione).

    A runtime, la query di aggregazione viene generata automaticamente:
    - colonne metriche scoperte per introspection dalla source
    - GROUP BY sulle colonne grain
    - SUM per ogni metrica numerica

    Non richiede un file SQL: il config è attivo.
    """

    model_config = ConfigDict(extra="forbid")

    level: str
    table: str
    grain: list[str]
    source_table: str | None = None
    exclude_metrics: list[str] = Field(default_factory=list)

    @field_validator("level")
    @classmethod
    def _validate_level(cls, value: str) -> str:
        v = value.strip()
        if not v:
            raise ValueError("mart.hierarchy.levels[].level must not be empty")
        return v

    @field_validator("table")
    @classmethod
    def _validate_table(cls, value: str) -> str:
        text = value.strip()
        if not text:
            raise ValueError("mart.hierarchy.levels[].table must not be empty")
        if not re.fullmatch(_SAFE_SQL_IDENTIFIER_RE, text):
            raise ValueError(
                "mart.hierarchy.levels[].table must be a safe SQL identifier "
                "(letters, numbers, underscore; cannot start with a number)"
            )
        return text

    @field_validator("source_table")
    @classmethod
    def _validate_source_table(cls, value: str | None) -> str | None:
        if value is not None:
            text = value.strip()
            if text and not re.fullmatch(_SAFE_SQL_IDENTIFIER_RE, text):
                raise ValueError(
                    "mart.hierarchy.levels[].source_table must be a safe SQL identifier "
                    "(letters, numbers, underscore; cannot start with a number)"
                )
        return value

    @field_validator("grain")
    @classmethod
    def _validate_grain(cls, value: list[str]) -> list[str]:
        for g in value:
            if not re.fullmatch(_SAFE_SQL_IDENTIFIER_RE, g.strip()):
                raise ValueError(
                    f"mart.hierarchy.levels[].grain element '{g}' must be a safe SQL identifier "
                    "(letters, numbers, underscore; cannot start with a number)"
                )
        return value

    @field_validator("exclude_metrics")
    @classmethod
    def _validate_exclude_metrics(cls, value: list[str]) -> list[str]:
        for m in value:
            if not re.fullmatch(_SAFE_SQL_IDENTIFIER_RE, m.strip()):
                raise ValueError(
                    f"mart.hierarchy.levels[].exclude_metrics element '{m}' must be a safe SQL identifier "
                    "(letters, numbers, underscore; cannot start with a number)"
                )
        return value


class HierarchyConfig(BaseModel):
    """Gerarchia mart: aggregazione per asse naturale del dato."""

    model_config = ConfigDict(extra="forbid")

    axis: str = Field(..., pattern=r"^(territoriale|temporale|categorico)$")
    levels: list[HierarchyLevel] = Field(min_length=1)

    @field_validator("levels")
    @classmethod
    def _validate_levels_order(cls, value: list[HierarchyLevel]) -> list[HierarchyLevel]:
        if len(value) < 1:
            raise ValueError("mart.hierarchy.levels must have at least one level")
        return value


class MartConfig(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    tables: list[MartTableConfig] = Field(default_factory=list)
    required_tables: list[str] = Field(default_factory=list)
    hierarchy: HierarchyConfig | None = None
    validate_config: MartValidateConfig = Field(
        default_factory=MartValidateConfig,
        alias="validate",
    )

    @field_validator("required_tables", mode="before")
    @classmethod
    def _normalize_required_tables(cls, value: Any) -> list[str]:
        return ensure_str_list(value, "mart.required_tables")

    @model_validator(mode="after")
    def _default_required_tables_from_tables(self) -> MartConfig:
        """If required_tables is empty, default to all table names from tables."""
        if not self.required_tables and self.tables:
            object.__setattr__(self, "required_tables", [t.name for t in self.tables])
        return self

    @property
    def validate(self) -> MartValidateConfig:  # type: ignore[override]
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
    def validate(self) -> MartValidateConfig:  # type: ignore[override]
        return self.validate_config
