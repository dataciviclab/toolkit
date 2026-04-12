"""Pydantic models for the cross_year layer configuration."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class CrossYearTableConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    sql: Path
    source_layer: Literal["clean", "mart"] = "clean"
    source_table: str | None = None


class CrossYearConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tables: list[CrossYearTableConfig] = Field(default_factory=list)
