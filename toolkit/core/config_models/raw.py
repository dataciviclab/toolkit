"""Pydantic models for the raw layer configuration."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from toolkit.core.config_models.common import parse_bool


class ClientConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    timeout: int | None = None
    retries: int | None = None
    user_agent: str | None = None
    headers: dict[str, str] | None = None

    @field_validator("headers", mode="before")
    @classmethod
    def _validate_headers(cls, value: Any) -> dict[str, str] | None:
        if value is None:
            return None
        if not isinstance(value, dict):
            raise ValueError("raw.sources[].client.headers must be a dict")
        if not all(isinstance(k, str) and isinstance(v, str) for k, v in value.items()):
            raise ValueError("raw.sources[].client.headers must be a dict[str, str]")
        return dict(value)


class ExtractorConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["identity", "unzip_all", "unzip_first", "unzip_first_csv"] = "identity"
    args: dict[str, Any] = Field(default_factory=dict)

    @field_validator("args", mode="before")
    @classmethod
    def _validate_args(cls, value: Any) -> dict[str, Any]:
        if value is None:
            return {}
        if not isinstance(value, dict):
            raise ValueError("raw.extractor.args must be a dict")
        return dict(value)


class RawSourceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = None
    type: str = "http_file"
    client: ClientConfig = Field(default_factory=ClientConfig)
    args: dict[str, Any] = Field(default_factory=dict)
    extractor: ExtractorConfig | None = None
    primary: bool = False

    @field_validator("primary", mode="before")
    @classmethod
    def _parse_primary(cls, value: Any) -> bool:
        return parse_bool(value, "raw.sources[].primary")

    @field_validator("args", mode="before")
    @classmethod
    def _validate_args(cls, value: Any) -> dict[str, Any]:
        if value is None:
            return {}
        if not isinstance(value, dict):
            raise ValueError("raw.sources[].args must be a dict")
        return dict(value)


class RawConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    output_policy: Literal["overwrite", "versioned"] = "versioned"
    extractor: ExtractorConfig | None = None
    sources: list[RawSourceConfig] = Field(default_factory=list)
