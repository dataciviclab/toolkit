"""ToolkitConfigModel and load_config_model - config loading entry points."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from toolkit.core.config_models.clean import CleanConfig
from toolkit.core.config_models.cross_year import CrossYearConfig
from toolkit.core.config_models.mart import MartConfig
from toolkit.core.config_models.raw import RawConfig
from toolkit.core.config_models.common import (
    ConfigPolicy,
    DatasetBlock,
    GlobalValidationConfig,
    OutputConfig,
    SupportDatasetConfig,
    _err,
    _ensure_root_within_repo,
    _normalize_legacy_payload,
    _normalize_section_paths,
    _require_map,
    _resolve_root,
    _warn_or_reject_unknown_keys,
    parse_bool,
)


class ToolkitConfigModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    base_dir: Path
    schema_version: int = 1
    root: Path
    root_source: str
    dataset: DatasetBlock
    raw: RawConfig = Field(default_factory=RawConfig)
    clean: CleanConfig = Field(default_factory=CleanConfig)
    mart: MartConfig = Field(default_factory=MartConfig)
    support: list[SupportDatasetConfig] = Field(default_factory=list)
    cross_year: CrossYearConfig = Field(default_factory=CrossYearConfig)
    config: ConfigPolicy = Field(default_factory=ConfigPolicy)
    validation: GlobalValidationConfig = Field(default_factory=GlobalValidationConfig)
    output: OutputConfig = Field(default_factory=OutputConfig)

    @model_validator(mode="after")
    def _validate_unique_support_names(self) -> "ToolkitConfigModel":
        names = [entry.name for entry in self.support]
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise ValueError(
                "support[].name values must be unique: " + ", ".join(duplicates)
            )
        return self


def _validation_error_to_value_error(exc: ValidationError, *, path: Path) -> ValueError:
    messages: list[str] = []
    for error in exc.errors():
        loc = ".".join(str(part) for part in error.get("loc", ()))
        msg = error.get("msg", "Invalid value")
        messages.append(f"{loc}: {msg}" if loc else msg)
    return _err("Config validation failed: " + "; ".join(messages), path=path)


def _read_strict_config(data: dict[str, Any], *, path: Path) -> bool:
    raw_config = data.get("config")
    if raw_config is None:
        return False
    if not isinstance(raw_config, dict):
        raise _err("config must be a mapping object if provided.", path=path)
    strict_value = raw_config.get("strict", False)
    return parse_bool(strict_value, "config.strict")


def load_config_model(
    path: str | Path,
    *,
    strict_config: bool = False,
    repo_root: str | Path | None = None,
) -> ToolkitConfigModel:
    """
    Load and normalize toolkit config.

    repo_root is an optional guardrail for callers that need to enforce that
    the resolved effective root stays inside a known repository tree. This is
    intentionally opt-in so the toolkit can still support valid workflows that
    write outputs outside the project directory. A typical caller is external
    CI that validates dataset.yml contracts for monorepos such as
    dataset-incubator.
    """
    p = Path(path)
    base_dir = p.parent.resolve()

    try:
        data = yaml.safe_load(p.read_text(encoding="utf-8"))
    except Exception as e:
        raise _err(f"Impossibile leggere YAML: {e}", path=p)

    if not isinstance(data, dict):
        raise _err("dataset.yml deve essere una mappa YAML.", path=p)

    dataset_block = _require_map(data, "dataset", path=p)
    if "name" not in dataset_block:
        raise _err("Campo obbligatorio mancante o non valido: dataset.name (string).", path=p)
    if "years" not in dataset_block:
        raise _err("dataset.years deve essere una lista non vuota, es: [2022, 2023].", path=p)

    strict_mode = strict_config or _read_strict_config(data, path=p)
    normalized = _normalize_legacy_payload(data, path=p, strict_config=strict_mode)
    normalized = _warn_or_reject_unknown_keys(normalized, path=p, strict_config=strict_mode)
    root_path, root_source = _resolve_root(normalized.get("root"), base_dir=base_dir)
    if repo_root is not None:
        repo_root_path = Path(repo_root).expanduser().resolve()
        if not repo_root_path.is_dir():
            raise _err(
                f"repo_root does not exist or is not a directory: {repo_root_path}",
                path=p,
            )
        root_path = _ensure_root_within_repo(root_path, repo_root=repo_root_path, path=p)

    raw = normalized.get("raw", {}) or {}
    clean = normalized.get("clean", {}) or {}
    mart = normalized.get("mart", {}) or {}
    support = normalized.get("support", []) or []
    cross_year = normalized.get("cross_year", {}) or {}

    normalized_fields: list[tuple[str, Path]] = []
    if isinstance(raw, dict):
        raw, raw_changes = _normalize_section_paths("raw", raw, base_dir=base_dir)
        normalized_fields.extend(raw_changes)
    if isinstance(clean, dict):
        clean, clean_changes = _normalize_section_paths("clean", clean, base_dir=base_dir)
        normalized_fields.extend(clean_changes)
    if isinstance(mart, dict):
        mart, mart_changes = _normalize_section_paths("mart", mart, base_dir=base_dir)
        normalized_fields.extend(mart_changes)
    if isinstance(support, list):
        support, support_changes = _normalize_section_paths("support", support, base_dir=base_dir)
        normalized_fields.extend(support_changes)
    if isinstance(cross_year, dict):
        cross_year, cross_year_changes = _normalize_section_paths("cross_year", cross_year, base_dir=base_dir)
        normalized_fields.extend(cross_year_changes)
    normalized_fields.append(("root", root_path))

    if normalized_fields:
        summary = ", ".join(f"{field}={value}" for field, value in normalized_fields)
        logger = logging.getLogger("toolkit.core.config")
        logger.debug("Normalized config paths: %s", summary)

    payload = {
        **normalized,
        "base_dir": base_dir,
        "root": root_path,
        "root_source": root_source,
        "raw": raw,
        "clean": clean,
        "mart": mart,
        "support": support,
        "cross_year": cross_year,
    }

    try:
        return ToolkitConfigModel.model_validate(payload)
    except ValidationError as exc:
        raise _validation_error_to_value_error(exc, path=p) from None
