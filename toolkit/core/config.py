"""Config loading and typed access.

ToolkitConfig exposes typed attribute access to the underlying Pydantic
config models (cfg.raw.sources, cfg.clean.sql, etc.) and provides
ensure_dict() for the runner layer that still expects plain dicts.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from toolkit.core.config_models import (
    CleanConfig,
    ConfigPolicy,
    GlobalValidationConfig,
    MartConfig,
    OutputConfig,
    RawConfig,
    SupportDatasetConfig,
    TimeCoverage,
    ToolkitConfigModel,
    ensure_str_list as _ensure_str_list,
    load_config_model,
    parse_bool as _parse_bool,
)


@dataclass(frozen=True)
class ToolkitConfig:
    base_dir: Path
    schema_version: int
    root: Path
    root_source: str
    dataset: str
    source_id: str | None
    years: list[int]
    time_coverage: TimeCoverage | None

    # Internal: the typed model (used by typed properties below)
    _model: ToolkitConfigModel = field(repr=False, compare=False)

    # --- Typed accessors ---

    @property
    def raw(self) -> RawConfig:
        return self._model.raw

    @property
    def clean(self) -> CleanConfig:
        return self._model.clean

    @property
    def mart(self) -> MartConfig:
        return self._model.mart

    @property
    def config(self) -> ConfigPolicy:
        return self._model.config

    @property
    def validation(self) -> GlobalValidationConfig:
        return self._model.validation

    @property
    def output(self) -> OutputConfig:
        return self._model.output

    @property
    def support(self) -> list[SupportDatasetConfig]:
        return list(self._model.support)

    def resolve(self, rel_path: str | Path) -> Path:
        p = Path(rel_path)
        return p if p.is_absolute() else (self.base_dir / p)


def parse_bool(value: Any, field_name: str) -> bool:
    return _parse_bool(value, field_name)


def ensure_str_list(value: Any, field_name: str) -> list[str]:
    return _ensure_str_list(value, field_name)


def ensure_dict(cfg: Any) -> Any:
    """Convert Pydantic model to dict, preserving aliases.

    Uses by_alias=True so that fields like validate_config are serialized
    as "validate" (matching the YAML alias). Excludes unset fields to
    keep the dict lean — consumers use .get(key, default) for missing keys.
    """
    if hasattr(cfg, 'model_dump'):
        return cfg.model_dump(mode="python", by_alias=True, exclude_none=True, exclude_unset=True)
    if isinstance(cfg, list):
        return [ensure_dict(item) for item in cfg]
    return cfg


def load_config(
    path: str | Path,
    *,
    strict_config: bool = False,
    repo_root: str | Path | None = None,
    root_override: str | Path | None = None,
) -> ToolkitConfig:
    model = load_config_model(path, strict_config=strict_config, repo_root=repo_root)
    effective_root = Path(root_override).expanduser().resolve() if root_override else model.root
    return ToolkitConfig(
        base_dir=model.base_dir,
        schema_version=model.schema_version,
        root=effective_root,
        root_source="--root" if root_override else model.root_source,
        dataset=model.dataset.name,
        source_id=model.dataset.source_id,
        years=list(model.dataset.years),
        time_coverage=model.dataset.time_coverage,
        _model=model,
    )
