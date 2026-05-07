"""Config loading and typed access.

ToolkitConfig exposes both typed attribute access (cfg.raw.sources) and
dict-style backward compat (cfg.raw.get("sources")) for gradual migration.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from toolkit.core.config_models import (
    TimeCoverage,
    ToolkitConfigModel,
    ensure_str_list as _ensure_str_list,
    load_config_model,
    parse_bool as _parse_bool,
)


class _CompatModel:
    """Wraps a Pydantic model to support both attribute and dict-style access.

    During migration, consumers can use either:
      cfg.raw.sources        (typed, preferred)
      cfg.raw.get("sources") (backward compat)
      cfg.raw["sources"]     (backward compat)
    """

    def __init__(self, model: BaseModel) -> None:
        self._model = model

    def __getattr__(self, name: str) -> Any:
        # Attribute access: cfg.raw.sources
        # Only called for attributes NOT on _CompatModel itself
        return getattr(self._model, name)

    def get(self, key: str, default: Any = None) -> Any:
        """Dict-style access: cfg.clean.get("sql")

        Returns nested Pydantic models as plain dicts to maintain
        full dict-style compatibility for existing consumers.
        """
        value = getattr(self._model, key, default)
        if isinstance(value, BaseModel):
            return value.model_dump(mode="python", by_alias=True, exclude_none=True, exclude_unset=True)
        if isinstance(value, list):
            return [
                item.model_dump(mode="python", by_alias=True, exclude_none=True, exclude_unset=True)
                if isinstance(item, BaseModel) else item
                for item in value
            ]
        return value

    def __eq__(self, other: object) -> bool:
        """Compare against dict or model for backward compat."""
        if isinstance(other, dict):
            return self._model.model_dump(mode="python", by_alias=True, exclude_none=True, exclude_unset=True) == other
        if isinstance(other, BaseModel):
            return self._model == other
        return NotImplemented

    def __getitem__(self, key: str) -> Any:
        """Dict-style item access: cfg.clean["sql"]"""
        value = getattr(self._model, key)
        if isinstance(value, BaseModel):
            return _CompatModel(value)
        return value

    def __contains__(self, key: str) -> bool:
        return hasattr(self._model, key)

    def model_dump(self, **kwargs) -> dict[str, Any]:
        return self._model.model_dump(**kwargs)


def _wrap_model(obj: BaseModel) -> _CompatModel:
    return _CompatModel(obj)


@dataclass(frozen=True)
class ToolkitConfig:
    base_dir: Path
    schema_version: int
    root: Path
    root_source: str
    dataset: str
    years: list[int]
    time_coverage: TimeCoverage | None

    # Internal: the typed model (used by typed properties below)
    _model: ToolkitConfigModel = field(repr=False, compare=False)

    # --- Typed accessors (return _CompatModel for gradual migration) ---

    @property
    def raw(self) -> _CompatModel:
        return _wrap_model(self._model.raw)

    @property
    def clean(self) -> _CompatModel:
        return _wrap_model(self._model.clean)

    @property
    def mart(self) -> _CompatModel:
        return _wrap_model(self._model.mart)

    @property
    def cross_year(self) -> _CompatModel:
        return _wrap_model(self._model.cross_year)

    @property
    def config(self) -> _CompatModel:
        return _wrap_model(self._model.config)

    @property
    def validation(self) -> _CompatModel:
        return _wrap_model(self._model.validation)

    @property
    def output(self) -> _CompatModel:
        return _wrap_model(self._model.output)

    @property
    def support(self) -> list[_CompatModel]:
        return [_wrap_model(item) for item in self._model.support]

    def resolve(self, rel_path: str | Path) -> Path:
        p = Path(rel_path)
        return p if p.is_absolute() else (self.base_dir / p)


def parse_bool(value: Any, field_name: str) -> bool:
    return _parse_bool(value, field_name)


def ensure_str_list(value: Any, field_name: str) -> list[str]:
    return _ensure_str_list(value, field_name)


def ensure_dict(cfg: Any) -> Any:
    """Convert _CompatModel or Pydantic model to dict, preserving aliases.

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
) -> ToolkitConfig:
    model = load_config_model(path, strict_config=strict_config, repo_root=repo_root)
    return ToolkitConfig(
        base_dir=model.base_dir,
        schema_version=model.schema_version,
        root=model.root,
        root_source=model.root_source,
        dataset=model.dataset.name,
        years=list(model.dataset.years),
        time_coverage=model.dataset.time_coverage,
        _model=model,
    )
