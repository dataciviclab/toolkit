from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from toolkit.core.config_models import (
    TimeCoverage,
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
    years: list[int]
    time_coverage: TimeCoverage | None
    raw: dict[str, Any]
    clean: dict[str, Any]
    mart: dict[str, Any]
    support: list[dict[str, Any]]
    cross_year: dict[str, Any]
    config: dict[str, Any]
    validation: dict[str, Any]
    output: dict[str, Any]

    def resolve(self, rel_path: str | Path) -> Path:
        p = Path(rel_path)
        return p if p.is_absolute() else (self.base_dir / p)

    def resolved_root(self) -> Path:
        return self.root


def parse_bool(value: Any, field_name: str) -> bool:
    return _parse_bool(value, field_name)


def ensure_str_list(value: Any, field_name: str) -> list[str]:
    return _ensure_str_list(value, field_name)


def _model_dump(obj: Any) -> dict[str, Any]:
    """Standardized model_dump: aliases resolved, clean output."""
    return obj.model_dump(mode="python", by_alias=True, exclude_none=True, exclude_unset=True)


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
        raw=_model_dump(model.raw),
        clean=_model_dump(model.clean),
        mart=_model_dump(model.mart),
        support=[_model_dump(item) for item in model.support],
        cross_year=_model_dump(model.cross_year),
        config=_model_dump(model.config),
        validation=_model_dump(model.validation),
        output=_model_dump(model.output),
    )
