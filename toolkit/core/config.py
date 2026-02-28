from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from toolkit.core.config_models import (
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
    years: list[int]
    raw: dict[str, Any]
    clean: dict[str, Any]
    mart: dict[str, Any]
    config: dict[str, Any]
    validation: dict[str, Any]
    output: dict[str, Any]
    bq: dict[str, Any] | None

    def resolve(self, rel_path: str | Path) -> Path:
        p = Path(rel_path)
        return p if p.is_absolute() else (self.base_dir / p)

    def resolved_root(self) -> Path:
        return self.root


def parse_bool(value: Any, field_name: str) -> bool:
    return _parse_bool(value, field_name)


def ensure_str_list(value: Any, field_name: str) -> list[str]:
    return _ensure_str_list(value, field_name)


def _compat_raw(model: ToolkitConfigModel) -> dict[str, Any]:
    raw = model.raw.model_dump(mode="python", exclude_none=True, exclude_unset=True)
    sources = raw.get("sources") or []
    if sources and "source" not in raw:
        raw["source"] = dict(sources[0])
    return raw


def _compat_clean(model: ToolkitConfigModel) -> dict[str, Any]:
    return model.clean.model_dump(
        mode="python",
        by_alias=True,
        exclude_none=True,
        exclude_unset=True,
    )


def _compat_mart(model: ToolkitConfigModel) -> dict[str, Any]:
    return model.mart.model_dump(
        mode="python",
        by_alias=True,
        exclude_none=True,
        exclude_unset=True,
    )


def load_config(path: str | Path, *, strict_config: bool = False) -> ToolkitConfig:
    model = load_config_model(path, strict_config=strict_config)
    return ToolkitConfig(
        base_dir=model.base_dir,
        schema_version=model.schema_version,
        root=model.root,
        root_source=model.root_source,
        dataset=model.dataset.name,
        years=list(model.dataset.years),
        raw=_compat_raw(model),
        clean=_compat_clean(model),
        mart=_compat_mart(model),
        config=model.config.model_dump(mode="python"),
        validation=model.validation.model_dump(mode="python"),
        output=model.output.model_dump(mode="python"),
        bq=model.bq,
    )
