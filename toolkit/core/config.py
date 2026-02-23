# toolkit/core/config.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml


@dataclass(frozen=True)
class ToolkitConfig:
    base_dir: Path
    schema_version: int
    root: Path | None
    dataset: str
    years: list[int]
    raw: dict[str, Any]
    clean: dict[str, Any]
    mart: dict[str, Any]
    bq: dict[str, Any] | None

    def resolve(self, rel_path: str | Path) -> Path:
        p = Path(rel_path)
        return p if p.is_absolute() else (self.base_dir / p)

    def resolved_root(self) -> Path | None:
        if self.root is None:
            return None
        # root può essere relativo al file dataset.yml
        return self.resolve(self.root)


def _err(msg: str, *, path: Path) -> ValueError:
    return ValueError(f"{msg} (file: {path})")


def _require_map(data: Mapping[str, Any], key: str, *, path: Path) -> dict[str, Any]:
    val = data.get(key)
    if not isinstance(val, dict):
        raise _err(f"Campo '{key}' mancante o non valido (deve essere una mappa).", path=path)
    return val


def load_config(path: str | Path) -> ToolkitConfig:
    p = Path(path)

    try:
        data = yaml.safe_load(p.read_text(encoding="utf-8"))
    except Exception as e:
        raise _err(f"Impossibile leggere YAML: {e}", path=p)

    if not isinstance(data, dict):
        raise _err("dataset.yml deve essere una mappa YAML.", path=p)

    schema_version = int(data.get("schema_version", 1))

    dataset_block = _require_map(data, "dataset", path=p)
    name = dataset_block.get("name")
    if not name or not isinstance(name, str):
        raise _err("Campo obbligatorio mancante o non valido: dataset.name (string).", path=p)

    years = dataset_block.get("years")
    if not isinstance(years, (list, tuple)) or not years:
        raise _err("dataset.years deve essere una lista non vuota, es: [2022, 2023].", path=p)
    try:
        years_int = [int(y) for y in years]
    except Exception:
        raise _err("dataset.years deve contenere solo numeri (es: [2022, 2023]).", path=p)

    raw = data.get("raw", {}) or {}
    clean = data.get("clean", {}) or {}
    mart = data.get("mart", {}) or {}
    if not isinstance(raw, dict) or not isinstance(clean, dict) or not isinstance(mart, dict):
        raise _err("raw/clean/mart devono essere mappe YAML (oggetti).", path=p)

    root = data.get("root")
    root_path = Path(root) if isinstance(root, str) and root.strip() else None

    bq = data.get("bq")
    if bq is not None and not isinstance(bq, dict):
        raise _err("bq deve essere una mappa YAML (oggetto) oppure null.", path=p)

    return ToolkitConfig(
        base_dir=p.parent,
        schema_version=schema_version,
        root=root_path,
        dataset=name,
        years=years_int,
        raw=raw,
        clean=clean,
        mart=mart,
        bq=bq,
    )