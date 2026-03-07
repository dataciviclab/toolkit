# toolkit/core/paths.py
from __future__ import annotations

import os
import re
from pathlib import Path, PurePath, PurePosixPath, PureWindowsPath

def _to_pure_path(path: str | os.PathLike[str]) -> PurePath:
    raw = os.fspath(path)
    if "\\" in raw or re.match(r"^[A-Za-z]:[\\/]", raw):
        return PureWindowsPath(raw)
    return PurePosixPath(raw)


def to_root_relative(path: Path, root: Path) -> str:
    path_pure = _to_pure_path(path)
    root_pure = _to_pure_path(root)
    rel = path_pure.relative_to(root_pure)
    return rel.as_posix()


def from_root_relative(rel: str, root: Path) -> Path:
    root_pure = _to_pure_path(root)
    rel_pure = _to_pure_path(rel.replace("\\", "/"))
    return Path(str(root_pure / rel_pure))


def resolve_root(root: str | os.PathLike[str]) -> Path:
    return Path(root).expanduser()


def dataset_dir(root: str | os.PathLike[str], layer: str, dataset: str) -> Path:
    return resolve_root(root) / "data" / layer / dataset


def layer_year_dir(root: str | os.PathLike[str], layer: str, dataset: str, year: int | str) -> Path:
    return dataset_dir(root, layer, dataset) / str(year)


def layer_dataset_dir(root: str | os.PathLike[str], layer: str, dataset: str) -> Path:
    return dataset_dir(root, layer, dataset)


def run_dir(root: str | os.PathLike[str], layer: str, dataset: str, year: int | str, run_id: str) -> Path:
    # Esempio: data/clean/ispra/2022/_runs/20260223T221500Z_x9a2f
    return layer_year_dir(root, layer, dataset, year) / "_runs" / run_id


def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p
