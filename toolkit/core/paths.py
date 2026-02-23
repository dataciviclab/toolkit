# toolkit/core/paths.py
from __future__ import annotations

import os
from pathlib import Path


def _is_colab() -> bool:
    return Path("/content").exists() and os.environ.get("COLAB_RELEASE_TAG") is not None


def resolve_root(root: str | None) -> Path:
    if root:
        return Path(root)

    env_root = os.environ.get("DCL_ROOT")
    if env_root:
        return Path(env_root)

    drive = Path("/content/drive/MyDrive/DataCivicLab")
    if _is_colab() and drive.exists():
        return drive

    return Path.cwd()


def dataset_dir(root: str | None, layer: str, dataset: str) -> Path:
    return resolve_root(root) / "data" / layer / dataset


def layer_year_dir(root: str | None, layer: str, dataset: str, year: int | str) -> Path:
    return dataset_dir(root, layer, dataset) / str(year)


def run_dir(root: str | None, layer: str, dataset: str, year: int | str, run_id: str) -> Path:
    # Esempio: data/clean/ispra/2022/_runs/20260223T221500Z_x9a2f
    return layer_year_dir(root, layer, dataset, year) / "_runs" / run_id


def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p