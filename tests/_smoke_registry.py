from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import yaml

SMOKE_DIR = Path(__file__).resolve().parent.parent / "smoke"

# Source types che non richiedono rete né server locale
OFFLINE_SOURCE_TYPES = {"local_file"}


@dataclass(frozen=True)
class SmokeTemplate:
    """Uno scenario smoke canonico, pronto per essere usato dai test."""

    path: Path
    """Path assoluto alla directory dello smoke."""

    name: str
    """Nome breve, es. 'local_file_csv'."""

    source_type: str
    """Tipo di fonte: local_file, http_file, ckan, sdmx."""

    requires_network: bool
    """Se True, lo smoke fa richieste HTTP reali (skippato in PR CI)."""


def _read_source_type(smoke_path: Path) -> str | None:
    """Legge il primo source type dal dataset.yml di uno smoke."""
    config_path = smoke_path / "dataset.yml"
    if not config_path.exists():
        return None
    with open(config_path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    sources = (data or {}).get("raw", {}).get("sources", [])
    if not sources:
        return None
    return sources[0].get("type")


def discover_smokes() -> list[SmokeTemplate]:
    """Scansiona ``smoke/`` e restituisce tutti gli SmokeTemplate disponibili."""
    if not SMOKE_DIR.is_dir():
        return []

    result: list[SmokeTemplate] = []
    for entry in sorted(SMOKE_DIR.iterdir()):
        if not entry.is_dir():
            continue
        if not (entry / "dataset.yml").exists():
            continue
        source_type = _read_source_type(entry)
        if not source_type:
            continue
        result.append(
            SmokeTemplate(
                path=entry.resolve(),
                name=entry.name,
                source_type=source_type,
                requires_network=source_type not in OFFLINE_SOURCE_TYPES,
            )
        )
    return result


def discover_offline_smokes() -> list[SmokeTemplate]:
    """Solo smoke che non richiedono rete (local_file)."""
    return [s for s in discover_smokes() if not s.requires_network]


def iter_smoke_configs(smokes: list[SmokeTemplate]) -> Iterator[tuple[str, Path]]:
    """Per parametrizzazione pytest: (node_id, path a dataset.yml)."""
    for s in smokes:
        yield s.name, s.path / "dataset.yml"
