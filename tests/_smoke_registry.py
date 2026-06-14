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
    """Nome breve, es. 'local_file_csv' o 'bdap_http_csv_offline'."""

    source_type: str
    """Tipo di fonte: local_file, http_file, ckan, sdmx."""

    requires_network: bool
    """Se True, lo smoke fa richieste HTTP reali (skippato in PR CI)."""

    _config_name: str = "dataset.yml"
    """Nome del file dataset.yml da usare (dataset.yml o dataset.offline.yml)."""

    @property
    def config_path(self) -> Path:
        """Path assoluto al file dataset.yml da usare per questo smoke."""
        return self.path / self._config_name


OFFLINE_CONFIG_NAME = "dataset.offline.yml"
ONLINE_CONFIG_NAME = "dataset.yml"


def _read_source_type_from_config(config_path: Path) -> str | None:
    """Legge il primo source type da un file dataset.yml."""
    if not config_path.exists():
        return None
    with open(config_path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    sources = (data or {}).get("raw", {}).get("sources", [])
    if not sources:
        return None
    return sources[0].get("type")


def _has_online_config(smoke_path: Path) -> bool:
    """Vero se esiste dataset.yml (config online/rete)."""
    return (smoke_path / ONLINE_CONFIG_NAME).exists()


def _has_offline_config(smoke_path: Path) -> bool:
    """Vero se esiste dataset.offline.yml (config locale/fixture)."""
    return (smoke_path / OFFLINE_CONFIG_NAME).exists()


def discover_smokes() -> list[SmokeTemplate]:
    """Scansiona ``smoke/`` e restituisce tutti gli SmokeTemplate disponibili.

    Per ogni directory con ``dataset.yml``, crea un template.
    Se esiste anche ``dataset.offline.yml``, crea un secondo template
    con suffisso ``_offline``.
    """
    if not SMOKE_DIR.is_dir():
        return []

    result: list[SmokeTemplate] = []
    for entry in sorted(SMOKE_DIR.iterdir()):
        if not entry.is_dir():
            continue

        # Config online (dataset.yml) — sorgente con rete
        if _has_online_config(entry):
            source_type = _read_source_type_from_config(entry / ONLINE_CONFIG_NAME)
            if source_type:
                result.append(
                    SmokeTemplate(
                        path=entry.resolve(),
                        name=entry.name,
                        source_type=source_type,
                        requires_network=source_type not in OFFLINE_SOURCE_TYPES,
                        _config_name=ONLINE_CONFIG_NAME,
                    )
                )

        # Config offline (dataset.offline.yml) — sorgente locale con fixture
        if _has_offline_config(entry):
            offline_type = _read_source_type_from_config(entry / OFFLINE_CONFIG_NAME)
            if offline_type:
                result.append(
                    SmokeTemplate(
                        path=entry.resolve(),
                        name=f"{entry.name}_offline",
                        source_type=offline_type,
                        requires_network=offline_type not in OFFLINE_SOURCE_TYPES,
                        _config_name=OFFLINE_CONFIG_NAME,
                    )
                )

    return result


def discover_offline_smokes() -> list[SmokeTemplate]:
    """Solo smoke che non richiedono rete (local_file)."""
    return [s for s in discover_smokes() if not s.requires_network]


def iter_smoke_configs(smokes: list[SmokeTemplate]) -> Iterator[tuple[str, Path]]:
    """Per parametrizzazione pytest: (node_id, path a dataset.yml)."""
    for s in smokes:
        yield s.name, s.config_path
