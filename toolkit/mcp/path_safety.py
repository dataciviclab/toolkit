"""Path safety and config loading for the MCP toolkit client.

Provides:
- _safe_path: resolve and validate a config path
- _load_cfg: load a toolkit config with error translation
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

from lab_connectors.mcp.errors import ErrorCode

from toolkit.core.config import load_config
from toolkit.mcp.errors import ToolkitClientError


TOOLKIT_ROOT = Path(__file__).resolve().parents[2]
WORKSPACE_ROOT = Path(
    os.environ.get("DATACIVICLAB_WORKSPACE", str(TOOLKIT_ROOT.parent))
).expanduser()
TOOLKIT_PYTHON = Path(os.environ.get("DATACIVICLAB_TOOLKIT_PYTHON", sys.executable))


def _safe_path(config_path: str | Path) -> Path:
    path = Path(config_path).expanduser()
    if not path.is_absolute():
        path = (WORKSPACE_ROOT / path).resolve()

    # Se il path è una directory, prova dataset.yml al suo interno,
    # in modo che i tool accettino anche path di directory (es. da list_candidates).
    if path.is_dir():
        probe = path / "dataset.yml"
        if probe.exists():
            return probe

    if not path.exists():
        # Fallback: tenta risoluzione come slug
        resolved = _resolve_dataset(str(config_path))
        if resolved is not None:
            return resolved
        raise ToolkitClientError(
            f"Config non trovata: {path}. "
            f"Se è uno slug, verifica che sia presente in candidates/ o support_datasets/.",
            code=ErrorCode.CONFIG_NOT_FOUND,
        )
    return path


_RESOLVED_SLUG_CACHE: dict[str, Path] = {}
# Nota: cache globale senza invalidazione. In un server MCP long-running,
# l'aggiunta/rimozione di candidate non invalida la cache. Impatto basso
# perché il server ricarica i moduli solo al restart. Se in futuro il server
# diventa persistente, aggiungere un meccanismo di invalidazione (timestamp,
# TTL, o watch del filesystem).


def _resolve_dataset(slug_or_path: str | Path) -> Path | None:
    """Risolve uno slug o path di dataset in un path assoluto a dataset.yml.

    MAI chiama ``_safe_path`` (nessun loop). Se non trova, restituisce ``None``.

    1. Se il valore è un file esistente → restituisce quello.
    2. Se è uno slug (es. ``terna-electricity-by-source``) → cerca in:
       - ``{WORKSPACE}/dataset-incubator/candidates/{slug}/dataset.yml``
       - ``{WORKSPACE}/dataset-incubator/support_datasets/{slug}/dataset.yml``
    3. Path annidato (es. ``ispra-ru-costi-kg/sources/a_ru_base``).
    4. Fallback glob ricorsivo (cache).

    Returns:
        Path assoluto al file dataset.yml, o ``None`` se non trovato.
    """
    candidate = Path(slug_or_path).expanduser()

    # Strategy 1: file esistente → usalo direttamente
    if candidate.exists() and candidate.suffix in (".yml", ".yaml"):
        return candidate.resolve()

    key = str(slug_or_path)
    if key in _RESOLVED_SLUG_CACHE:
        return _RESOLVED_SLUG_CACHE[key]

    # Strategy 2: slug in candidates/ o support_datasets/
    for subdir in ("candidates", "support_datasets"):
        probe = WORKSPACE_ROOT / "dataset-incubator" / subdir / str(slug_or_path) / "dataset.yml"
        if probe.exists():
            resolved = probe.resolve()
            _RESOLVED_SLUG_CACHE[key] = resolved
            return resolved

    # Strategy 3: path annidato dentro dataset-incubator
    for subdir in ("candidates", "support_datasets"):
        probe = WORKSPACE_ROOT / "dataset-incubator" / subdir / str(slug_or_path)
        if probe.exists() and probe.suffix in (".yml", ".yaml"):
            _RESOLVED_SLUG_CACHE[key] = probe.resolve()
            return _RESOLVED_SLUG_CACHE[key]
        probe_yml = probe / "dataset.yml" if probe.suffix != ".yml" else probe
        if probe_yml.exists():
            _RESOLVED_SLUG_CACHE[key] = probe_yml.resolve()
            return _RESOLVED_SLUG_CACHE[key]

    # Strategy 4: fallback glob ricorsivo
    incubator = WORKSPACE_ROOT / "dataset-incubator"
    if incubator.exists():
        matches = list(incubator.rglob(f"**/{slug_or_path}/dataset.yml"))
        if not matches:
            matches = list(incubator.rglob(f"**/{slug_or_path}.yml"))
        if matches:
            resolved = matches[0].resolve()
            _RESOLVED_SLUG_CACHE[key] = resolved
            return resolved

    # Non trovato
    return None


def _load_cfg(config_path: str | Path) -> tuple[Path, Any]:
    config = _safe_path(str(config_path))
    try:
        cfg = load_config(str(config), strict_config=False)
    except Exception as exc:
        raise ToolkitClientError(f"Load config fallito per {config}: {exc}", code=ErrorCode.CONFIG_NOT_FOUND) from exc
    return config, cfg
