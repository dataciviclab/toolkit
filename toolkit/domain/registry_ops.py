"""Lettura degli artifact registry committati nei repo del workspace.

Il registry builder (``toolkit.registry.builders``) genera e i repo committano
i cataloghi in ``{repo}/registry/*.json`` (clean_catalog, mart_catalog,
pipeline_signals, codelists, ...). Questo modulo li espone all'agente (CLI e
MCP) senza riprogettare la discovery: lettura diretta dei file committati,
cross-repo.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from toolkit.core.paths import WORKSPACE_ROOT

# Nomi file esclusi dalla vista (schemi e helper, non artifact dati).
EXCLUDED = (
    ".schema.json",
    "README",
)


def _entries_count(name: str, payload: dict[str, Any]) -> int | None:
    """Conteggio entries per tipo artifact (None se struttura sconosciuta)."""
    if name == "clean_catalog":
        return len(payload.get("datasets") or [])
    if name == "mart_catalog":
        return len(payload.get("marts") or [])
    if name == "pipeline_signals":
        return len(payload.get("signals") or [])
    if name == "codelists":
        return len(payload.get("codelists") or {})
    return None


def _scan_repo(repo_dir: Path) -> list[dict[str, Any]]:
    artifacts: list[dict[str, Any]] = []
    registry_dir = repo_dir / "registry"
    if not registry_dir.is_dir():
        return artifacts
    for path in sorted(registry_dir.glob("*.json")):
        if path.name.endswith(EXCLUDED[0]) or path.name.startswith(EXCLUDED[1]):
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            payload = {}
        artifacts.append(
            {
                "name": path.stem,
                "size_bytes": path.stat().st_size,
                "entries": _entries_count(path.stem, payload),
            }
        )
    return artifacts


def list_registries(workspace: Path = WORKSPACE_ROOT) -> dict[str, Any]:
    """Elenca gli artifact registry committati nei repo del workspace.

    Returns:
        Dict con ``repos`` (lista) e ``total_repos``. Ogni repo:
        ``{repo, artifacts: [{name, size_bytes, entries}]}``.
    """
    repos: list[dict[str, Any]] = []
    if not workspace.is_dir():
        return {"repos": repos, "total_repos": 0}

    for repo_dir in sorted(p for p in workspace.iterdir() if p.is_dir()):
        artifacts = _scan_repo(repo_dir)
        if artifacts:
            repos.append({"repo": repo_dir.name, "artifacts": artifacts})

    return {"repos": repos, "total_repos": len(repos)}


def show_registry(
    repo: str,
    artifact: str,
    slug: str | None = None,
    workspace: Path = WORKSPACE_ROOT,
) -> dict[str, Any]:
    """Legge un artifact committato di un repo del workspace.

    Args:
        repo: Nome dir del repo (es. ``eurostat``).
        artifact: Nome artifact senza estensione (es. ``clean_catalog``).
        slug: Se fornito, filtra l'entry (dataset slug per clean/mart,
            id per signals, codelist name per codelists).

    Raises:
        FileNotFoundError: se il file non esiste nel workspace.
    """
    path = workspace / repo / "registry" / f"{artifact}.json"
    if not path.is_file():
        raise FileNotFoundError(
            f"Artifact '{artifact}' non trovato in {workspace}/{repo}/registry/ "
            f"(usa 'toolkit registry list' per gli artifact disponibili)"
        )
    payload = json.loads(path.read_text(encoding="utf-8"))

    if slug:
        filtered = _filter_entry(artifact, payload, slug)
        return {"repo": repo, "artifact": artifact, "slug": slug, "entry": filtered["entry"]}
    return {"repo": repo, "artifact": artifact, "data": payload}


def _filter_entry(name: str, payload: dict[str, Any], slug: str) -> dict[str, Any]:
    """Filtra l'entry richiesta dall'artifact (errore se non trovata)."""
    if name == "clean_catalog":
        entries = payload.get("datasets") or []
        hit = next((d for d in entries if d.get("slug") == slug), None)
        if hit is None:
            raise FileNotFoundError(f"Dataset '{slug}' non presente in clean_catalog")
        return {"slug": slug, "entry": hit}
    if name == "mart_catalog":
        entries = payload.get("marts") or []
        hit = next((m for m in entries if m.get("slug") == slug), None)
        if hit is None:
            raise FileNotFoundError(f"Mart '{slug}' non presente in mart_catalog")
        return {"slug": slug, "entry": hit}
    if name == "pipeline_signals":
        entries = payload.get("signals") or []
        hit = next((s for s in entries if s.get("id") == slug), None)
        if hit is None:
            raise FileNotFoundError(f"Signal '{slug}' non presente in pipeline_signals")
        return {"slug": slug, "entry": hit}
    if name == "codelists":
        entries = payload.get("codelists") or {}
        if slug not in entries:
            raise FileNotFoundError(f"Codelist '{slug}' non presente in codelists")
        return {"slug": slug, "entry": entries[slug]}
    raise FileNotFoundError(
        f"Filtro per slug non supportato per l'artifact '{name}' "
        "(supportati: clean_catalog, mart_catalog, pipeline_signals, codelists)"
    )
