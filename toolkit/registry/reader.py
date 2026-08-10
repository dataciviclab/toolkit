"""Lettore dell'artifact registry committato nei repo del workspace.

Il registry builder (``toolkit.registry.builders``) genera e i repo committano
il file unico ``{repo}/registry/registry.json`` (fusion ADR): sezioni
``datasets``, ``marts``, ``signals``, ``codelists``, ``entities``.

Esposizione all'agente (CLI e MCP) via ``registry list`` /
``registry show <repo> <section>``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from toolkit.core.paths import WORKSPACE_ROOT

# Sezioni esposte (ordine stabile per list).
SECTIONS = ("datasets", "marts", "signals", "codelists", "entities")


def _section_count(section: str, payload: dict[str, Any]) -> int | None:
    """Conteggio entries di una sezione del registry unico."""
    if section == "datasets":
        return len(payload.get("datasets") or [])
    if section == "marts":
        return len(payload.get("marts") or [])
    if section == "signals":
        return len(payload.get("signals") or [])
    if section == "codelists":
        return len(payload.get("codelists") or {})
    if section == "entities":
        return len((payload.get("entities") or {}).get("entities") or {})
    return None


def load_repo_registry(repo_dir: Path) -> dict[str, Any] | None:
    """Carica il registry unico del repo (registry.json), o None se assente."""
    path = repo_dir / "registry" / "registry.json"
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def _scan_repo(repo_dir: Path) -> list[dict[str, Any]]:
    """Artifact del repo: registry unico (con conteggi per sezione)."""
    payload = load_repo_registry(repo_dir)
    if payload is None:
        return []
    counts = {section: _section_count(section, payload) for section in SECTIONS}
    return [
        {
            "name": "registry",
            "size_bytes": (repo_dir / "registry" / "registry.json").stat().st_size,
            "sections": counts,
        }
    ]


def list_registries(workspace: Path = WORKSPACE_ROOT) -> dict[str, Any]:
    """Elenca i registry committati nei repo del workspace.

    Returns:
        Dict con ``repos`` (lista) e ``total_repos``. Ogni repo:
        ``{repo, artifacts: [{name, size_bytes, sections: {...}}]}``.
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
    """Mostra il registry (o una sezione) di un repo del workspace.

    Args:
        repo: Nome dir del repo (es. ``eurostat``).
        artifact: Sezione da mostrare (``datasets``, ``marts``, ``signals``,
            ``codelists``, ``entities``) o ``registry`` (intero).
        slug: Se fornito, filtra l'entry (dataset slug, mart slug, signal id,
            codelist name).

    Raises:
        FileNotFoundError: se il repo/section non esiste nel workspace.
    """
    repo_dir = workspace / repo
    payload = load_repo_registry(repo_dir)

    if payload is None:
        raise FileNotFoundError(
            f"Registry non trovato in {workspace}/{repo}/registry/ "
            f"(usa 'toolkit registry list' per i repo disponibili)"
        )

    if artifact == "registry":
        return {
            "repo": repo,
            "artifact": "registry",
            "data": payload,
        }
    if artifact not in SECTIONS:
        raise FileNotFoundError(
            f"Sezione '{artifact}' non valida (usa una di: registry, " + ", ".join(SECTIONS) + ")"
        )
    data = payload.get(artifact)
    if data is None:
        raise FileNotFoundError(f"Sezione '{artifact}' non presente nel registry di {repo}")

    if slug:
        return {
            "repo": repo,
            "artifact": artifact,
            "slug": slug,
            "entry": _filter_entry(artifact, data, slug),
        }
    return {"repo": repo, "artifact": artifact, "data": data}


def _filter_entry(section: str, data: Any, slug: str) -> dict[str, Any]:
    """Filtra l'entry richiesta dalla sezione (errore se non trovata).

    Ritorna l'entry stessa (senza wrapper ``{slug, entry}``: il livello di
    wrapping è già fornito da ``show_registry``).
    """
    if section in ("datasets", "marts"):
        entries = data or []
        hit = next((d for d in entries if d.get("slug") == slug), None)
        if hit is None:
            raise FileNotFoundError(f"'{slug}' non presente in {section}")
        return hit
    if section == "signals":
        entries = data or []
        hit = next((s for s in entries if s.get("id") == slug), None)
        if hit is None:
            raise FileNotFoundError(f"Signal '{slug}' non presente in signals")
        return hit
    if section == "codelists":
        entries = data or {}
        if slug not in entries:
            raise FileNotFoundError(f"Codelist '{slug}' non presente in codelists")
        return entries[slug]
    if section == "entities":
        entities = (data or {}).get("entities") or {}
        if slug not in entities:
            raise FileNotFoundError(f"Entità '{slug}' non presente in entities")
        return entities[slug]
    raise FileNotFoundError(f"Filtro per slug non supportato per la sezione '{section}'")
