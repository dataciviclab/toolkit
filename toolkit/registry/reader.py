"""Lettore dell'artifact registry committato nei repo del workspace.

Il registry builder (``toolkit.registry.builders``) genera e i repo committano
il file unico ``{repo}/registry/registry.json`` (fusion ADR): sezioni
``datasets``, ``marts``, ``signals``, ``codelists``, ``entities``.

Compatibilità: se il repo non ha ancora migrato (solo i vecchi
clean_catalog/mart_catalog/pipeline_signals/codelists separati), il lettore
cade sui file legacy. Esposizione all'agente (CLI e MCP) via
``registry list`` / ``registry show <repo> <section>``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from toolkit.core.paths import WORKSPACE_ROOT

# Mappa: nome legacy → sezione del registry.json unico (per compatibilità).
LEGACY_TO_SECTION = {
    "clean_catalog": "datasets",
    "mart_catalog": "marts",
    "pipeline_signals": "signals",
    "codelists": "codelists",
    "entity_graph": "entities",
}

# Sezioni esposte (ordine stabile per list).
SECTIONS = ("datasets", "marts", "signals", "codelists", "entities")

EXCLUDED = (".schema.json", "README")


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


def _load_registry(repo_dir: Path) -> dict[str, Any] | None:
    """Carica il registry unico del repo (registry.json), o None se assente."""
    path = repo_dir / "registry" / "registry.json"
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def _load_legacy(repo_dir: Path) -> tuple[dict[str, Any] | None, set[str]]:
    """Fallback: carica i vecchi artifact separati come dict di sezioni.

    Returns:
        (payload sezioni, set delle sezioni effettivamente presenti nei file).
    """
    registry_dir = repo_dir / "registry"
    if not registry_dir.is_dir():
        return None, set()
    payload: dict[str, Any] = {
        "datasets": [],
        "marts": [],
        "signals": [],
        "codelists": {},
        "entities": {},
    }
    found_sections: set[str] = set()
    legacy_files = {
        "clean_catalog": "datasets",
        "mart_catalog": "marts",
        "pipeline_signals": "signals",
        "codelists": "codelists",
        "entity_graph": "entities",
    }
    for fname, section in legacy_files.items():
        path = registry_dir / f"{fname}.json"
        if not path.is_file():
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        if section == "datasets":
            payload["datasets"] = data.get("datasets", [])
        elif section == "marts":
            payload["marts"] = data.get("marts", [])
        elif section == "signals":
            payload["signals"] = data.get("signals", [])
        elif section == "codelists":
            payload["codelists"] = data.get("codelists", {})
        elif section == "entities":
            payload["entities"] = data
        found_sections.add(section)
    if not found_sections:
        return None, set()
    return payload, found_sections


def _scan_repo(repo_dir: Path) -> list[dict[str, Any]]:
    """Artifact del repo: registry unico (con conteggi per sezione)."""
    payload = _load_registry(repo_dir)
    if payload is not None:
        counts = {section: _section_count(section, payload) for section in SECTIONS}
        return [
            {
                "name": "registry",
                "size_bytes": (repo_dir / "registry" / "registry.json").stat().st_size,
                "sections": counts,
            }
        ]

    legacy, _sections = _load_legacy(repo_dir)
    if legacy is not None:
        counts = {section: _section_count(section, legacy) for section in SECTIONS}
        return [{"name": "registry", "size_bytes": None, "sections": counts, "legacy": True}]

    return []


def load_repo_registry(repo_dir: Path) -> tuple[dict[str, Any] | None, bool]:
    """Carica il registry di un repo: formato unico o fallback legacy.

    Returns:
        (payload sezioni, is_legacy). None se il repo non ha registry.
    """
    payload = _load_registry(repo_dir)
    if payload is not None:
        return payload, False
    payload, _sections = _load_legacy(repo_dir)
    if payload is not None:
        return payload, True
    return None, False


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
            Accetta anche i nomi legacy (``clean_catalog`` → ``datasets``).
        slug: Se fornito, filtra l'entry (dataset slug, mart slug, signal id,
            codelist name).

    Raises:
        FileNotFoundError: se il repo/section non esiste nel workspace.
    """
    repo_dir = workspace / repo
    payload = _load_registry(repo_dir)
    legacy_sections: set[str] = set()
    if payload is None:
        payload, legacy_sections = _load_legacy(repo_dir)

    if payload is None:
        raise FileNotFoundError(
            f"Registry non trovato in {workspace}/{repo}/registry/ "
            f"(usa 'toolkit registry list' per i repo disponibili)"
        )

    section = LEGACY_TO_SECTION.get(artifact, artifact)
    if artifact == "registry":
        return {
            "repo": repo,
            "artifact": "registry",
            "legacy": bool(legacy_sections),
            "data": payload,
        }
    if section not in SECTIONS:
        raise FileNotFoundError(
            f"Sezione '{artifact}' non valida (usa una di: registry, " + ", ".join(SECTIONS) + ")"
        )
    if legacy_sections and section not in legacy_sections:
        raise FileNotFoundError(f"Sezione '{section}' non presente nel registry di {repo} (legacy)")
    data = payload.get(section)
    if data is None:
        raise FileNotFoundError(f"Sezione '{section}' non presente nel registry di {repo}")

    if slug:
        return {
            "repo": repo,
            "artifact": section,
            "slug": slug,
            "entry": _filter_entry(section, data, slug),
        }
    return {"repo": repo, "artifact": section, "data": data}


def _filter_entry(section: str, data: Any, slug: str) -> dict[str, Any]:
    """Filtra l'entry richiesta dalla sezione (errore se non trovata)."""
    if section in ("datasets", "marts"):
        entries = data or []
        hit = next((d for d in entries if d.get("slug") == slug), None)
        if hit is None:
            raise FileNotFoundError(f"'{slug}' non presente in {section}")
        return {"slug": slug, "entry": hit}
    if section == "signals":
        entries = data or []
        hit = next((s for s in entries if s.get("id") == slug), None)
        if hit is None:
            raise FileNotFoundError(f"Signal '{slug}' non presente in signals")
        return {"slug": slug, "entry": hit}
    if section == "codelists":
        entries = data or {}
        if slug not in entries:
            raise FileNotFoundError(f"Codelist '{slug}' non presente in codelists")
        return {"slug": slug, "entry": entries[slug]}
    if section == "entities":
        entities = (data or {}).get("entities") or {}
        if slug not in entities:
            raise FileNotFoundError(f"Entità '{slug}' non presente in entities")
        return {"slug": slug, "entry": entities[slug]}
    raise FileNotFoundError(f"Filtro per slug non supportato per la sezione '{section}'")
