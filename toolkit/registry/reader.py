"""Lettore dell'artifact registry committato nei repo del workspace.

Il registry builder (``toolkit.registry.builders``) genera e i repo committano
il file unico ``{repo}/registry/registry.json`` (schema v2): sezioni
``datasets``, ``marts``, ``checks``, ``codelists``.

Utilizza ``lab_connectors.registry`` per il parsing tipizzato dei registry
(v1 e v2) e per la derivazione delle location.

Esposizione all'agente (CLI e MCP) via ``registry list`` /
``registry show <repo> <section>``. La sezione v1 ``signals`` resta leggibile
come vista sintetizzata (``last_run`` + ``checks``) per non rompere gli agenti
che la chiamano; ``entities`` è stata rimossa dal contratto.
"""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import Any

from lab_connectors.registry.client import load_registry_local, registry_to_dict
from lab_connectors.registry.models import Registry

from toolkit.core.paths import WORKSPACE_ROOT

# Sezioni esposte (ordine stabile per list).
SECTIONS = ("datasets", "marts", "checks", "codelists")

# Sezioni v1 non più prodotte. ``signals`` è una vista sintetizzata;
# ``entities`` è rimossa (nessun consumer, i join sono in dcl-mcp).
_LEGACY_SECTIONS = ("signals", "entities")


def _section_count(section: str, reg: Registry) -> int | None:
    """Conteggio entries di una sezione del registry tipizzato."""
    if section == "datasets":
        return len(reg.datasets)
    if section == "marts":
        return len(reg.marts)
    if section == "checks":
        return len(reg.checks)
    if section == "codelists":
        return len(reg.codelists)
    if section == "signals":
        return len(reg.datasets)  # vista sintetizzata: una voce per dataset
    if section == "entities":
        return 0
    return None


def load_repo_registry(repo_dir: Path) -> dict[str, Any] | None:
    """Carica il registry unico del repo (registry.json), o None se assente.

    Usa ``lab_connectors.registry.client.load_registry_local`` per il parsing
    tipizzato, poi converte in dict per backward compat.
    """
    path = repo_dir / "registry" / "registry.json"
    if not path.is_file():
        return None
    try:
        reg = load_registry_local(path)
        return registry_to_dict(reg)
    except (ValueError, OSError):
        return None


def load_repo_registry_typed(repo_dir: Path) -> Registry | None:
    """Carica il registry tipizzato, o None se assente.

    Versione tipizzata di ``load_repo_registry`` per uso interno.
    """
    path = repo_dir / "registry" / "registry.json"
    if not path.is_file():
        return None
    try:
        return load_registry_local(path)
    except (ValueError, OSError):
        return None


def _scan_repo(repo_dir: Path) -> list[dict[str, Any]]:
    """Artifact del repo: registry unico (con conteggi per sezione)."""
    reg = load_repo_registry_typed(repo_dir)
    if reg is None:
        return []
    counts = {section: _section_count(section, reg) for section in SECTIONS}
    return [
        {
            "name": "registry",
            "size_bytes": (repo_dir / "registry" / "registry.json").stat().st_size,
            "sections": counts,
        }
    ]


def list_registries(workspace: Path = WORKSPACE_ROOT) -> dict[str, Any]:
    """Elenca i registry committati nei repo del workspace.

    Usa ``workspace_repo_dirs()`` per includere repo annidati
    (es. ``incubation/``, ``esperimenti-locali/``).

    Returns:
        Dict con ``repos`` (lista) e ``total_repos``. Ogni repo:
        ``{repo, artifacts: [{name, size_bytes, sections: {...}}]}``.
    """
    from toolkit.registry.layout import workspace_repo_dirs

    repos: list[dict[str, Any]] = []
    if not workspace.is_dir():
        return {"repos": repos, "total_repos": 0}

    for repo_dir in workspace_repo_dirs(workspace):
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
        artifact: Sezione da mostrare (``datasets``, ``marts``,
            ``checks``, ``codelists``, ``signals``) o ``registry`` (intero).
        slug: Se fornito, filtra l'entry (dataset slug, mart slug, check id,
            codelist name).
        workspace: Root del workspace.

    Raises:
        FileNotFoundError: se il repo/section non esiste nel workspace.
    """
    repo_dir = _resolve_repo_dir(repo, workspace)
    reg = load_repo_registry_typed(repo_dir) if repo_dir is not None else None

    if reg is None or repo_dir is None:
        raise FileNotFoundError(
            f"Registry non trovato in {workspace}/{repo}/registry/ "
            f"(usa 'toolkit registry list' per i repo disponibili)"
        )

    if artifact == "registry":
        return {
            "repo": repo,
            "artifact": "registry",
            "data": registry_to_dict(reg),
        }
    if artifact not in SECTIONS and artifact not in _LEGACY_SECTIONS:
        raise FileNotFoundError(
            f"Sezione '{artifact}' non valida (usa una di: registry, "
            + ", ".join(SECTIONS + _LEGACY_SECTIONS)
            + ")"
        )

    data = _get_section_data(reg, artifact)
    if data is None or (isinstance(data, list) and len(data) == 0):
        raise FileNotFoundError(f"Sezione '{artifact}' non presente nel registry di {repo}")

    if slug:
        return {
            "repo": repo,
            "artifact": artifact,
            "slug": slug,
            "entry": _filter_entry(artifact, data, slug),
        }
    return {"repo": repo, "artifact": artifact, "data": data}


def _resolve_repo_dir(repo: str, workspace: Path) -> Path | None:
    """Trova il repo dati per nome, includendo quelli annidati.

    Dopo la riorganizzazione del workspace i repo dati vivono in
    ``{dominio}/{repo}`` (es. ``pubblica-amministrazione/open-siope``): il
    nome del repo è la chiave pubblica (MCP/CLI), il path no.
    """
    direct = workspace / repo
    if (direct / "registry" / "registry.json").is_file():
        return direct
    from toolkit.registry.layout import workspace_repo_dirs

    for repo_dir in workspace_repo_dirs(workspace):
        if repo_dir.name == repo:
            return repo_dir
    return None


def _synthetic_signals(reg: Registry) -> list[dict[str, Any]]:
    """Vista ``signals`` v1 sintetizzata da ``datasets[].last_run`` + ``checks``.

    Serve solo alla superficie CLI/MCP: il file non ha più la sezione. Un
    dataset senza check è ``ok``; il ``run`` riporta l'ultima esecuzione.
    """
    checks = {c.get("id"): c for c in (reg.checks or []) if isinstance(c, dict)}
    signals: list[dict[str, Any]] = []
    for ds in reg.datasets:
        check = checks.get(ds.slug) or {}
        entry: dict[str, Any] = {
            "id": ds.slug,
            "status": check.get("status", "ok"),
            "label": ds.slug,
            "detail": check.get("detail", ""),
            "action": check.get("action", ""),
        }
        if ds.source_id:
            entry["source_id"] = ds.source_id
        if ds.last_run is not None:
            entry["run"] = {
                "id": ds.last_run.run_id,
                "at": ds.last_run.started_at,
                "year": ds.last_run.year,
                "status": ds.last_run.status,
            }
        signals.append(entry)
    return signals


def _get_section_data(reg: Registry, artifact: str) -> Any:
    """Estrae i dati raw di una sezione dal Registry tipizzato."""
    from dataclasses import asdict

    if artifact == "datasets":
        return [asdict(ds) for ds in reg.datasets]
    if artifact == "marts":
        return [asdict(m) for m in reg.marts]
    if artifact == "checks":
        return reg.checks
    if artifact == "codelists":
        return reg.codelists
    if artifact == "signals":
        if reg.signals:
            # Registry v1: la sezione esiste nel file, si restituisce così com'è.
            return [asdict(sig) for sig in reg.signals]
        warnings.warn(
            "La sezione 'signals' non è più nel registry v2: vista sintetizzata da "
            "datasets[].last_run + checks. Usa 'datasets' o 'checks'.",
            DeprecationWarning,
            stacklevel=3,
        )
        return _synthetic_signals(reg)
    if artifact == "entities":
        raise FileNotFoundError(
            "La sezione 'entities' è stata rimossa dal registry v2: i join tra dataset "
            "sono esposti da dcl-mcp (join_suggestions, dai semantic_type delle colonne)."
        )
    return None


def _filter_entry(section: str, data: Any, slug: str) -> dict[str, Any]:
    """Filtra l'entry richiesta dalla sezione (errore se non trovata).

    Ritorna l'entry stessa (senza wrapper ``{slug, entry}``: il livello di
    wrapping è già fornito da ``show_registry``).
    """
    if section in ("datasets", "marts", "signals"):
        entries = data or []
        hit = next((d for d in entries if d.get("slug") == slug or d.get("id") == slug), None)
        if hit is None:
            raise FileNotFoundError(f"'{slug}' non presente in {section}")
        return hit
    if section == "checks":
        entries = data or []
        hit = next((c for c in entries if c.get("id") == slug), None)
        if hit is None:
            raise FileNotFoundError(f"Check '{slug}' non presente in checks")
        return hit
    if section == "codelists":
        entries = data or []
        if slug not in entries:
            raise FileNotFoundError(f"Codelist '{slug}' non presente in codelists")
        return {"name": slug}
    raise FileNotFoundError(f"Filtro per slug non supportato per la sezione '{section}'")
