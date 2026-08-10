"""Grafo entità → dataset aggregato cross-repo (fusion ADR).

Aggrega gli artifact ``entities`` dei registry committati nel workspace
(``load_repo_registry``): nodi = entità del mondo reale (Comune, Provincia,
Gara, ...), archi = dataset che le descrivono, bridge = relazioni tra entità.

Consumato dal tool MCP ``toolkit_graph`` (speculare al ``dataset_graph`` del
MCP clean-query, ma cross-repo: clean-query leggeva solo entity_graph.json di
dataset-incubator).

Merge per nome entity: lo stesso concetto (es. ``Comune``) può comparire in
più repo — le entry ``datasets`` vengono concatenate e i conteggi ``types``
sommati. I bridge sono concatenati (dedup).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from toolkit.core.paths import WORKSPACE_ROOT
from toolkit.registry.reader import load_repo_registry

# Domini logici → tipi semantici (per il filtro by_domain del graph).
# Portato dal MCP clean-query (dataset_graph) per parità di comportamento.
DOMAIN_KEYWORDS: dict[str, set[str]] = {
    "appalti": {"cig_code", "ausa_code", "cpv_code"},
    "enti": {"fiscal_code", "ipa_code", "siope_code", "miur_code", "ssn_code", "sogei_code"},
    "territorio": {
        "municipality_code",
        "cadastral_code",
        "municipality_name",
        "province_code",
        "region_code",
        "nuts_code",
    },
    "giustizia": {"ricorso_number"},
    "scuola": {"school_code"},
    "progetti": {"cup_code"},
    "economia": {"ateco_code"},
}


def _entity_section(payload: dict[str, Any] | None) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Estrae (entities, bridges) dal payload registry (fusion o legacy)."""
    if not payload:
        return {}, []
    section = payload.get("entities") or {}
    entities = section.get("entities") or {}
    bridges = section.get("bridges") or []
    return entities, bridges


def load_workspace_graph(workspace: Path = WORKSPACE_ROOT) -> dict[str, Any]:
    """Grafo aggregato cross-repo: entità fuse per nome + bridge concatenati.

    Returns:
        ``{"entities": {name: {...}}, "bridges": [...], "repos": [nomi]}`` —
        ogni entity ha ``datasets`` (da tutti i repo) e ``types`` sommati.
    """
    entities: dict[str, dict[str, Any]] = {}
    bridges: list[dict[str, Any]] = []
    repos: list[str] = []

    if not workspace.is_dir():
        return {"entities": {}, "bridges": [], "repos": []}

    for repo_dir in sorted(p for p in workspace.iterdir() if p.is_dir()):
        payload = load_repo_registry(repo_dir)
        repo_entities, repo_bridges = _entity_section(payload)
        if not repo_entities and not repo_bridges:
            continue
        repos.append(repo_dir.name)

        for name, info in repo_entities.items():
            if name not in entities:
                entities[name] = {
                    "entity": name,
                    "label": info.get("label", name),
                    "datasets": [],
                    "types": {},
                }
            entities[name]["datasets"].extend(info.get("datasets", []) or [])
            for stype, count in (info.get("types") or {}).items():
                entities[name]["types"][stype] = entities[name]["types"].get(stype, 0) + count

        for bridge in repo_bridges:
            if bridge not in bridges:
                bridges.append(bridge)

    for info in entities.values():
        info["datasets"].sort(key=lambda d: d["slug"])

    return {"entities": entities, "bridges": bridges, "repos": repos}


def filter_graph(
    graph: dict[str, Any],
    by_key: str = "",
    by_dataset: str = "",
    by_registry: str = "",
    by_domain: str = "",
) -> dict[str, Any]:
    """Filtra il grafo aggregato per chiave, dataset, entità o dominio.

    Stesso contratto del ``dataset_graph`` di clean-query.

    Returns:
        Dict con ``entities`` (filtrate), ``bridges``, ``summary`` e ``tip``.
        Con ``by_domain`` restituisce ``{"domain", "relations", "count"}``.
    """
    entities = graph.get("entities", {})
    bridges = graph.get("bridges", [])

    # ── Filtra per dominio → solo bridge pertinenti ──
    if by_domain:
        keywords = DOMAIN_KEYWORDS.get(by_domain.lower(), set())
        if not keywords:
            return {
                "error": f"Dominio '{by_domain}' non riconosciuto. "
                f"Disponibili: {', '.join(sorted(DOMAIN_KEYWORDS))}"
            }
        filtered_bridges = [b for b in bridges if any(kw in str(b).lower() for kw in keywords)]
        return {
            "domain": by_domain,
            "relations": filtered_bridges,
            "count": len(filtered_bridges),
        }

    # ── Filtra per entità (by_registry) ──
    if by_registry:
        by_reg_lower = by_registry.lower()
        matched = {
            name: info
            for name, info in entities.items()
            if by_reg_lower in name.lower() or by_reg_lower in info.get("label", "").lower()
        }
        if not matched:
            return {
                "error": f"Entità '{by_registry}' non trovata. "
                f"Disponibili: {sorted(entities.keys())}"
            }
        entities = matched

    # ── Filtra per chiave (by_key) e/o dataset (by_dataset) ──
    if by_key or by_dataset:
        entities_filtered: dict[str, dict[str, Any]] = {}
        by_key_lower = by_key.lower()
        by_ds_lower = by_dataset.lower()
        for entity_name, entity_info in entities.items():
            ds_filtered = []
            for ds in entity_info.get("datasets", []):
                if by_key and by_key_lower not in ds.get("semantic_type", "").lower():
                    continue
                if by_dataset and not (
                    by_ds_lower in ds["slug"].lower() or by_ds_lower in ds.get("name", "").lower()
                ):
                    continue
                ds_filtered.append(ds)
            if ds_filtered:
                filtered_info = dict(entity_info)
                filtered_info["datasets"] = ds_filtered
                entities_filtered[entity_name] = filtered_info
        entities = entities_filtered

    total_datasets = sum(len(e["datasets"]) for e in entities.values())

    return {
        "entities": entities,
        "bridges": bridges,
        "summary": {
            "total_entities": len(entities),
            "total_relations": len(bridges),
            "total_datasets": total_datasets,
        },
        "tip": (
            "Usa by_key='municipality_code' per vedere i dataset con codice ISTAT, "
            "by_dataset='irpef_comunale' per vedere le entità collegate, "
            "by_registry='Comune' per esplorare un'entità, "
            "o by_domain='appalti' per i bridge del dominio appalti."
        ),
    }
