"""MCP wrapper per catalog ops.

Chiama il backend condiviso ``toolkit.cli.catalog_ops`` e traduce
eccezioni in ``ToolkitClientError`` per il server MCP.
"""

from __future__ import annotations

from typing import Any

from lab_connectors.mcp.errors import ErrorCode

from toolkit.domain.catalog import CatalogResolver
from toolkit.mcp.errors import ToolkitClientError

_resolver: CatalogResolver | None = None


def _get_resolver() -> CatalogResolver:
    global _resolver
    if _resolver is None:
        _resolver = CatalogResolver()
    return _resolver


def reset_resolver() -> None:
    """Resetta il resolver (utile nei test)."""
    global _resolver
    _resolver = None


def mcp_find(
    query: str = "",
    layer: str | None = None,
    limit: int = 15,
    source: str = "all",
    stage: str = "all",
    status_filter: str | None = None,
    metric_only: bool = False,
) -> dict[str, Any]:
    """Cerca dataset nel manifest GCS e/o workspace locale."""
    try:
        resolver = _get_resolver()
        return resolver.list_datasets(
            query=query,
            layer=layer,
            limit=limit,
            source=source,
            stage=stage,
            status_filter=status_filter,
            metric_only=metric_only,
        )
    except (FileNotFoundError, TimeoutError) as exc:
        raise ToolkitClientError(
            f"Manifest GCS non raggiungibile: {exc}",
            code=ErrorCode.GCS_UNAVAILABLE,
        ) from exc
    except ValueError as exc:
        raise ToolkitClientError(
            f"Errori parametri: {exc}",
            code=ErrorCode.INVALID_PARAMS,
        ) from exc


def mcp_dataset_overview(
    slug: str,
    layer: str = "clean",
    year: int | None = None,
    source: str = "all",
    profile: bool = False,
) -> dict[str, Any]:
    """Overview di un dataset: schema, row count e preview."""
    try:
        resolver = _get_resolver()
        return resolver.describe_slug(slug, layer=layer, year=year, source=source, profile=profile)
    except FileNotFoundError as exc:
        raise ToolkitClientError(
            str(exc),
            code=ErrorCode.PARQUET_NOT_FOUND,
        ) from exc
    except RuntimeError as exc:
        raise ToolkitClientError(
            str(exc),
            code=ErrorCode.UNEXPECTED,
        ) from exc


def mcp_dataset_related(slug: str) -> dict[str, Any]:
    """Trova dataset correlati a uno slug.

    Restituisce:
    - summary: frase sintetica dei risultati
    - same_source: dataset con stesso source_id (max 5)
    - same_category: dataset con stessa category (max 5)
    - same_tags: dataset che condividono tag, ordinati per tag sharing (max 5)
    - joinable: dataset joinabili, ordinati per numero di colonne condivise (max 5)
    - best_join: dataset migliore per join (più colonne in comune)
    """
    resolver = _get_resolver()

    # Trova il dataset target (cerca per slug esatto)
    target = None
    try:
        result = resolver.list_datasets(query=slug, source="all", limit=50)
    except Exception as exc:
        raise ToolkitClientError(
            f"Errore ricerca dataset: {exc}",
            code=ErrorCode.UNEXPECTED,
        ) from exc

    for ds in result.get("datasets", []):
        if ds.get("slug") == slug:
            target = ds
            break

    if target is None:
        raise ToolkitClientError(
            f"Dataset '{slug}' non trovato",
            code=ErrorCode.CONFIG_NOT_FOUND,
        )

    target_tags = set(target.get("tags") or [])
    target_category = target.get("category", "")
    target_source = target.get("source_id") or ""

    # Cerca tutti i dataset per trovare correlati
    all_result = resolver.list_datasets(query="", source="all", limit=500)

    same_source = []
    same_category = []
    same_tags = []
    seen_slugs = {slug}

    for ds in all_result.get("datasets", []):
        ds_slug = ds.get("slug", "")
        if ds_slug in seen_slugs:
            continue

        # Same source
        ds_source = ds.get("source_id") or ""
        if target_source and ds_source and ds_source == target_source:
            same_source.append(ds_slug)

        # Same category
        ds_category = ds.get("category", "")
        if target_category and ds_category and ds_category == target_category:
            same_category.append(ds_slug)

        # Same tags
        ds_tags = set(ds.get("tags") or [])
        shared = target_tags & ds_tags
        if shared:
            same_tags.append({"slug": ds_slug, "shared_tags": sorted(shared), "score": len(shared)})

        seen_slugs.add(ds_slug)

    # Ordina same_tags per score decrescente
    same_tags.sort(key=lambda x: x["score"], reverse=True)

    # Entity graph: dataset con stesso semantic_type
    joinable_map: dict[str, dict[str, Any]] = {}  # slug → {columns: [], score: int}
    try:
        from toolkit.registry.graph import load_workspace_graph, filter_graph

        graph = load_workspace_graph()
        graph_result = filter_graph(graph, by_dataset=slug)

        # Trova tutti gli semantic_type del dataset target
        target_types = set()
        for entity_info in graph_result.get("entities", {}).values():
            for ds in entity_info.get("datasets", []):
                if ds.get("slug") == slug:
                    st = ds.get("semantic_type", "")
                    if st:
                        target_types.add(st)

        # Per ogni semantic_type, trova altri dataset
        for st in sorted(target_types):
            st_result = filter_graph(graph, by_key=st)
            for entity_info in st_result.get("entities", {}).values():
                for ds in entity_info.get("datasets", []):
                    ds_slug = ds.get("slug", "")
                    if ds_slug != slug:
                        if ds_slug not in joinable_map:
                            joinable_map[ds_slug] = {"columns": [], "score": 0}
                        joinable_map[ds_slug]["columns"].append(st)
                        joinable_map[ds_slug]["score"] += 1
    except Exception:
        pass  # Graph non disponibile, skip

    # Ordina joinable per score decrescente (più colonne in comune = migliore)
    joinable_sorted = sorted(joinable_map.items(), key=lambda x: x[1]["score"], reverse=True)
    joinable = [
        {"slug": s, "join_columns": info["columns"], "score": info["score"]}
        for s, info in joinable_sorted
    ]

    # Best join: il dataset con più colonne in comune
    best_join = joinable[0] if joinable else None

    # Summary sintetico
    parts = []
    if same_source:
        parts.append(f"{len(same_source)} from same source")
    if same_category:
        parts.append(f"{len(same_category)} same category")
    if same_tags:
        parts.append(f"{len(same_tags)} share tags")
    if joinable:
        parts.append(
            f"{len(joinable)} joinable ({best_join['score']} cols)"
            if best_join
            else f"{len(joinable)} joinable"
        )
    summary = f"{slug}: {', '.join(parts)}" if parts else f"{slug}: no related datasets found"

    return {
        "slug": slug,
        "summary": summary,
        "same_source": same_source[:5],
        "same_category": same_category[:5],
        "same_tags": [{"slug": t["slug"], "shared_tags": t["shared_tags"]} for t in same_tags[:5]],
        "joinable": joinable[:5],
        "best_join": best_join,
    }
