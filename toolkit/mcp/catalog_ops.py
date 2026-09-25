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
    - same_source: dataset con stesso source_id
    - same_category: dataset con stessa category
    - same_tags: dataset che condividono tag
    - same_entities: dataset che condividono semantic_type (via entity graph)
    - joinable: dataset joinabili su colonne condivise
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
            same_tags.append({"slug": ds_slug, "shared_tags": sorted(shared)})

        seen_slugs.add(ds_slug)

    # Entity graph: dataset con stesso semantic_type
    same_entities = []
    joinable = []
    entity_seen = {slug}  # Separate set for entity graph
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

        # Per ogni semantic_type, trova altri dataset (usa lo stesso graph)
        for st in sorted(target_types):
            st_result = filter_graph(graph, by_key=st)
            for entity_info in st_result.get("entities", {}).values():
                for ds in entity_info.get("datasets", []):
                    ds_slug = ds.get("slug", "")
                    if ds_slug != slug and ds_slug not in entity_seen:
                        same_entities.append(
                            {
                                "slug": ds_slug,
                                "semantic_type": st,
                            }
                        )
                        joinable.append(
                            {
                                "slug": ds_slug,
                                "join_column": st,
                            }
                        )
                        entity_seen.add(ds_slug)
    except Exception:
        pass  # Graph non disponibile, skip

    return {
        "slug": slug,
        "same_source": same_source[:10],
        "same_category": same_category[:10],
        "same_tags": same_tags[:10],
        "same_entities": same_entities[:15],
        "joinable": joinable[:10],
    }
