"""Toolkit MCP server.

Espone 5 tool aggregati per ispezione dataset, query, pipeline e fonti.

Tool:
- toolkit_dataset: find, overview, status, preflight, schema-diff
- toolkit_query: run (SQL), preview (URL CSV/TSV)
- toolkit_pipeline: contract, runs, registry_list, registry_show, graph
- toolkit_source: probe, ckan, links, sparql
- toolkit_contract: contratti pipeline (backward compat)

Usa ``lab_connectors.mcp`` per init standardizzato, error handling e logging.
"""

from __future__ import annotations

from typing import Any

from lab_connectors.mcp import create_mcp_server, guard_timed
from toolkit.mcp.response import shape

from toolkit.mcp.errors import ToolkitClientError
from lab_connectors.mcp.errors import ErrorCode

from .toolkit_client import (
    list_runs as list_runs_impl,
    mcp_ckan_package_show as ckan_package_show_impl,
    mcp_html_extract_links as html_extract_links_impl,
    mcp_preview_url as preview_url_impl,
    mcp_probe_url as probe_url_impl,
    mcp_probe_url_routed as probe_url_routed_impl,
    mcp_sparql_query as sparql_query_impl,
    schema_diff as schema_diff_impl,
)

from .aggregate_ops import (
    dataset_status as dataset_status_impl,
    layer_query as layer_query_impl,
)

from .catalog_ops import (
    mcp_dataset_overview as dataset_overview_impl,
    mcp_find as find_impl,
)

from .registry_ops import (
    mcp_graph as graph_impl,
    mcp_registry_list as registry_list_impl,
    mcp_registry_show as registry_show_impl,
)


mcp = create_mcp_server(
    name="toolkit",
    instructions=(
        "Toolkit pipeline server — ispeziona dataset, esegue query, "
        "e fornisce contratti per agenti AI.\n\n"
        "4 tool:\n"
        "- toolkit_dataset: find, overview, status, preflight, schema-diff\n"
        "- toolkit_query: run (SQL su raw/clean/mart), preview (URL CSV/TSV)\n"
        "- toolkit_pipeline: contract, runs, registry_list/show, graph\n"
        "- toolkit_source: probe HTTP, CKAN, HTML links, SPARQL\n\n"
        "📌 PRIMA di scrivere clean.sql o mart.sql: chiama "
        "toolkit_pipeline(action='contract', layer='clean') per view name (raw_input), "
        "macro disponibili, regole validazione e formati numerici."
    ),
)


# ---------------------------------------------------------------------------
# toolkit_dataset — find, overview, status, preflight, schema-diff
# ---------------------------------------------------------------------------


@mcp.tool(
    description=(
        "Ispezione dataset: find, overview, status, preflight, schema-diff.\n\n"
        "Actions:\n"
        "- find: cerca dataset per slug/testo/source (params: query, layer, limit, source, stage, status_filter)\n"
        "- overview: schema colonne + conteggio + preview (params: slug, layer, year, source, profile)\n"
        "- status: stato completo dataset (params: config_path, year, since, until)\n"
        "- preflight: diagnostica pre-run (params: config_path, years)\n"
        "- schema-diff: confronto schema raw tra anni (params: config_path)"
    ),
    structured_output=True,
)
def toolkit_dataset(
    action: str,
    # find
    query: str = "",
    layer: str | None = None,
    limit: int = 15,
    source: str = "all",
    stage: str = "all",
    status_filter: str | None = None,
    # overview
    slug: str | None = None,
    year: int | None = None,
    profile: bool = False,
    # status / preflight / schema-diff
    config_path: str | None = None,
    since: str | None = None,
    until: str | None = None,
    years: str | None = None,
) -> dict[str, Any]:
    if action == "find":
        result = guard_timed(
            find_impl,
            "toolkit_dataset_find",
            query=query,
            layer=layer,
            limit=limit,
            source=source,
            stage=stage,
            status_filter=status_filter,
        )
        # Strip nulls da ogni entry del catalogo
        if "datasets" in result:
            result["datasets"] = [shape(ds) for ds in result["datasets"]]
        return result
    if action == "overview":
        if not slug:
            raise ToolkitClientError("overview richiede slug", ErrorCode.INVALID_PARAMS)
        result = guard_timed(
            dataset_overview_impl,
            "toolkit_dataset_overview",
            slug=slug,
            layer=layer or "clean",
            year=year,
            source=source,
            profile=profile,
        )
        return shape(result)
    if action == "status":
        if not config_path:
            raise ToolkitClientError("status richiede config_path", ErrorCode.INVALID_PARAMS)
        result = guard_timed(
            dataset_status_impl,
            "toolkit_dataset_status",
            config_path,
            year=year or 0,
            since=since,
            until=until,
        )
        # Trim: paths_info è sovrapposto con summary
        result.pop("paths_info", None)
        return shape(result)
    if action == "preflight":
        if not config_path:
            raise ToolkitClientError("preflight richiede config_path", ErrorCode.INVALID_PARAMS)
        from toolkit.domain.preflight import run_preflight

        result = guard_timed(
            run_preflight, "toolkit_dataset_preflight", config_path, years_arg=years
        )
        # Trim: strip nulls da ogni source entry
        if "sources" in result:
            result["sources"] = [shape(s) for s in result["sources"]]
        return shape(result)
    if action == "schema-diff":
        if not config_path:
            raise ToolkitClientError("schema-diff richiede config_path", ErrorCode.INVALID_PARAMS)
        return guard_timed(schema_diff_impl, "toolkit_dataset_schema_diff", config_path)
    raise ToolkitClientError(
        f"Azione '{action}' non valida. Usare: find, overview, status, preflight, schema-diff",
        ErrorCode.INVALID_PARAMS,
    )


# ---------------------------------------------------------------------------
# toolkit_query — run SQL, preview URL
# ---------------------------------------------------------------------------


@mcp.tool(
    description=(
        "Query dati: SQL su dataset (raw/clean/mart) o preview URL CSV/TSV.\n\n"
        "Actions:\n"
        "- run: esegui SQL su uno o piu' dataset\n"
        "  Params: datasets (list[str]), sql, layer, mode, year, limit, dry_run, config_path, mart_index, table\n"
        "  Catalog mode: datasets=[slug1, slug2], sql usa gli slug come tabelle\n"
        "  Pipeline mode: config_path, sql usa 'data' come tabella\n"
        "- preview: preview remoto CSV/TSV (params: url, known_encoding, known_delim, known_decimal, known_skip)"
    ),
    structured_output=True,
)
def toolkit_query(
    action: str,
    # run params
    datasets: list[str] | None = None,
    sql: str | None = None,
    layer: str = "clean",
    mode: str = "sql",
    year: int = 0,
    limit: int = 20,
    dry_run: bool = False,
    config_path: str | None = None,
    mart_index: int = 0,
    table: str | None = None,
    # preview params
    url: str | None = None,
    known_encoding: str | None = None,
    known_delim: str | None = None,
    known_decimal: str | None = None,
    known_skip: int | None = None,
) -> dict[str, Any]:
    if action == "run":
        needs_sql = mode == "sql"
        if needs_sql and not sql:
            raise ToolkitClientError("run con mode=sql richiede sql", ErrorCode.INVALID_PARAMS)
        if not datasets and not config_path:
            raise ToolkitClientError(
                "run richiede datasets o config_path", ErrorCode.INVALID_PARAMS
            )
        return guard_timed(
            layer_query_impl,
            "toolkit_query_run",
            config_path=config_path,
            datasets=datasets,
            layer=layer,
            mode=mode,
            year=year or None,
            limit=limit,
            sql=sql,
            mart_index=mart_index,
            table=table,
            dry_run=dry_run,
        )
    if action == "preview":
        if not url:
            raise ToolkitClientError("preview richiede url", ErrorCode.INVALID_PARAMS)
        result = guard_timed(
            preview_url_impl,
            "toolkit_query_preview",
            url,
            known_encoding=known_encoding,
            known_delim=known_delim,
            known_decimal=known_decimal,
            known_skip=known_skip,
        )
        return shape(result)
    raise ToolkitClientError(
        f"Azione '{action}' non valida. Usare: run, preview",
        ErrorCode.INVALID_PARAMS,
    )


# ---------------------------------------------------------------------------
# toolkit_pipeline — contract, runs, registry, graph
# ---------------------------------------------------------------------------


@mcp.tool(
    description=(
        "Pipeline toolkit: contratti, run history, registry e grafo relazioni.\n\n"
        "Actions:\n"
        "- contract: contratti pipeline per layer (params: layer)\n"
        "- runs: lista run records (params: config_path, year, since, until, status, limit, cross_year)\n"
        "- registry_list: elenca artifact registry committati\n"
        "- registry_show: mostra artifact registry (params: repo, artifact, slug)\n"
        "- graph: mappa relazioni tra dataset (params: by_key, by_dataset, by_registry, by_domain)"
    ),
    structured_output=True,
)
def toolkit_pipeline(
    action: str,
    # contract
    layer: str = "all",
    # runs
    config_path: str | None = None,
    year: int = 0,
    since: str | None = None,
    until: str | None = None,
    status: str | None = None,
    limit: int | None = None,
    cross_year: bool = False,
    # registry_show
    repo: str | None = None,
    artifact: str | None = None,
    slug: str | None = None,
    # graph
    by_key: str = "",
    by_dataset: str = "",
    by_registry: str = "",
    by_domain: str = "",
) -> dict[str, Any]:
    if action == "contract":
        from toolkit.contracts.pipeline import CONTRACTS

        if layer == "all":
            return CONTRACTS
        if layer in CONTRACTS:
            return {"layer": layer, **CONTRACTS[layer]}
        return CONTRACTS
    if action == "runs":
        if not config_path:
            raise ToolkitClientError("runs richiede config_path", ErrorCode.INVALID_PARAMS)
        return guard_timed(
            list_runs_impl,
            "toolkit_pipeline_runs",
            config_path,
            year or None,
            since=since,
            until=until,
            status=status,
            limit=limit,
            cross_year=cross_year,
        )
    if action == "registry_list":
        return guard_timed(registry_list_impl, "toolkit_pipeline_registry_list")
    if action == "registry_show":
        if not repo or not artifact:
            raise ToolkitClientError(
                "registry_show richiede repo e artifact", ErrorCode.INVALID_PARAMS
            )
        return guard_timed(
            registry_show_impl, "toolkit_pipeline_registry_show", repo, artifact, slug
        )
    if action == "graph":
        return guard_timed(
            graph_impl,
            "toolkit_pipeline_graph",
            by_key=by_key,
            by_dataset=by_dataset,
            by_registry=by_registry,
            by_domain=by_domain,
        )
    raise ToolkitClientError(
        f"Azione '{action}' non valida. Usare: contract, runs, registry_list, registry_show, graph",
        ErrorCode.INVALID_PARAMS,
    )


# ---------------------------------------------------------------------------
# toolkit_source — probe, ckan, links, sparql
# ---------------------------------------------------------------------------


@mcp.tool(
    description=(
        "Fonti dati esterne: probe HTTP, CKAN, HTML links, SPARQL.\n\n"
        "Actions:\n"
        "- probe: reachability HTTP (params: url, timeout, routed)\n"
        "- ckan: fetch dataset CKAN (params: endpoint, package_id, timeout)\n"
        "- links: estrai link dati da pagina HTML (params: url, timeout)\n"
        "- sparql: query SPARQL SELECT (params: endpoint, query, timeout, max_rows)"
    ),
    structured_output=True,
)
def toolkit_source(
    action: str,
    url: str | None = None,
    endpoint: str | None = None,
    package_id: str | None = None,
    query: str | None = None,
    timeout: int = 30,
    routed: bool = False,
    max_rows: int = 500,
) -> dict[str, Any]:
    if action == "probe":
        if not url:
            raise ToolkitClientError("probe richiede url", ErrorCode.INVALID_PARAMS)
        impl = probe_url_routed_impl if routed else probe_url_impl
        name = "toolkit_source_probe"
        if routed:
            return shape(guard_timed(impl, f"{name}_routed", url, timeout))
        return shape(guard_timed(impl, name, url, timeout))
    if action == "ckan":
        if not endpoint or not package_id:
            raise ToolkitClientError(
                "ckan richiede endpoint e package_id", ErrorCode.INVALID_PARAMS
            )
        return guard_timed(
            ckan_package_show_impl, "toolkit_source_ckan", endpoint, package_id, timeout
        )
    if action == "links":
        if not url:
            raise ToolkitClientError("links richiede url", ErrorCode.INVALID_PARAMS)
        return guard_timed(html_extract_links_impl, "toolkit_source_links", url, timeout)
    if action == "sparql":
        if not endpoint or not query:
            raise ToolkitClientError("sparql richiede endpoint e query", ErrorCode.INVALID_PARAMS)
        return guard_timed(
            sparql_query_impl, "toolkit_source_sparql", endpoint, query, timeout, max_rows
        )
    raise ToolkitClientError(
        f"Azione '{action}' non valida. Usare: probe, ckan, links, sparql",
        ErrorCode.INVALID_PARAMS,
    )


if __name__ == "__main__":
    mcp.run()
