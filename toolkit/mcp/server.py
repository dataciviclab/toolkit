"""Toolkit MCP server.

Espone tool read-only per ispezione della pipeline toolkit.
Include sia tool granulari (backward compat) che tool aggregati.

Tool aggregati:
- toolkit_layer: schema/preview/profile/sql su RAW/CLEAN/MART in un tool
- toolkit_status: paths + summary + readiness + run_stats + info in un tool

Tool catalogo (nuovi, basati su GCS manifest):
- toolkit_find: cerca dataset pubblicati su GCS per slug/layer
- toolkit_dataset_overview: schema + conteggio + preview da slug

Tool granulari:
- schema_diff, list_runs
- scout: probe_url, ckan, sparql, html, preview_url

Usa ``lab_connectors.mcp`` per init standardizzato, error handling e logging.
"""

from __future__ import annotations

from typing import Any

from lab_connectors.mcp import create_mcp_server, guard_timed

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


mcp = create_mcp_server(
    name="toolkit",
    instructions=(
        "Toolkit pipeline server — ispeziona dataset, esegue preview, "
        "e fornisce contratti per agenti AI.\n\n"
        "📌 **PRIMA di scrivere clean.sql o mart.sql**: chiama "
        "toolkit_contract(layer='clean') per view name (raw_input), "
        "macro disponibili, regole validazione e formati numerici.\n"
        "Chiama toolkit_contract(layer='mart') per la view mart (clean_input).\n\n"
        "Supporta slug dataset (es. 'terna-electricity-by-source') "
        "al posto del path assoluto a dataset.yml."
    ),
)


@mcp.tool(
    description="Confronta i segnali di schema raw (encoding, colonne, ecc.) tra gli anni configurati per un dataset.",
    structured_output=True,
)
def toolkit_schema_diff(config_path: str) -> dict[str, Any]:
    return guard_timed(schema_diff_impl, "toolkit_schema_diff", config_path)


@mcp.tool(
    description="Lista run records con filtri opzionali. Ritorna record completi (non solo metadata).",
    structured_output=True,
)
def toolkit_list_runs(
    config_path: str,
    year: int = 0,
    *,
    since: str | None = None,
    until: str | None = None,
    status: str | None = None,
    limit: int | None = None,
    cross_year: bool = False,
) -> dict[str, Any]:
    return guard_timed(
        list_runs_impl,
        "toolkit_list_runs",
        config_path,
        year or None,
        since=since,
        until=until,
        status=status,
        limit=limit,
        cross_year=cross_year,
    )


@mcp.tool(
    description="Preview remoto di un URL CSV/TSV: colonne, tipi, granularità. "
    "HEAD + Range GET + sniff + DuckDB profile. Solo CSV/TSV.",
    structured_output=True,
)
def toolkit_preview_url(
    url: str,
    known_encoding: str | None = None,
    known_delim: str | None = None,
    known_decimal: str | None = None,
    known_skip: int | None = None,
) -> dict[str, Any]:
    return guard_timed(
        preview_url_impl,
        "toolkit_preview_url",
        url,
        known_encoding=known_encoding,
        known_delim=known_delim,
        known_decimal=known_decimal,
        known_skip=known_skip,
    )


# ---------------------------------------------------------------------------
# Aggregated tools
# ---------------------------------------------------------------------------


@mcp.tool(
    description="Query unificata su RAW/CLEAN/MART: schema, preview, profilo o SQL. "
    "Due modalita': config_path (pipeline locale) o datasets (catalogo GCS/workspace). "
    "mode=sql funziona su tutti i layer (raw->CSV, clean/mart->parquet). "
    "Per layer=mart, table specifica la tabella (es. 'mart_top_sa'). "
    "Catalog mode (datasets) supporta solo mode='sql'. "
    "Esempi: mode=sql, datasets=['anac_bandi_gara', 'popolazione_istat']",
    structured_output=True,
)
def toolkit_layer(
    config_path: str | None = None,
    datasets: list[str] | None = None,
    layer: str = "clean",
    mode: str = "schema",
    year: int = 0,
    limit: int = 20,
    sql: str | None = None,
    mart_index: int = 0,
    table: str | None = None,
) -> dict[str, Any]:
    return guard_timed(
        layer_query_impl,
        "toolkit_layer",
        config_path=config_path,
        datasets=datasets,
        layer=layer,
        mode=mode,
        year=year or None,
        limit=limit,
        sql=sql,
        mart_index=mart_index,
        table=table,
    )


@mcp.tool(
    description="Stato completo di un dataset: paths + summary + readiness + run_stats + info. "
    "Aggrega inspect_paths, summary, review_readiness, run_summary e dataset_info "
    "in una unica chiamata. I parametri since/until filtrano i run per finestra temporale.",
    structured_output=True,
)
def toolkit_status(
    config_path: str,
    year: int = 0,
    *,
    since: str | None = None,
    until: str | None = None,
) -> dict[str, Any]:
    return guard_timed(
        dataset_status_impl,
        "toolkit_status",
        config_path,
        year=year or None,
        since=since,
        until=until,
    )


# ---------------------------------------------------------------------------
# Catalog tools (basati su GCS manifest)
# ---------------------------------------------------------------------------


@mcp.tool(
    description="Cerca dataset per slug, source, layer (clean/mart) o testo. "
    "source='gcs' = pubblicati (da gcs_manifest.json), "
    "source='workspace' = in sviluppo (da dataset.yml + parquet locali), "
    "source='all' (default) = unione. "
    "Filtri aggiuntivi: stage (candidates/support), status_filter (SUCCESS/FAILED/DRY_RUN). "
    "Restituisce slug, layer, anni, file count, size, run_status, flag source.",
    structured_output=True,
)
def toolkit_find(
    query: str = "",
    layer: str | None = None,
    limit: int = 15,
    source: str = "all",
    stage: str = "all",
    status_filter: str | None = None,
) -> dict[str, Any]:
    return guard_timed(
        find_impl,
        "toolkit_find",
        query=query,
        layer=layer,
        limit=limit,
        source=source,
        stage=stage,
        status_filter=status_filter,
    )


@mcp.tool(
    description="Overview di un dataset: schema colonne (DESCRIBE DuckDB), "
    "conteggio righe e preview dati. "
    "Parametro source='gcs' (solo pubblicati), 'workspace' (solo sviluppo), "
    "'all' (default) — entrambi, preferisce locale.",
    structured_output=True,
)
def toolkit_dataset_overview(
    slug: str,
    layer: str = "clean",
    year: int | None = None,
    source: str = "all",
) -> dict[str, Any]:
    return guard_timed(
        dataset_overview_impl,
        "toolkit_dataset_overview",
        slug=slug,
        layer=layer,
        year=year,
        source=source,
    )


# ---------------------------------------------------------------------------
# Pipeline contracts (AI agent interface)
# ---------------------------------------------------------------------------


@mcp.tool(
    description="Restituisce i contratti di pipeline del toolkit in formato "
    "strutturato. Usalo PRIMA di scrivere dataset.yml, clean.sql o mart.sql "
    "per conoscere tipi fonte raw, view names (raw_input, clean_input), "
    "macro disponibili, regole di validazione, e formati numerici italiani. "
    "Parametro layer='raw' | 'clean' | 'mart' | 'all' (default).",
    structured_output=True,
)
def toolkit_contract(layer: str = "all") -> dict[str, object]:
    from toolkit.contracts.pipeline import CONTRACTS

    if layer == "all":
        return CONTRACTS
    if layer in CONTRACTS:
        return {"layer": layer, **CONTRACTS[layer]}  # type: ignore[misc]
    return CONTRACTS


# ---------------------------------------------------------------------------
# Validate config
# ---------------------------------------------------------------------------


@mcp.tool(
    description="Pre-flight check per un dataset: valida config, verifica "
    "raggiungibilita' fonti, e per CSV produce quality score PA. "
    "Non esegue la pipeline — solo diagnostica preventiva.",
    structured_output=True,
)
def toolkit_preflight(config_path: str, years: str | None = None) -> dict[str, Any]:
    from toolkit.domain.preflight import run_preflight

    return guard_timed(run_preflight, "toolkit_preflight", config_path, years_arg=years)


# ---------------------------------------------------------------------------
# Scout tools
# ---------------------------------------------------------------------------


@mcp.tool(
    description="Probe HTTP: reachability, status code, content-type. "
    "HEAD + GET Range. Nessun body scaricato. "
    "Con routed=True attiva routing automatico (rileva CKAN, SDMX, HTML, file diretto).",
    structured_output=True,
)
def toolkit_probe_url(url: str, timeout: int = 15, routed: bool = False) -> dict[str, Any]:
    impl = probe_url_routed_impl if routed else probe_url_impl
    name = "toolkit_probe_url"
    if routed:
        return guard_timed(impl, f"{name}_routed", url, timeout)
    return guard_timed(impl, name, url, timeout)


@mcp.tool(
    description="Fetch di un dataset CKAN via API package_show. "
    "Restituisce metadati, risorse, organization, tags, formato e DataStore availability.",
    structured_output=True,
)
def toolkit_ckan_package_show(
    endpoint: str,
    package_id: str,
    timeout: int = 30,
) -> dict[str, Any]:
    return guard_timed(
        ckan_package_show_impl, "toolkit_ckan_package_show", endpoint, package_id, timeout
    )


@mcp.tool(
    description="Estrae link a file dati (CSV, JSON, XLSX, ZIP, XML) da una pagina HTML. "
    "Scarica la pagina, analizza i link, e restituisce URL trovati raggruppati per formato.",
    structured_output=True,
)
def toolkit_html_extract_links(url: str, timeout: int = 20) -> dict[str, Any]:
    return guard_timed(html_extract_links_impl, "toolkit_html_extract_links", url, timeout)


@mcp.tool(
    description="Esegue una query SPARQL SELECT su un endpoint pubblico. "
    "Restituisce risultati in formato tabellare (lista di righe con colonne). "
    "Supporta qualsiasi endpoint HTTPS SPARQL.",
    structured_output=True,
)
def toolkit_sparql_query(
    endpoint: str, query: str, timeout: int = 60, max_rows: int = 500
) -> dict[str, Any]:
    return guard_timed(
        sparql_query_impl, "toolkit_sparql_query", endpoint, query, timeout, max_rows
    )


if __name__ == "__main__":
    mcp.run()
