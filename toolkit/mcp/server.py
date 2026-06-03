"""Toolkit MCP server.

Espone 13 tool read-only per ispezione della pipeline toolkit:
- ispezione standard: inspect_paths, inspect_schema, inspect_profile, summary
- diagnostica: review_readiness, schema_diff, run_summary
- run history: list_runs
- discovery: list_candidates, dataset_info
- preview: csv_preview, clean_preview, raw_preview

Usa ``lab_connectors.mcp`` per init standardizzato, error handling e logging.
"""

from __future__ import annotations

from typing import Any

from lab_connectors.mcp import create_mcp_server, guard_timed

from .toolkit_client import (
    clean_preview as clean_preview_impl,
    csv_preview as csv_preview_impl,
    dataset_info as dataset_info_impl,
    inspect_paths as inspect_paths_impl,
    list_candidates as list_candidates_impl,
    list_runs as list_runs_impl,
    mcp_ckan_package_show as ckan_package_show_impl,
    mcp_html_extract_links as html_extract_links_impl,
    mcp_infer_topic as infer_topic_impl,
    mcp_list_ckan_datasets as list_ckan_datasets_impl,
    mcp_list_sdmx_dataflows as list_sdmx_dataflows_impl,
    mcp_sdmx_dataflow_info as sdmx_dataflow_info_impl,
    mcp_probe_url as probe_url_impl,
    mcp_probe_url_routed as probe_url_routed_impl,
    mcp_sparql_query as sparql_query_impl,
    raw_preview as raw_preview_impl,
    raw_profile as raw_profile_impl,
    review_readiness as review_readiness_impl,
    run_summary as run_summary_impl,
    schema_diff as schema_diff_impl,
    summary as summary_impl,
    show_schema as show_schema_impl,
)

mcp = create_mcp_server(
    name="toolkit",
    instructions=(
        "Server MCP locale, read-only, per ispezionare path risolti, "
        "schemi, stato run e preview dati del toolkit. "
        "Supporta slug dataset (es. 'terna-electricity-by-source') "
        "al posto del path assoluto a dataset.yml."
    ),
)


@mcp.tool(
    description="Mostra il path contract risolto per un dataset config.", structured_output=True
)
def toolkit_inspect_paths(config_path: str, year: int = 0) -> dict[str, Any]:
    return guard_timed(inspect_paths_impl, "toolkit_inspect_paths", config_path, year or None)


@mcp.tool(
    description="Mostra lo schema di raw, clean o mart.",
    structured_output=True,
)
def toolkit_inspect_schema(config_path: str, layer: str = "clean", year: int = 0) -> dict[str, Any]:
    return guard_timed(show_schema_impl, "toolkit_inspect_schema", config_path, layer, year or None)


@mcp.tool(
    description="Mostra il profilo raw: encoding, delimiter, colonne, missingness e mapping suggestions.",
    structured_output=True,
)
def toolkit_inspect_profile(config_path: str, year: int = 0) -> dict[str, Any]:
    return guard_timed(raw_profile_impl, "toolkit_inspect_profile", config_path, year or None)


@mcp.tool(
    description="Statistiche aggregate dei run: totali, successi, fallimenti, durata media.",
    structured_output=True,
)
def toolkit_run_summary(
    config_path: str,
    year: int = 0,
    *,
    since: str | None = None,
    until: str | None = None,
) -> dict[str, Any]:
    return guard_timed(run_summary_impl, "toolkit_run_summary", config_path, year or None, since=since, until=until)


@mcp.tool(
    description="Mostra un riepilogo diagnostico minimo per un dataset config.",
    structured_output=True,
)
def toolkit_summary(config_path: str, year: int = 0) -> dict[str, Any]:
    return guard_timed(summary_impl, "toolkit_summary", config_path, year or None)


@mcp.tool(
    description="Check di readiness per review candidate: config, layer, output e coerenza run record.",
    structured_output=True,
)
def toolkit_review_readiness(config_path: str, year: int = 0) -> dict[str, Any]:
    return guard_timed(review_readiness_impl, "toolkit_review_readiness", config_path, year or None)


@mcp.tool(
    description="Elenca tutti i dataset disponibili in dataset-incubator (candidates e support_datasets). Opzionalmente filtra per last_run_status.",
    structured_output=True,
)
def toolkit_list_candidates(
    stage: str = "all",
    status_filter: str | None = None,
) -> dict[str, Any]:
    return guard_timed(list_candidates_impl, "toolkit_list_candidates", stage, status_filter)


@mcp.tool(
    description="Info di base da un dataset.yml: fonte, URL, anni, tabelle mart, support datasets.",
    structured_output=True,
)
def toolkit_dataset_info(config_path: str) -> dict[str, Any]:
    return guard_timed(dataset_info_impl, "toolkit_dataset_info", config_path)


@mcp.tool(
    description="Preview dei dati puliti (clean parquet) o mart. Mostra schema + prime N righe.",
    structured_output=True,
)
def toolkit_clean_preview(
    config_path: str,
    layer: str = "clean",
    mart_index: int = 0,
    year: int = 0,
    limit: int = 10,
) -> dict[str, Any]:
    return guard_timed(clean_preview_impl, "toolkit_clean_preview", config_path, layer, mart_index, year or None, limit)


@mcp.tool(
    description="Preview del raw file primario (CSV) di un dataset. Wrapper su csv_preview + inspect_paths.",
    structured_output=True,
)
def toolkit_raw_preview(
    config_path: str,
    year: int = 0,
    limit: int = 20,
) -> dict[str, Any]:
    return guard_timed(raw_preview_impl, "toolkit_raw_preview", config_path, year or None, limit)


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
    return guard_timed(list_runs_impl, "toolkit_list_runs", config_path, year or None, since=since, until=until, status=status, limit=limit, cross_year=cross_year)


@mcp.tool(
    description="Legge un CSV usando la stessa pipeline di profile_raw (sniff_source_file + profile_with_read_cfg). "
    "Restituisce schema, preview, mapping_suggestions e parametri sniff (delim, encoding, decimal, skip). "
    "Utile per ispezionare rapidamente il contenuto di un file raw senza runnare la pipeline.",
    structured_output=True,
)
def toolkit_csv_preview(csv_path: str, limit: int = 20) -> dict[str, Any]:
    return guard_timed(csv_preview_impl, "toolkit_csv_preview", csv_path, limit)


# ---------------------------------------------------------------------------
# Scout tools
# ---------------------------------------------------------------------------


@mcp.tool(
    description="Probe HTTP leggero: reachability, status code, content-type, content-disposition. "
    "HEAD + GET Range fallback. Nessun body scaricato.",
    structured_output=True,
)
def toolkit_probe_url(url: str, timeout: int = 15) -> dict[str, Any]:
    return guard_timed(probe_url_impl, "toolkit_probe_url", url, timeout)


@mcp.tool(
    description="Probe HTTP arricchito con routing automatico: rileva CKAN, SDMX, HTML con link dati, "
    "o file diretto. Per CKAN scopre risorse via API, per SDMX ricava flow e anni.",
    structured_output=True,
)
def toolkit_probe_url_routed(url: str, timeout: int = 15) -> dict[str, Any]:
    return guard_timed(probe_url_routed_impl, "toolkit_probe_url_routed", url, timeout)


@mcp.tool(
    description="Inferisce topic tematici da un testo (18 topic: lavoro, economia, sanita, istruzione, "
    "trasporti, ambiente, agricoltura, turismo, giustizia, demografia, energia, ecc.).",
    structured_output=True,
)
def toolkit_infer_topic(text: str) -> dict[str, Any]:
    return guard_timed(infer_topic_impl, "toolkit_infer_topic", text)


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
    return guard_timed(ckan_package_show_impl, "toolkit_ckan_package_show", endpoint, package_id, timeout)


@mcp.tool(
    description="Elenca i dataset di un portale CKAN via API package_search. "
    "Accetta portal_url, query testuale opzionale (Solr), e numero massimo di risultati.",
    structured_output=True,
)
def toolkit_list_ckan_datasets(
    portal_url: str,
    query: str | None = None,
    rows: int = 100,
    timeout: int = 30,
) -> dict[str, Any]:
    return guard_timed(list_ckan_datasets_impl, "toolkit_list_ckan_datasets", portal_url, query, rows, timeout)


@mcp.tool(
    description="Elenca i dataflow SDMX disponibili per un'agenzia SDMX. "
    "Default: IT1 (ISTAT). Restituisce id, nome, agency_id e versione.",
    structured_output=True,
)
def toolkit_list_sdmx_dataflows(
    agency: str = "IT1",
    timeout: int = 30,
) -> dict[str, Any]:
    return guard_timed(list_sdmx_dataflows_impl, "toolkit_list_sdmx_dataflows", agency, timeout)


@mcp.tool(
    description="Restituisce dimensioni e codici validi per un dataflow SDMX "
    "(es. ISTAT). Usa SdmxSource.preview_constraints(). "
    "Utile per capire come comporre i filtri prima di chiamare fetch.",
    structured_output=True,
)
def toolkit_sdmx_dataflow_info(
    dataflow_id: str,
    agency: str = "IT1",
    version: str = "1.0",
    timeout: int = 30,
) -> dict[str, Any]:
    return guard_timed(sdmx_dataflow_info_impl, "toolkit_sdmx_dataflow_info", dataflow_id, agency, version, timeout)


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
    return guard_timed(sparql_query_impl, "toolkit_sparql_query", endpoint, query, timeout, max_rows)


if __name__ == "__main__":
    mcp.run()
