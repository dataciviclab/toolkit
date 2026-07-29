"""Scout tools — URL probe, CKAN, HTML, SPARQL, preview URL.

Implementazioni thin che chiamano le funzioni di ``toolkit.scout`` e
``toolkit.plugins`` e restituiscono dict serializzabili per MCP.

Nota: mcp_infer_topic, mcp_list_ckan_datasets, mcp_list_sdmx_dataflows
e mcp_probe_url_headers sono state rimosse (mai registrate come tool MCP).
"""

from __future__ import annotations

from typing import Any

from toolkit.scout.http import (
    fetch_ckan_package,
    fetch_html_body,
)
from toolkit.scout.link_extractor import (
    extract_data_links,
    group_links,
)
from toolkit.scout.probe import probe_url, probe_url_routed


# ---------------------------------------------------------------------------
# Probe URL
# ---------------------------------------------------------------------------


def mcp_probe_url(url: str, timeout: int = 15) -> dict[str, Any]:
    """Probe HTTP: reachability, content-type, formato, link candidati.

    Chiama ``toolkit.scout.probe.probe_url()``.
    """
    return probe_url(url, timeout=timeout)


def mcp_probe_url_routed(
    url: str, timeout: int = 15, protocol: str | None = None
) -> dict[str, Any]:
    """Probe arricchito con routing automatico (CKAN, SDMX, HTML, file).

    Chiama ``toolkit.scout.probe.probe_url_routed()``.

    Args:
        url: URL da probeare.
        timeout: Timeout HTTP.
        protocol: Hint protocollo (ckan, sdmx, sparql, html, http).
            Se fornito, salta l'euristica e usa routing deterministico.
    """
    return probe_url_routed(url, timeout=timeout, protocol=protocol)


# ---------------------------------------------------------------------------
# Preview URL (HEAD + Range GET + sniff + DuckDB profile + infer)
# ---------------------------------------------------------------------------


def mcp_preview_url(
    url: str,
    *,
    known_encoding: str | None = None,
    known_delim: str | None = None,
    known_decimal: str | None = None,
    known_skip: int | None = None,
) -> dict[str, Any]:
    """Preview remoto di un URL CSV/TSV: colonne, tipi, granularità, anni.

    Chiama ``toolkit.profile.preview.preview_url()``: HEAD + Range GET
    + sniff + DuckDB profile + infer in un colpo solo.  Solo CSV/TSV.

    Returns:
        Dict con status, reachable, http_status, file_size, resource_format,
        encoding_suggested, delim_suggested, decimal_suggested, skip_suggested,
        columns, col_types, preview_row_count, mapping_suggestions,
        robust_read_suggested, granularity, year_min, year_max.
    """
    from dataclasses import asdict

    from toolkit.profile.preview import preview_url as _preview_url

    result = _preview_url(
        url,
        known_encoding=known_encoding,
        known_delim=known_delim,
        known_decimal=known_decimal,
        known_skip=known_skip,
    )
    return asdict(result)


# ---------------------------------------------------------------------------
# CKAN package_show
# ---------------------------------------------------------------------------


def mcp_ckan_package_show(
    endpoint: str,
    package_id: str,
    timeout: int = 30,
) -> dict[str, Any]:
    """Fetch di un dataset CKAN via API ``package_show``.

    Chiama ``toolkit.scout.http.fetch_ckan_package()``.

    Returns:
        Dict con i metadati del dataset CKAN, o ``{"error": ...}`` se non trovato.
    """
    result = fetch_ckan_package(endpoint, package_id, timeout=timeout)
    if result is None:
        return {
            "error": f"dataset non trovato su {endpoint} con id={package_id}",
            "endpoint": endpoint,
            "package_id": package_id,
        }
    return result


# ---------------------------------------------------------------------------
# HTML extract links
# ---------------------------------------------------------------------------


def mcp_html_extract_links(url: str, timeout: int = 20) -> dict[str, Any]:
    """Estrae link a file dati (CSV, JSON, XLSX, ZIP, XML) da una pagina HTML.

    1. Scarica la pagina via ``fetch_html_body()``.
    2. Usa ``extract_data_links()`` per estrarre link con metadati (formato,
       prefisso, anni).
    3. Usa ``group_links()`` per raggruppare i link in dataset.

    Returns:
        Dict con:
        - ``url``, ``is_reachable``, ``http_status``, ``total``
        - ``data_links`` (list[dict] con url, format, title, prefix, years, page_url)
        - ``groups`` (list[dict] con group_id, prefix, count, year_range, formats)
    """
    try:
        body = fetch_html_body(url, timeout=timeout)
    except RuntimeError as exc:
        return {
            "url": url,
            "is_reachable": False,
            "error": str(exc),
            "total": 0,
            "data_links": [],
            "groups": [],
        }

    html_text = body.get("html_text", "")
    data_links = extract_data_links(url, html_text)
    groups = group_links(data_links)

    return {
        "url": url,
        "is_reachable": True,
        "http_status": body.get("status_code"),
        "total": len(data_links),
        "data_links": [
            {
                "url": dl.url,
                "format": dl.format,
                "title": dl.title,
                "prefix": dl.prefix,
                "years": dl.years,
                "page_url": dl.page_url or url,
            }
            for dl in data_links
        ],
        "groups": [
            {
                "group_id": g.group_id,
                "prefix": g.prefix,
                "count": g.count,
                "year_range": g.year_range,
                "formats": sorted(g.formats),
                "title": g.title,
            }
            for g in groups
        ],
    }


# ---------------------------------------------------------------------------
# SPARQL query
# ---------------------------------------------------------------------------


def mcp_sparql_query(
    endpoint: str,
    query: str,
    timeout: int = 60,
    max_rows: int = 500,
) -> dict[str, Any]:
    """Esegue una query SPARQL SELECT su un endpoint pubblico.

    Usa ``lab_connectors.http.sparql.execute_sparql`` (POST + GET fallback,
    supporta JSON e SPARQL Results XML).

    Returns:
        Dict con results (lista di righe), columns, total_rows, endpoint.

    Raises:
        RuntimeError: se la query fallisce o l'endpoint non risponde.

    """
    from lab_connectors.http.sparql import execute_sparql

    try:
        bindings = execute_sparql(endpoint, query, timeout=timeout)
    except Exception as exc:
        return {
            "endpoint": endpoint,
            "error": f"SPARQL query failed: {exc}",
            "results": [],
            "columns": [],
            "total_rows": 0,
        }

    if not bindings:
        return {
            "endpoint": endpoint,
            "columns": [],
            "total_rows": 0,
            "results": [],
        }

    columns = list(bindings[0].keys())
    rows: list[dict[str, Any]] = []
    for binding in bindings:
        if len(rows) >= max_rows:
            break
        row: dict[str, Any] = {}
        for col in columns:
            entry = binding.get(col)
            row[col] = entry.get("value", "") if isinstance(entry, dict) else ""
        rows.append(row)

    return {
        "endpoint": endpoint,
        "columns": columns,
        "total_rows": len(rows),
        "results": rows,
        "truncated": len(rows) >= max_rows,
    }


# (mcp_list_ckan_datasets e mcp_list_sdmx_dataflows rimosse —
#  mai registrate come MCP tool, nessun consumer)
