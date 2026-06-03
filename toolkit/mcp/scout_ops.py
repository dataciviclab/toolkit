"""Scout tools: URL probe, inferenza, CKAN, HTML, SPARQL.

Implementazioni thin che chiamano le funzioni di ``toolkit.scout`` e
``toolkit.plugins`` e restituiscono dict serializzabili per MCP.
"""

from __future__ import annotations

from typing import Any

from lab_connectors.http import HttpClient

from toolkit.plugins.sdmx import ISTAT_ESPLORADATI_BASE
from toolkit.plugins.sparql import SparqlSource
from toolkit.scout.http import (
    extract_candidate_links,
    fetch_ckan_package,
    fetch_html_body,
    probe_url_headers,
)
from toolkit.scout.infer import infer_topics
from toolkit.scout.probe import probe_url, probe_url_routed


# ---------------------------------------------------------------------------
# Probe URL
# ---------------------------------------------------------------------------


def mcp_probe_url(url: str, timeout: int = 15) -> dict[str, Any]:
    """Probe HTTP: reachability, content-type, formato, link candidati.

    Chiama ``toolkit.scout.probe.probe_url()``.
    """
    return probe_url(url, timeout=timeout)


def mcp_probe_url_routed(url: str, timeout: int = 15) -> dict[str, Any]:
    """Probe arricchito con routing automatico (CKAN, SDMX, HTML, file).

    Chiama ``toolkit.scout.probe.probe_url_routed()``.
    """
    return probe_url_routed(url, timeout=timeout)


def mcp_probe_url_headers(url: str, timeout: int = 15) -> dict[str, Any]:
    """Probe HTTP leggero: solo HEAD + Range fallback, nessun body.

    Chiama ``toolkit.scout.http.probe_url_headers()``.
    """
    return probe_url_headers(url, timeout=timeout)


# ---------------------------------------------------------------------------
# Topic inference
# ---------------------------------------------------------------------------


def mcp_infer_topic(text: str) -> dict[str, Any]:
    """Inferisce topic tematici da un testo (18 topic).

    Chiama ``toolkit.scout.infer.infer_topics()``.
    """
    return {"topics": infer_topics(text)}


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

    1. Scrive ``fetch_html_body()`` per scaricare la pagina.
    2. ``extract_candidate_links()`` per estrarre i link ai dati.

    Returns:
        Dict con url, total, formats, links, is_reachable.
    """
    try:
        body = fetch_html_body(url, timeout=timeout)
    except RuntimeError as exc:
        return {
            "url": url,
            "is_reachable": False,
            "error": str(exc),
            "links": [],
            "total": 0,
            "formats": {},
        }

    html_text = body.get("html_text", "")
    links = extract_candidate_links(url, html_text)

    formats: dict[str, int] = {}
    for link in links:
        ext = link.rsplit(".", 1)[-1].lower() if "." in link else "unknown"
        formats[ext] = formats.get(ext, 0) + 1

    return {
        "url": url,
        "is_reachable": True,
        "http_status": body.get("status_code"),
        "total": len(links),
        "links": links,
        "formats": formats,
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

    Usa ``toolkit.plugins.sparql.SparqlSource``.

    Returns:
        Dict con results (lista di righe), columns, total_rows, endpoint.

    Raises:
        RuntimeError: se la query fallisce o l'endpoint non risponde.
    """
    source = SparqlSource(timeout=timeout)
    try:
        csv_bytes, _ = source.fetch(endpoint, query, accept_format="csv")
    except Exception as exc:
        return {
            "endpoint": endpoint,
            "error": f"SPARQL query failed: {exc}",
            "results": [],
            "columns": [],
            "total_rows": 0,
        }

    # Parse CSV bytes into dict rows
    import csv
    import io

    text = csv_bytes.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    rows: list[dict[str, str]] = []
    for i, row in enumerate(reader):
        if i >= max_rows:
            break
        rows.append(dict(row))

    columns = reader.fieldnames or []

    return {
        "endpoint": endpoint,
        "columns": columns,
        "total_rows": len(rows),
        "results": rows,
        "truncated": len(rows) >= max_rows,
    }


# ---------------------------------------------------------------------------
# CKAN dataset listing
# ---------------------------------------------------------------------------


def _ckan_action_url(base_url: str, action: str) -> str:
    """Build a CKAN action URL from a portal base URL."""
    base = base_url.rstrip("/")
    if base.endswith("/api/3/action"):
        return f"{base}/{action}"
    return f"{base}/api/3/action/{action}"


def mcp_list_ckan_datasets(
    portal_url: str,
    query: str | None = None,
    rows: int = 100,
    timeout: int = 30,
) -> dict[str, Any]:
    """Elenca i dataset di un portale CKAN via ``package_search``.

    Args:
        portal_url: URL del portale CKAN (es. ``https://www.dati.gov.it/opendata``).
        query: Query testuale di ricerca (Solr). Default ``*:*`` (tutti).
        rows: Numero massimo di dataset da restituire (default 100, max 500).
        timeout: Timeout HTTP in secondi.

    Returns:
        Dict con ``portal_url``, ``count`` (totale disponibile), ``returned``,
        ``datasets`` (lista di dataset con id, name, title, organization, resources_count).
    """
    safe_rows = max(1, min(int(rows or 100), 500))
    search_url = _ckan_action_url(portal_url, "package_search")
    client = HttpClient(timeout=timeout, max_retries=1)

    result = client.get(search_url, params={
        "q": query or "*:*",
        "rows": safe_rows,
    })

    if not result.is_ok or result.response is None:
        return {
            "portal_url": portal_url,
            "error": "CKAN request failed",
            "detail": str(result.err),
        }
    response = result.response
    if response.status_code != 200:
        return {
            "portal_url": portal_url,
            "error": f"HTTP {response.status_code}",
        }

    try:
        data = response.json()
    except Exception as exc:
        return {"portal_url": portal_url, "error": f"Invalid JSON: {exc}"}

    if not data.get("success"):
        return {
            "portal_url": portal_url,
            "error": "CKAN API returned unsuccessful response",
        }

    search_result = data.get("result", {})
    count = search_result.get("count", 0)
    raw_datasets = search_result.get("results", [])

    datasets = []
    for ds in raw_datasets:
        org = ds.get("organization") or {}
        datasets.append({
            "id": ds.get("id") or ds.get("name"),
            "name": ds.get("name") or ds.get("id"),
            "title": ds.get("title"),
            "organization": org.get("title") or org.get("name"),
            "resources_count": len(ds.get("resources") or []),
            "metadata_modified": ds.get("metadata_modified"),
        })

    return {
        "portal_url": portal_url,
        "query": query,
        "count": count,
        "returned": len(datasets),
        "datasets": datasets,
    }


# ---------------------------------------------------------------------------
# SDMX dataflow listing
# ---------------------------------------------------------------------------


def mcp_list_sdmx_dataflows(
    agency: str = "IT1",
    timeout: int = 30,
) -> dict[str, Any]:
    """Elenca i dataflow SDMX disponibili per un'agenzia.

    Args:
        agency: ID dell'agenzia SDMX (default ``IT1`` per ISTAT).
        timeout: Timeout HTTP in secondi.

    Returns:
        Dict con ``agency``, ``returned``, ``dataflows`` (lista con id, name,
        agency_id, version).
    """
    import json

    client = HttpClient(timeout=timeout, max_retries=1)
    dataflow_url = f"{ISTAT_ESPLORADATI_BASE}/dataflow/{agency}/all/latest"

    result = client.get(
        dataflow_url,
        headers={"Accept": "application/vnd.sdmx.structure+json; version=2"},
    )

    if not result.is_ok or result.response is None:
        return {
            "agency": agency,
            "error": "SDMX request failed",
            "detail": str(result.err),
        }

    response = result.response
    if response.status_code != 200:
        return {
            "agency": agency,
            "error": f"HTTP {response.status_code}",
        }

    try:
        payload = json.loads(response.text)
    except Exception as exc:
        return {"agency": agency, "error": f"Invalid JSON: {exc}"}

    flows = payload.get("data", {}).get("dataflows", [])
    dataflows = []
    for flow in flows:
        dataflows.append({
            "dataflow_id": flow.get("id"),
            "name": flow.get("name"),
            "agency_id": flow.get("agencyID"),
            "version": flow.get("version"),
        })

    return {
        "agency": agency,
        "returned": len(dataflows),
        "dataflows": dataflows,
    }
