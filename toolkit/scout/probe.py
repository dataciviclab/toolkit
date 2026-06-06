"""Routing e orchestrazione URL scout — probe classico e probe arricchito.

Questo modulo NON contiene logica HTTP/fetch pura (sta in toolkit.scout.http).
Contiene solo orchestrazione: probe_url(), probe_url_routed().

Schema a strati:
  toolkit.scout.http      → HTTP transport, format detection, CKAN/SDMX fetch
  toolkit.scout.probe     → orchestrazione probe (QUI)
  toolkit.scout.infer     → inferenze pure (anni, granularità, validation)
  toolkit.scout.scaffold  → scaffold YAML
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

from toolkit.scout.http import (
    DEFAULT_TIMEOUT,
    DEFAULT_USER_AGENT,
    detect_ckan_in_html,
    discover_ckan_resources,
    extract_candidate_links,
    extract_ckan_dataset_id,
    fetch_ckan_package,
    fetch_html_body,
    fetch_sdmx_years,
    is_file_like,
    is_html_content,
    is_sdmx_url,
    is_sparql_endpoint,
    probe_url_headers,
    resolve_preview_kind,
)

__all__ = [
    "probe_url",
    "probe_url_routed",
]

# ---------------------------------------------------------------------------
# Classic probe_url
# ---------------------------------------------------------------------------


def probe_url(
    url: str,
    *,
    timeout: int = DEFAULT_TIMEOUT,
    user_agent: str = DEFAULT_USER_AGENT,
    capture_html: bool = False,
) -> dict[str, Any]:
    """Probe HTTP classico. HEAD + Range fallback, GET body solo per HTML.

    Returns dict: requested_url, final_url, status_code, content_type,
                  content_disposition, kind (file/html/opaque), candidate_links.
    """
    probe = probe_url_headers(url, timeout=timeout, user_agent=user_agent)
    content_type = probe["content_type"]
    content_disposition = probe["content_disposition"]
    final_url = probe["final_url"]

    candidate_links: list[str] = []
    kind: str = "opaque"
    raw_html: bytes = b""

    if is_html_content(content_type):
        try:
            body = fetch_html_body(url, timeout=timeout)
            text = body["html_text"]
            candidate_links = extract_candidate_links(final_url, text)
            kind = "html"
            if capture_html:
                raw_html = text.encode("utf-8", errors="replace")
        except RuntimeError:
            pass
    elif is_file_like(final_url, content_type, content_disposition):
        kind = "file"

    result: dict[str, Any] = {
        "requested_url": url,
        "final_url": final_url,
        "status_code": probe["status_code"],
        "content_type": content_type,
        "content_disposition": content_disposition,
        "kind": kind,
        "candidate_links": candidate_links,
    }
    if capture_html and raw_html:
        result["html_content"] = raw_html
    return result


# ---------------------------------------------------------------------------
# Routed probe (arricchito: rileva CKAN, SDMX, SPARQL, HTML, file)
# ---------------------------------------------------------------------------


def probe_url_routed(
    url: str,
    *,
    timeout: int = DEFAULT_TIMEOUT,
    user_agent: str = DEFAULT_USER_AGENT,
) -> dict[str, Any]:
    """Probe arricchito con routing automatico del tipo fonte.

    Rispetto a probe_url():
    - Rileva CKAN, SDMX, SPARQL, HTML con link, o file diretto
    - Per CKAN: scopre risorse via API
    - Per SDMX: ricava flow e anni
    - Per SPARQL: esegue una probe query leggera
    - Per HTML: estrae link candidati a dati

    Returns dict con source_type, e chiavi specifiche per tipo.
    """
    probe = probe_url_headers(url, timeout=timeout, user_agent=user_agent)
    content_type = probe["content_type"]
    content_disposition = probe["content_disposition"]
    final_url = probe["final_url"]

    result: dict[str, Any] = {
        "requested_url": url,
        "final_url": final_url,
        "status_code": probe["status_code"],
        "content_type": content_type,
        "content_disposition": content_disposition,
        "resolved_format": resolve_preview_kind(url, content_type, content_disposition),
    }

    # SPARQL prima di HTML perche' gli endpoint SPARQL spesso
    # tornano text/html (pagina UI / errore), ma la presenza di
    # "/sparql" nell'URL e' un segnale piu' forte del Content-Type.
    if is_sparql_endpoint(final_url, content_type):
        return _route_sparql(final_url, result, timeout=timeout)
    elif is_html_content(content_type):
        return _route_html(url, final_url, result, timeout=timeout, user_agent=user_agent)
    elif is_sdmx_url(final_url):
        return _route_sdmx(final_url, result, timeout=timeout)
    elif is_file_like(final_url, content_type, content_disposition):
        result["source_type"] = "file"
        result["ckan_resources"] = None
        result["candidate_links"] = []
        result["sdmx_info"] = None
        result["sparql_info"] = None
        return result
    else:
        result["source_type"] = "opaque"
        result["ckan_resources"] = None
        result["candidate_links"] = []
        result["sdmx_info"] = None
        result["sparql_info"] = None
        return result


def _base_result(
    result: dict[str, Any],
    source_type: str,
    *,
    candidate_links: list[str] | None = None,
) -> dict[str, Any]:
    """Aggiunge le chiavi comuni a tutti i source_type."""
    result["source_type"] = source_type
    result.setdefault("ckan_resources", None)
    result["candidate_links"] = (
        candidate_links if candidate_links is not None else result.get("candidate_links", [])
    )
    result.setdefault("sdmx_info", None)
    result.setdefault("sparql_info", None)
    return result


def _route_html(
    url: str, final_url: str, result: dict[str, Any], *, timeout: int, user_agent: str
) -> dict[str, Any]:
    """Route per URL HTML: cerca CKAN o link candidati."""
    try:
        body = fetch_html_body(url, timeout=timeout)
        html_text = body["html_text"]
        html_bytes = html_text.encode("utf-8", errors="replace")
    except RuntimeError:
        return _base_result(result, "html", candidate_links=[])

    candidate_links = extract_candidate_links(final_url, html_text)
    is_ckan = detect_ckan_in_html(html_bytes)
    # Se l'URL contiene /dataset/, probabilmente è CKAN anche senza firme HTML
    if not is_ckan and "/dataset/" in urlparse(final_url).path:
        is_ckan = True

    if is_ckan:
        # Tentativo: se l'URL punta a un dataset specifico, fetcha le risorse
        dataset_id = extract_ckan_dataset_id(final_url, html_text)
        if dataset_id:
            pkg = fetch_ckan_package(final_url, dataset_id, timeout=timeout)
            if pkg:
                resources = discover_ckan_resources(pkg)
                if resources:
                    result["source_type"] = "ckan"
                    result["ckan_resources"] = resources
                    result["candidate_links"] = candidate_links
                    result["sdmx_info"] = None
                    result["sparql_info"] = None
                    result["ckan_dataset_title"] = pkg.get("title") or pkg.get("name") or ""
                    result["ckan_notes"] = (pkg.get("notes") or "")[:500]
                    result["ckan_tags"] = [
                        t.get("display_name") or t.get("name", "")
                        for t in (pkg.get("tags") or [])
                        if isinstance(t, dict)
                    ]
                    return result

        # CKAN rilevato ma nessun dataset specifico — segnala comunque il portale
        result["source_type"] = "ckan"
        result["ckan_resources"] = None
        result["candidate_links"] = candidate_links
        result["sdmx_info"] = None
        result["sparql_info"] = None
        result["ckan_portal"] = True
        return result

    # HTML semplice con link candidati (non CKAN)
    return _base_result(result, "html", candidate_links=candidate_links)


def _route_sdmx(final_url: str, result: dict[str, Any], *, timeout: int) -> dict[str, Any]:
    """Route per URL SDMX."""
    flow_id = None
    parsed_url = urlparse(final_url)
    path = parsed_url.path
    if "/dataflow/" in path:
        parts = path.split("/dataflow/", 1)[1].split("/")
        if len(parts) >= 2:
            flow_id = parts[1]
    elif parsed_url.query:
        from urllib.parse import parse_qs

        qs = parse_qs(parsed_url.query)
        flow_id = next(iter(qs.get("flow") or qs.get("id") or []), None)

    if flow_id:
        year_min, year_max = fetch_sdmx_years(final_url, flow_id, timeout=timeout)
        result["source_type"] = "sdmx"
        result["sdmx_info"] = {"flow_id": flow_id, "year_min": year_min, "year_max": year_max}
    else:
        result["source_type"] = "sdmx"
        result["sdmx_info"] = {"flow_id": None, "year_min": None, "year_max": None}

    result["ckan_resources"] = None
    result["candidate_links"] = []
    result["sparql_info"] = None
    return result


def _route_sparql(final_url: str, result: dict[str, Any], *, timeout: int) -> dict[str, Any]:
    """Route per URL SPARQL: probe + discovery dataset DCAT.

    1. **ASK probe**: ``ASK WHERE {{ ?s ?p ?o }}`` in formato CSV.
    2. **DCAT discovery**: se risponde, esegue una query DCAT per elencare
       i dataset disponibili (titolo, URI, descrizione).

    Se fallisce (timeout, errore, endpoint bloccato), scala a opaco.
    """
    from toolkit.plugins.sparql import SparqlSource

    probe_query = "ASK WHERE { ?s ?p ?o }"
    try:
        source = SparqlSource(timeout=min(timeout, 10))
        source.fetch(final_url, probe_query, accept_format="csv")
    except Exception as exc:
        exc_msg = str(exc)[:200]
        if (
            "timeout" in exc_msg.lower()
            or "refused" in exc_msg.lower()
            or "connect" in exc_msg.lower()
        ):
            result["source_type"] = "opaque"
        else:
            result["source_type"] = "sparql"
        result["sparql_info"] = {
            "endpoint": final_url,
            "responded": "timeout" if "timeout" in exc_msg.lower() else False,
            "error": exc_msg,
        }
        result["ckan_resources"] = None
        result["candidate_links"] = []
        result["sdmx_info"] = None
        return result

    # ── DCAT discovery ─────────────────────────────────────────────────
    dcat_query = """PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dct: <http://purl.org/dc/terms/>
SELECT DISTINCT ?dataset ?title ?description
WHERE {
  ?dataset a dcat:Dataset .
  OPTIONAL { ?dataset dct:title ?title . }
  OPTIONAL { ?dataset dct:description ?description . }
}
ORDER BY ?dataset
LIMIT 100"""
    datasets: list[dict[str, str]] = []
    try:
        dcat_csv, _ = source.fetch(final_url, dcat_query, accept_format="csv")
        import csv
        import io

        reader = csv.DictReader(io.StringIO(dcat_csv.decode("utf-8", errors="replace")))
        for row in reader:
            datasets.append(
                {
                    "uri": row.get("dataset", ""),
                    "title": row.get("title", "") or row.get("dataset", "").rsplit("/", 1)[-1],
                    "description": (row.get("description") or "")[:200],
                }
            )
    except Exception:
        pass  # DCAT non disponibile — non blocca, info parziale

    result["source_type"] = "sparql"
    result["sparql_info"] = {
        "endpoint": final_url,
        "responded": True,
        "datasets": datasets,
        "dataset_count": len(datasets),
    }
    result["ckan_resources"] = None
    result["candidate_links"] = []
    result["sdmx_info"] = None
    return result
