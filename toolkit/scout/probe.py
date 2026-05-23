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
# Routed probe (arricchito: rileva CKAN, SDMX, HTML, file)
# ---------------------------------------------------------------------------


def probe_url_routed(
    url: str,
    *,
    timeout: int = DEFAULT_TIMEOUT,
    user_agent: str = DEFAULT_USER_AGENT,
) -> dict[str, Any]:
    """Probe arricchito con routing automatico del tipo fonte.

    Rispetto a probe_url():
    - Rileva CKAN, SDMX, HTML con link, o file diretto
    - Per CKAN: scopre risorse via API
    - Per SDMX: ricava flow e anni
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

    if is_html_content(content_type):
        return _route_html(url, final_url, result, timeout=timeout, user_agent=user_agent)
    elif is_sdmx_url(final_url):
        return _route_sdmx(final_url, result, timeout=timeout)
    elif is_file_like(final_url, content_type, content_disposition):
        result["source_type"] = "file"
        result["ckan_resources"] = None
        result["candidate_links"] = []
        result["sdmx_info"] = None
        return result
    else:
        result["source_type"] = "opaque"
        result["ckan_resources"] = None
        result["candidate_links"] = []
        result["sdmx_info"] = None
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
        result["source_type"] = "html"
        result["ckan_resources"] = None
        result["candidate_links"] = []
        result["sdmx_info"] = None
        return result

    candidate_links = extract_candidate_links(final_url, html_text)
    is_ckan = detect_ckan_in_html(html_bytes)

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
        result["ckan_portal"] = True
        return result

    # HTML semplice con link candidati (non CKAN)
    result["source_type"] = "html"
    result["ckan_resources"] = None
    result["candidate_links"] = candidate_links
    result["sdmx_info"] = None
    return result


def _route_sdmx(
    final_url: str, result: dict[str, Any], *, timeout: int
) -> dict[str, Any]:
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
    return result
