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

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urljoin, urlparse

from lab_connectors.http import HttpClient
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
# Protocol-based routing (dispatcher dedicati per protocollo)
# ---------------------------------------------------------------------------
# probe_url_routed(), quando riceve un hint protocol, usa questo dict per
# instradare al dispatcher appropriato invece di fare auto-detect.
# I dispatcher sono le funzioni _route_* definite piu' avanti.
_PROTOCOL_ROUTER: dict[str, str] = {
    "http": "file",
    "ckan": "ckan",
    "sdmx": "sdmx",
    "sparql": "sparql",
    "html": "html",
}

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
    protocol: str | None = None,
) -> dict[str, Any]:
    """Probe arricchito con routing automatico del tipo fonte.

    Rispetto a probe_url():
    - Rileva CKAN, SDMX, SPARQL, HTML con link, o file diretto
    - Per CKAN: scopre risorse via API
    - Per SDMX: ricava flow e anni
    - Per SPARQL: esegue una probe query leggera
    - Per HTML: estrae link candidati a dati

    Args:
        url: URL da probeare.
        timeout: Timeout HTTP.
        user_agent: User-Agent.
        protocol: Hint protocollo dalla registry (ckan, sdmx, sparql, html, http).
            Se fornito e noto a _PROTOCOL_ROUTER, salta l'euristica e usa
            il routing deterministico.

    Returns dict con source_type, e chiavi specifiche per tipo.
    """
    # Hint protocol: dispatcher deterministico per protocollo.
    # SDMX e SPARQL saltano la HEAD probe (i loro endpoint non
    # rispondono a HEAD — 405 Method Not Allowed) e vanno direttamente
    # al router dedicato che usa GET con header corretti.
    if protocol in _PROTOCOL_ROUTER:
        mapped = _PROTOCOL_ROUTER[protocol]
        if mapped in ("sdmx", "sparql"):
            # SDMX/SPARQL: nessuna HEAD, routing diretto.
            # requested_url/final_url sono l'URL ricevuto (nessun redirect).
            # status_code/content_type non disponibili (HEAD non fatto).
            route = _route_sdmx if mapped == "sdmx" else _route_sparql
            return route(
                url,
                _base_result(
                    {"requested_url": url, "final_url": url},
                    mapped,
                ),
                timeout=timeout,
            )
        # Per "html", "ckan", "http" → HEAD probe normale
        head = probe_url_headers(url, timeout=timeout, user_agent=user_agent)
        furl = head["final_url"]
        base_dict = {
            "requested_url": url,
            "final_url": furl,
            "status_code": head["status_code"],
            "content_type": head["content_type"],
            "content_disposition": head["content_disposition"],
            "resolved_format": resolve_preview_kind(
                url, head["content_type"], head["content_disposition"]
            ),
        }
        if mapped == "file":
            return _base_result(base_dict, "file")
        # html / ckan
        return _route_html(
            url, furl, _base_result(base_dict, "html"), timeout=timeout, user_agent=user_agent
        )

    # Auto-detect (fallback quando protocol non fornito o sconosciuto)
    head = probe_url_headers(url, timeout=timeout, user_agent=user_agent)
    content_type = head["content_type"]
    content_disposition = head["content_disposition"]
    final_url = head["final_url"]

    result: dict[str, Any] = {
        "requested_url": url,
        "final_url": final_url,
        "status_code": head["status_code"],
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


# ---------------------------------------------------------------------------
# Portal profile — probe leggero su portali HTML
# ---------------------------------------------------------------------------


@dataclass
class PortalProfile:
    """Profilo leggero di un portale HTML, ottenuto via probe senza crawl.

    Attributes:
        base_url: URL base del portale.
        has_robots_txt: ``/robots.txt`` raggiungibile.
        sitemap_urls: URLs delle sitemap trovate in robots.txt.
        sitemap_pages: URLs delle pagine estratte dalla sitemap.
        rss_feeds: Feed RSS trovati nell'HTML della homepage.
        has_json_api: Pattern JSON:API rilevato (es. Drupal).
        homepage_links: Numero di link interni nella homepage.
    """

    base_url: str
    has_robots_txt: bool = False
    sitemap_urls: list[str] = field(default_factory=list)
    sitemap_pages: list[str] = field(default_factory=list)
    rss_feeds: list[dict[str, str]] = field(default_factory=list)
    has_json_api: bool = False
    homepage_links: int = 0


def _fetch_robots_sitemaps(base_url: str, *, timeout: int = 10) -> tuple[bool, list[str]]:
    """Probe ``/robots.txt`` e estrae direttive ``Sitemap:``.

    Returns:
        (raggiungibile, lista URL sitemap)
    """
    robots_url = urljoin(base_url + "/", "robots.txt")
    client = HttpClient(timeout=timeout)
    result = client.get(robots_url)
    if not result.is_ok or result.response is None or result.response.status_code >= 400:
        return False, []

    sitemaps: list[str] = []
    for line in result.response.text.splitlines():
        line = line.strip()
        if line.lower().startswith("sitemap:"):
            url = line.split(":", 1)[1].strip()
            if url:
                sitemaps.append(url)
    return True, sitemaps


def _fetch_and_parse_sitemap(sitemap_url: str, *, timeout: int = 10) -> list[str]:
    """Fetch e parse una sitemap XML, ritorna lista di URL delle pagine."""
    client = HttpClient(timeout=timeout)
    result = client.get(sitemap_url)
    if not result.is_ok or result.response is None or result.response.status_code >= 400:
        return []
    try:
        root = ET.fromstring(result.response.text)
        # Namespace sitemap standard
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        urls: list[str] = []
        for url_elem in root.findall(".//sm:url", ns):
            loc = url_elem.find("sm:loc", ns)
            if loc is not None and loc.text:
                urls.append(loc.text.strip())
        # Fallback: prova senza namespace
        if not urls:
            for url_elem in root.findall(".//url"):
                loc = url_elem.find("loc")
                if loc is not None and loc.text:
                    urls.append(loc.text.strip())
        return urls
    except Exception:
        return []


def _scan_html_for_rss(html: str) -> list[dict[str, str]]:
    """Cerca link a feed RSS/Atom nell'HTML della homepage."""
    feeds: list[dict[str, str]] = []
    # Pattern: <link rel="alternate" type="application/rss+xml" href="..." title="...">
    pattern = re.compile(
        r'<link\s[^>]*?rel=["\']alternate["\'][^>]*?'
        r'type=["\']application/(?:rss|atom)\+xml["\'][^>]*?'
        r'href=["\']([^"\']+)["\'][^>]*?'
        r'(?:title=["\']([^"\']*)["\'])?',
        re.IGNORECASE | re.DOTALL,
    )
    for match in pattern.finditer(html):
        feeds.append({"url": match.group(1), "title": match.group(2) or ""})
    return feeds


def _scan_html_for_jsonapi(html: str, base_url: str) -> bool:
    """Rileva pattern JSON:API (Drupal) nell'HTML o nell'URL."""
    # Drupal 9+ espone link nel <head>
    if "jsonapi" in html.lower():
        return True
    # Verifica /jsonapi/ nell'URL base
    parsed = urlparse(base_url)
    test_url = f"{parsed.scheme}://{parsed.netloc}/jsonapi"
    client = HttpClient(timeout=5)
    result = client.get(test_url)
    if result.is_ok and result.response and result.response.status_code < 400:
        content_type = (result.response.headers.get("Content-Type") or "").lower()
        if "json" in content_type or "api" in content_type:
            return True
    return False


def probe_html_portal(
    base_url: str,
    *,
    timeout: int = 10,
    fetch_sitemap: bool = True,
    fetch_homepage: bool = True,
) -> PortalProfile:
    """Probe leggero su un portale HTML per scoprire struttura e pagine.

    Esegue una serie di probe a costo fisso (nessun crawl):
    1. ``/robots.txt`` → estrae URL delle sitemap
    2. Sitemap XML → lista pagine del portale
    3. Homepage → cerca feed RSS e pattern JSON:API (Drupal)

    Args:
        base_url: URL base del portale (es. ``https://dati.istruzione.it``).
        timeout: Timeout HTTP per ogni chiamata.
        fetch_sitemap: Se True, scarica e parse le sitemap trovate.
        fetch_homepage: Se True, scarica la homepage per RSS/API detection.

    Returns:
        PortalProfile con tutto ciò che è stato scoperto.
    """
    profile = PortalProfile(base_url=base_url)

    # 1. robots.txt → sitemap
    has_robots, sitemap_urls = _fetch_robots_sitemaps(base_url, timeout=timeout)
    profile.has_robots_txt = has_robots

    # 2. Prova anche sitemap nei path canonici (anche se robots.txt non le elenca)
    _common_sitemap_paths = ["/sitemap.xml", "/sitemap_index.xml"]
    for sm_path in _common_sitemap_paths:
        sm_url = urljoin(base_url.rstrip("/") + "/", sm_path.lstrip("/"))
        if sm_url not in sitemap_urls:
            pages = _fetch_and_parse_sitemap(sm_url, timeout=timeout)
            if pages:
                sitemap_urls.append(sm_url)
                profile.sitemap_urls = sitemap_urls
                profile.sitemap_pages.extend(pages)

    # 3. Sitemap da robots.txt → page URLs
    if fetch_sitemap and sitemap_urls:
        for sm_url in sitemap_urls[:3]:  # max 3 sitemap
            if sm_url not in _common_sitemap_paths or not profile.sitemap_pages:
                pages = _fetch_and_parse_sitemap(sm_url, timeout=timeout)
                for p in pages:
                    if p not in profile.sitemap_pages:
                        profile.sitemap_pages.append(p)

    # 3. Homepage → RSS + JSON:API + link interni
    if fetch_homepage:
        try:
            body = fetch_html_body(base_url, timeout=timeout)
            html = body.get("html_text", "")
            profile.rss_feeds = _scan_html_for_rss(html)
            profile.has_json_api = _scan_html_for_jsonapi(html, base_url)

            # Conta link interni (stesso dominio)
            from html.parser import HTMLParser

            class _InternalLinkCounter(HTMLParser):
                def __init__(self):
                    super().__init__()
                    self.count = 0
                    self._domain = urlparse(base_url).netloc.lower()

                def handle_starttag(self, tag, attrs):
                    if tag.lower() != "a":
                        return
                    for key, value in attrs:
                        if key.lower() == "href" and value:
                            href = value.strip()
                            if href and not href.startswith(
                                ("#", "mailto:", "tel:", "javascript:")
                            ):
                                full = urljoin(base_url, href)
                                if self._domain in urlparse(full).netloc.lower():
                                    self.count += 1
                            return

            parser = _InternalLinkCounter()
            parser.feed(html)
            profile.homepage_links = parser.count
        except Exception:
            pass

    return profile
