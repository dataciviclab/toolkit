"""HTTP transport layer per URL scout — probe, fetch, retry, format detection.

Tutta logica HTTP/fetch pura. Nessuna logica di routing o orchestrazione.
Condivisa tra CLI, MCP tools e SO.

Le funzioni di routing/orchestrazione stanno in toolkit.scout.probe.
"""

from __future__ import annotations

import logging
import re
import time
from html.parser import HTMLParser
from typing import Any
from urllib.parse import parse_qs, urljoin, urlparse

from lab_connectors.http import HttpClient

logger = logging.getLogger("toolkit.scout.http")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_TIMEOUT = 15
DEFAULT_USER_AGENT = "dataciviclab-toolkit/scout-url"
MAX_RETRIES = 2
RETRY_BACKOFF = 0.5

# Estensioni candidate per file dati
CANDIDATE_EXTENSIONS = (".csv", ".xlsx", ".xls", ".zip", ".json", ".parquet", ".geojson")
EXTENDED_EXTENSIONS = CANDIDATE_EXTENSIONS + (".sdmx", ".tds", ".xml")

# Tipi preview supportati dal profiler toolkit
_PREVIEW_KINDS = frozenset({"csv", "json", "xlsx", "xls", "tsv"})

# Firma HTML per rilevare CKAN
_CKAN_SIGNATURES = (
    b"data-view-embed",  # embedded data view
    b"/api/3/action",  # CKAN API reference
    b"ckan-",  # CSS class prefix
    b'"package_id"',  # JSON package reference
    b'generator" content="CKAN',  # HTML meta generator tag
    b"powered by CKAN",  # footer text
    b'data-module="dataset',  # CKAN dataset module
)

# Namespace SDMX per XML parsing
_SDMX_NS = {
    "message": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
    "structure": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
    "common": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
    "generic": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic",
}

_YEAR_RE = re.compile(r"(?<!\d)(19\d{2}|20[012]\d)(?!\d)")

# ---------------------------------------------------------------------------
# Anchor parser (candidate links da HTML)
# ---------------------------------------------------------------------------


class _AnchorParser(HTMLParser):
    """Parsa tag <a href=...> da HTML per estrarre link candidati a dati."""

    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        for key, value in attrs:
            if key.lower() == "href" and value:
                self.hrefs.append(value)
                return


# ---------------------------------------------------------------------------
# HTTP client factory
# ---------------------------------------------------------------------------


def _mk_client(
    *, timeout: int = DEFAULT_TIMEOUT, user_agent: str = DEFAULT_USER_AGENT
) -> HttpClient:
    return HttpClient(timeout=timeout, user_agent=user_agent)


# ---------------------------------------------------------------------------
# Content-type classification (pure, no HTTP)
# ---------------------------------------------------------------------------


def is_html_content(content_type: str | None) -> bool:
    """True se Content-Type indica HTML."""
    return bool(content_type and "html" in content_type.lower())


def is_file_like(url: str, content_type: str | None, content_disposition: str | None) -> bool:
    """True se URL/responses sembra puntare a un file di dati."""
    lowered = url.lower()
    if any(ext in lowered for ext in CANDIDATE_EXTENSIONS):
        return True
    if content_disposition and "attachment" in content_disposition.lower():
        return True
    if content_type and not is_html_content(content_type):
        ct_lower = content_type.lower()
        return any(token in ct_lower for token in ("csv", "excel", "spreadsheetml", "zip", "json"))
    return False


def resolve_preview_kind(
    url: str, content_type: str | None = None, content_disposition: str | None = None
) -> str | None:
    """Determina il formato preview (csv, json, xlsx, xls, tsv) da URL/headers.

    Ordine: estensione URL → Content-Disposition filename → Content-Type.
    """
    # 1. Da estensione URL
    parsed = urlparse(url)
    path = parsed.path or ""
    if "." in path:
        ext = path.rsplit(".", 1)[-1].lower()
        if ext in _PREVIEW_KINDS:
            return ext.upper()

    # 2. Da Content-Disposition (filename)
    fn = _filename_from_content_disposition(content_disposition)
    if fn and "." in fn:
        ext = fn.rsplit(".", 1)[-1].lower()
        if ext in _PREVIEW_KINDS:
            return ext.upper()

    # 3. Da Content-Type
    if content_type:
        ct_lower = content_type.lower()
        if "tab-separated" in ct_lower or ct_lower in ("text/tsv", "application/tsv"):
            return "TSV"
        if "csv" in ct_lower:
            return "CSV"
        if "json" in ct_lower:
            return "JSON"
        if "spreadsheetml" in ct_lower:
            return "XLSX"
        if "excel" in ct_lower or "xls" in ct_lower:
            return "XLS"

    return None


def is_sdmx_url(url: str) -> bool:
    """Rileva se URL punta a un endpoint SDMX."""
    lowered = url.lower()
    return any(pattern in lowered for pattern in ("/dataflow/", "/sdmx/", "sdmx", "?flow="))


def is_sparql_endpoint(url: str, content_type: str | None = None) -> bool:
    """Rileva se URL punta a un endpoint SPARQL.

    Controlla:
    - Path URL contenente ``/sparql`` (pattern piu' comune)
    - Content-Type contenente ``sparql-results`` o ``sparql``
    """
    lowered = url.lower()
    if "/sparql" in lowered:
        return True
    if content_type:
        ct = content_type.lower()
        if "sparql-results" in ct or "sparql" in ct:
            return True
    return False


def _filename_from_content_disposition(value: str | None) -> str | None:
    """Estrae filename da Content-Disposition (RFC 5987 / quoted / token)."""
    if not value:
        return None
    m = re.search(r"filename\*=(?:UTF-8''|utf-8'')([^;\s]+)", value)
    if m:
        from urllib.parse import unquote

        raw = m.group(1).strip().strip('"')
        return unquote(raw) if raw else None
    m = re.search(r'filename="([^"]+)"', value)
    if m:
        return m.group(1).strip() or None
    m = re.search(r"filename=([^;\s]+)", value)
    if m:
        return m.group(1).strip().strip('"') or None
    return None


# ---------------------------------------------------------------------------
# Candidate links extraction from HTML (pure, no network)
# ---------------------------------------------------------------------------


def extract_candidate_links(base_url: str, html_text: str) -> list[str]:
    """Estrae link a file dati (CSV/XLSX/etc.) da una pagina HTML.

    Returns lista di URL assoluti, deduplicati, ordinati per apparizione.
    """
    parser = _AnchorParser()
    parser.feed(html_text)
    links: list[str] = []
    seen: set[str] = set()
    for href in parser.hrefs:
        lowered = href.lower()
        if not any(ext in lowered for ext in CANDIDATE_EXTENSIONS):
            continue
        absolute = urljoin(base_url, href)
        if absolute in seen:
            continue
        seen.add(absolute)
        links.append(absolute)
    return links


# ---------------------------------------------------------------------------
# HTTP probe: HEAD preferito, GET+Range fallback
# ---------------------------------------------------------------------------


def probe_url_headers(
    url: str,
    *,
    timeout: int = DEFAULT_TIMEOUT,
    user_agent: str = DEFAULT_USER_AGENT,
    client: HttpClient | None = None,
) -> dict[str, Any]:
    """HEAD con retry, GET+Range fallback. Ritorna header info + reachability.

    Args:
        url: URL da probe.
        timeout: Timeout HTTP (ignorato se client è fornito).
        user_agent: User-Agent (ignorato se client è fornito).
        client: HttpClient opzionale. Se fornito, lo usa invece di crearne uno.

    Returns dict: requested_url, final_url, status_code, content_type,
                  content_disposition, method.
    """
    client = client or _mk_client(timeout=timeout, user_agent=user_agent)
    last_error: str | None = None

    # Tentativo HEAD con retry
    for attempt in range(1 + MAX_RETRIES):
        head_result = client.head(url)
        if (
            head_result.is_ok
            and head_result.response is not None
            and head_result.response.status_code < 500
        ):
            resp = head_result.response
            return _build_probe_result(
                requested_url=url,
                status_code=resp.status_code,
                content_type=resp.headers.get("Content-Type"),
                content_disposition=resp.headers.get("Content-Disposition"),
                final_url=resp.url,
                method="head",
            )
        if head_result.response is not None and head_result.response.status_code >= 500:
            last_error = f"server_error_{head_result.response.status_code}"
        elif head_result.err is not None:
            last_error = type(head_result.err).__name__
        else:
            last_error = "head_failed"
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_BACKOFF * (attempt + 1))
            continue
        break

    # HEAD fallito → GET con Range: bytes=0-0
    for attempt in range(1 + MAX_RETRIES):
        range_result = client.get(url, headers={"Range": "bytes=0-0"})
        if range_result.is_ok and range_result.response is not None:
            resp = range_result.response
            if resp.status_code < 400:
                return _build_probe_result(
                    requested_url=url,
                    status_code=resp.status_code,
                    content_type=resp.headers.get("Content-Type"),
                    content_disposition=resp.headers.get("Content-Disposition"),
                    final_url=resp.url,
                    method="get_range",
                )
            if resp.status_code >= 500 and attempt < MAX_RETRIES:
                last_error = f"server_error_{resp.status_code}"
                time.sleep(RETRY_BACKOFF * (attempt + 1))
                continue
            return _build_probe_result(
                requested_url=url,
                status_code=resp.status_code,
                content_type=resp.headers.get("Content-Type"),
                content_disposition=resp.headers.get("Content-Disposition"),
                final_url=resp.url,
                method="get_range",
            )
        if range_result.err is not None:
            last_error = type(range_result.err).__name__
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_BACKOFF * (attempt + 1))
            continue
        break

    raise RuntimeError(last_error or f"HEAD failed for {url}")


def _build_probe_result(
    *,
    requested_url: str,
    status_code: int,
    content_type: str | None,
    content_disposition: str | None,
    final_url: str,
    method: str,
) -> dict[str, Any]:
    return {
        "requested_url": requested_url,
        "final_url": final_url,
        "status_code": status_code,
        "content_type": content_type,
        "content_disposition": content_disposition,
        "method": method,
    }


# ---------------------------------------------------------------------------
# Fetch content (GET con Range, fallback intero)
# ---------------------------------------------------------------------------


def fetch_content(
    url: str,
    *,
    max_bytes: int = 1024 * 1024,
    timeout: int = DEFAULT_TIMEOUT,
    user_agent: str = DEFAULT_USER_AGENT,
    client: HttpClient | None = None,
) -> dict[str, Any]:
    """GET con Range header, fallback a GET intero se Range non supportato.

    Args:
        url: URL da scaricare.
        max_bytes: Dimensione massima in bytes.
        timeout: Timeout HTTP (ignorato se client è fornito).
        user_agent: User-Agent (ignorato se client è fornito).
        client: HttpClient opzionale.

    Returns dict: content (bytes), content_type, status_code, final_url, method.
    """
    client = client or _mk_client(timeout=timeout, user_agent=user_agent)

    # Tentativo con Range
    range_result = client.get(url, headers={"Range": f"bytes=0-{max_bytes - 1}"})
    if range_result.is_ok and range_result.response is not None:
        resp = range_result.response
        if resp.status_code in (206, 200) and resp.content:
            content = resp.content[:max_bytes]
            return {
                "content": content,
                "content_type": resp.headers.get("Content-Type"),
                "status_code": resp.status_code,
                "final_url": resp.url,
                "method": "range" if resp.status_code == 206 else "full",
            }

    # Range fallito → GET intero
    full_result = client.get(url)
    if full_result.is_ok and full_result.response is not None:
        resp = full_result.response
        if resp.status_code < 400:
            content = resp.content[:max_bytes]
            return {
                "content": content,
                "content_type": resp.headers.get("Content-Type"),
                "status_code": resp.status_code,
                "final_url": resp.url,
                "method": "full",
            }

    raise RuntimeError(f"GET failed for {url}")


def fetch_html_body(
    url: str,
    *,
    timeout: int = DEFAULT_TIMEOUT,
    user_agent: str = DEFAULT_USER_AGENT,
    client: HttpClient | None = None,
) -> dict[str, Any]:
    """GET body HTML. Ritorna dict con html_text, status_code, final_url, content_type.

    Args:
        client: HttpClient opzionale. Se fornito, lo usa invece di crearne uno.
    """
    client = client or _mk_client(timeout=timeout, user_agent=user_agent)
    result = client.get(url)
    if not result.is_ok or result.response is None:
        raise RuntimeError(f"GET failed for {url}")
    resp = result.response
    return {
        "html_text": resp.text,
        "status_code": resp.status_code,
        "final_url": resp.url,
        "content_type": resp.headers.get("Content-Type"),
    }


# ---------------------------------------------------------------------------
# CKAN detection + fetch
# ---------------------------------------------------------------------------


def detect_ckan_in_html(html_bytes: bytes) -> bool:
    """Rileva se HTML contiene firme CKAN."""
    return any(sig in html_bytes for sig in _CKAN_SIGNATURES)


def extract_ckan_dataset_id(url: str, html_text: str = "") -> str | None:
    """Estrae dataset ID CKAN da URL o HTML.

    Ordine: UUID in query param → UUID/slug in path → API reference in HTML.
    """
    # UUID in query param ?id=...
    match = re.search(r"[?&]id=([a-f0-9-]{36,})", url, re.IGNORECASE)
    if match:
        return match.group(1)
    # UUID o slug in path /dataset/...
    match = re.search(r"/dataset/([^/?#]+)", url, re.IGNORECASE)
    if match:
        return match.group(1)
    # Da HTML: /api/3/action/package_show?id=...
    if html_text:
        api_match = re.search(r'["\']?(/api/3/action/package_show\?id=[^"\'<>\s]+)', html_text)
        if api_match:
            qs = api_match.group(1).split("?", 1)[-1]
            parsed = parse_qs(qs)
            if "id" in parsed:
                return parsed["id"][0]
    return None


def fetch_ckan_package(
    portal_url: str,
    dataset_id: str,
    *,
    timeout: int = DEFAULT_TIMEOUT,
    client: HttpClient | None = None,
) -> dict[str, Any] | None:
    """Fetch CKAN package_show via API.

    Args:
        client: HttpClient opzionale. Se fornito, lo usa invece di crearne uno.
    """
    parsed = urlparse(portal_url)
    root = f"{parsed.scheme}://{parsed.netloc}"
    api_bases: list[str] = []
    if parsed.path.startswith("/api/3/action"):
        api_bases.append(f"{root}/api/3/action/package_show")
    elif parsed.path.startswith("/dataset/"):
        api_bases.append(f"{root}/api/3/action/package_show")
    else:
        api_bases.append(f"{root}/api/3/action/package_show")
        api_bases.append(f"{root}/package_show")

    client = client or _mk_client(timeout=timeout)
    for api_base in api_bases:
        pkg_url = f"{api_base}?id={dataset_id}"
        try:
            result = client.get(pkg_url)
            if not result.is_ok or result.response is None:
                continue
            resp = result.response
            if resp.status_code != 200:
                continue
            data = resp.json()
            if not data.get("success"):
                continue
            return data.get("result")
        except Exception:
            continue
    return None


def search_ckan_datasets(
    portal_url: str,
    query: str = "*:*",
    rows: int = 100,
    timeout: int = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """Cerca dataset in un portale CKAN via ``package_search``.

    Args:
        portal_url: URL base del portale CKAN.
        query: Query Solr (default ``*:*`` per tutti).
        rows: Max risultati (default 100, max 500).
        timeout: Timeout HTTP.

    Returns:
        Dict con ``count`` (totale), ``datasets`` (lista).

    Raises:
        RuntimeError: se la richiesta fallisce o l'API risponde con errore.
    """
    safe_rows = max(1, min(int(rows), 500))
    base = portal_url.rstrip("/")
    search_url = (
        f"{base}/api/3/action/package_search"
        if not base.endswith("/api/3/action")
        else f"{base}/package_search"
    )
    client = _mk_client(timeout=timeout)

    result = client.get(search_url, params={"q": query, "rows": safe_rows})
    if not result.is_ok or result.response is None:
        raise RuntimeError(f"CKAN package_search failed: {result.err}")

    resp = result.response
    if resp.status_code != 200:
        raise RuntimeError(f"CKAN HTTP {resp.status_code} for {search_url}")

    try:
        data = resp.json()
    except ValueError as exc:
        raise RuntimeError(f"CKAN JSON invalido: {exc}") from exc
    if not data.get("success"):
        raise RuntimeError("CKAN package_search returned unsuccessful")

    search_result = data.get("result", {})
    raw_datasets = search_result.get("results", [])

    datasets: list[dict[str, Any]] = []
    for ds in raw_datasets:
        org = ds.get("organization") or {}
        datasets.append(
            {
                "id": ds.get("id") or ds.get("name"),
                "name": ds.get("name") or ds.get("id"),
                "title": ds.get("title"),
                "organization": org.get("title") or org.get("name"),
                "resources_count": len(ds.get("resources") or []),
                "metadata_modified": ds.get("metadata_modified"),
            }
        )

    return {"count": search_result.get("count", 0), "datasets": datasets}


ISTAT_ESPLORADATI_BASE = "https://esploradati.istat.it/SDMXWS/rest"


def list_sdmx_dataflows(
    agency: str = "IT1",
    timeout: int = 30,
) -> list[dict[str, str]]:
    """Elenca i dataflow SDMX disponibili per un'agenzia.

    Args:
        agency: ID agenzia SDMX (default ``IT1`` per ISTAT).
        timeout: Timeout HTTP.

    Returns:
        Lista di dict con ``dataflow_id``, ``name``, ``agency_id``, ``version``.

    Raises:
        RuntimeError: se la richiesta fallisce o l'API risponde con errore.
    """
    import json

    client = _mk_client(timeout=timeout)
    dataflow_url = f"{ISTAT_ESPLORADATI_BASE}/dataflow/{agency}/all/latest"

    result = client.get(
        dataflow_url,
        headers={"Accept": "application/vnd.sdmx.structure+json; version=2"},
    )
    if not result.is_ok or result.response is None:
        raise RuntimeError(f"SDMX dataflow request failed: {result.err}")

    resp = result.response
    if resp.status_code != 200:
        raise RuntimeError(f"SDMX HTTP {resp.status_code} for {dataflow_url}")

    try:
        payload = json.loads(resp.text)
    except ValueError as exc:
        raise RuntimeError(f"SDMX JSON invalido: {exc}") from exc
    flows = payload.get("data", {}).get("dataflows", [])

    dataflows: list[dict[str, str]] = []
    for flow in flows:
        dataflows.append(
            {
                "dataflow_id": flow.get("id"),
                "name": flow.get("name"),
                "agency_id": flow.get("agencyID"),
                "version": flow.get("version"),
            }
        )
    return dataflows


def discover_ckan_resources(pkg: dict[str, Any]) -> list[dict[str, Any]]:
    """Estrae risorse scaricabili da un package CKAN."""
    resources: list[dict[str, Any]] = pkg.get("resources") or []
    discovered: list[dict[str, Any]] = []
    for res in resources:
        res_url = res.get("url") or ""
        if not res_url or not res_url.startswith("http"):
            continue
        discovered.append(
            {
                "id": res.get("id") or "",
                "name": res.get("name") or res.get("description") or res.get("id") or "",
                "format": (res.get("format") or "").lower(),
                "url": res_url,
            }
        )
    return discovered


# ---------------------------------------------------------------------------
# SDMX
# ---------------------------------------------------------------------------


def fetch_sdmx_years(
    base_url: str,
    flow_id: str,
    *,
    timeout: int = DEFAULT_TIMEOUT,
    client: HttpClient | None = None,
) -> tuple[int | None, int | None]:
    """Chiama endpoint SDMX per ricavare year_min/year_max da TIME_PERIOD.

    Args:
        client: HttpClient opzionale. Se fornito, lo usa invece di crearne uno.
    """
    try:
        base = base_url.split("?")[0].rstrip("/")
        if "/dataflow/" in base:
            sdmx_root = base[: base.index("/dataflow/")]
        elif base.endswith("/dataflow"):
            sdmx_root = base[: -len("/dataflow")]
        else:
            sdmx_root = base
        url = f"{sdmx_root}/data/{flow_id}?lastNObservations=1"
        client = client or _mk_client(timeout=timeout)
        result = client.get(url, headers={"Accept": "application/xml"})
        if not result.is_ok or result.response is None:
            return None, None
        r = result.response
        if r.status_code != 200:
            return None, None
        import xml.etree.ElementTree as ET

        root = ET.fromstring(r.text)
        time_values: list[str] = []
        for val_el in root.findall(".//generic:ObsKey/generic:Value", _SDMX_NS):
            if val_el.get("id") == "TIME_PERIOD":
                v = val_el.get("value")
                if v:
                    time_values.append(v)
        years: list[int] = []
        for tv in time_values:
            found = _YEAR_RE.findall(tv)
            years.extend(int(y) for y in found)
        if not years:
            return None, None
        return min(years), max(years)
    except Exception:
        return None, None
