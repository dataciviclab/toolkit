"""HTTP transport layer per URL scout — probe, fetch, retry, format detection.

Tutta logica HTTP/fetch pura. Nessuna logica di routing o orchestrazione.
Condivisa tra CLI, MCP tools e SO.

Le funzioni di routing/orchestrazione stanno in toolkit.scout.probe.
"""

from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import urlparse

from lab_connectors.http import HttpClient

logger = logging.getLogger("toolkit.scout.http")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_TIMEOUT = 15
DEFAULT_USER_AGENT = "dataciviclab-toolkit/scout-url"
# Estensioni candidate per file dati
CANDIDATE_EXTENSIONS = (".csv", ".xlsx", ".xls", ".zip", ".json", ".parquet", ".geojson")
EXTENDED_EXTENSIONS = CANDIDATE_EXTENSIONS + (".sdmx", ".tds", ".xml")

# Tipi preview supportati dal profiler toolkit
_PREVIEW_KINDS = frozenset({"csv", "json", "xlsx", "xls", "tsv"})

# ---------------------------------------------------------------------------
# Re-imports from plugins (canonical source of truth)
# ---------------------------------------------------------------------------

from toolkit.plugins.ckan import (  # noqa: E402
    CKAN_SIGNATURES as _CKAN_SIGNATURES,  # noqa: F401
    ckan_portal_base as _ckan_portal_base,
    detect_ckan_in_html,  # noqa: F401
    extract_ckan_dataset_id,  # noqa: F401
)
from toolkit.plugins.sdmx import SDMX_NS_FULL as _SDMX_NS  # noqa: E402
from toolkit.plugins.sdmx import is_sdmx_url  # noqa: E402, F401

_YEAR_RE = re.compile(r"(?<!\d)(19\d{2}|20[012]\d)(?!\d)")

# ---------------------------------------------------------------------------
# HTTP client factory
# ---------------------------------------------------------------------------


def _mk_client(
    *,
    timeout: int = DEFAULT_TIMEOUT,
    user_agent: str = DEFAULT_USER_AGENT,
    circuit_threshold: int = 0,
) -> HttpClient:
    return HttpClient(
        timeout=timeout,
        user_agent=user_agent,
        circuit_threshold=circuit_threshold,
    )


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
# Reindirizzato a link_extractor come contratto centrale.
# Mantenuto per backward compatibility.
# ---------------------------------------------------------------------------


def extract_candidate_links(base_url: str, html_text: str) -> list[str]:
    """Estrae link a file dati (CSV/XLSX/etc.) da una pagina HTML.

    Returns lista di URL assoluti, deduplicati, ordinati per apparizione.

    Nota: reindirizzato a ``toolkit.scout.link_extractor`` come contratto
    centrale. Questa funzione è mantenuta per backward compatibility.
    """
    from toolkit.scout.link_extractor import extract_candidate_links as _extract

    return _extract(base_url, html_text)


# ---------------------------------------------------------------------------
# HTTP probe: HEAD preferito, GET+Range fallback
# ---------------------------------------------------------------------------


def probe_url_headers(
    url: str,
    *,
    timeout: int = DEFAULT_TIMEOUT,
    user_agent: str = DEFAULT_USER_AGENT,
    client: HttpClient | None = None,
    circuit_threshold: int = 0,
) -> dict[str, Any]:
    """HEAD con retry, GET+Range fallback. Ritorna header info + reachability.

    Implementato in ``lab_connectors.http.probe`` (evita doppio retry).
    Questo wrapper mantiene compatibilità con i consumer esistenti.
    """
    from lab_connectors.http.probe import probe_url_headers as _probe

    if client is None:
        client = _mk_client(
            timeout=timeout, user_agent=user_agent, circuit_threshold=circuit_threshold
        )
    return _probe(url, timeout=timeout, user_agent=user_agent, client=client)


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
    """GET bounded: Range header + fallback streaming, mai oltre ``max_bytes``.

    Tutti i GET usano ``stream=True``: se il server ignora ``Range`` e risponde
    con ``200`` (full body), ``iter_content`` legge solo ``max_bytes`` e chiude.
    Nessun rischio di caricare file interi in memoria.

    Args:
        url: URL da scaricare.
        max_bytes: Dimensione massima in bytes.
        timeout: Timeout HTTP (ignorato se client è fornito).
        user_agent: User-Agent (ignorato se client è fornito).
        client: HttpClient opzionale.

    Returns dict: content (bytes), content_type, status_code, final_url, method,
                  content_length (int | None su 206 da Content-Range).

    Raises:
        RuntimeError: se la GET fallisce o non ritorna contenuto.
    """
    client = client or _mk_client(timeout=timeout, user_agent=user_agent)

    def _read_bounded(resp: Any, max_b: int) -> bytes:
        """Legge al massimo max_b bytes da una risposta streaming."""
        chunks: list[bytes] = []
        total = 0
        for chunk in resp.iter_content(chunk_size=65536):
            if chunk:
                remaining = max_b - total
                if remaining <= 0:
                    break
                chunks.append(chunk[:remaining])
                total += len(chunks[-1])
        return b"".join(chunks)

    def _parse_content_range(resp: Any) -> int | None:
        """Estrae la dimensione totale del file da Content-Range (es. ``bytes 0-0/123456``)."""
        cr = resp.headers.get("Content-Range", "")
        if cr and "/" in cr:
            total_str = cr.split("/")[-1].strip()
            if total_str.isdigit():
                return int(total_str)
        return None

    # Tentativo con Range — stream=True: se il server ignora Range e risponde
    # 200, leggiamo solo max_bytes e chiudiamo.
    range_result = client.get(url, headers={"Range": f"bytes=0-{max_bytes - 1}"}, stream=True)
    if range_result.is_ok and range_result.response is not None:
        resp = range_result.response
        try:
            if resp.status_code in (206, 200):
                content = _read_bounded(resp, max_bytes)
                if content:
                    file_size = _parse_content_range(resp) if resp.status_code == 206 else None
                    return {
                        "content": content,
                        "content_type": resp.headers.get("Content-Type"),
                        "status_code": resp.status_code,
                        "final_url": resp.url,
                        "method": "range" if resp.status_code == 206 else "full",
                        "content_length": file_size,
                    }
        finally:
            getattr(resp, "close", lambda: None)()

    # Range fallito → GET streaming bounded
    full_result = client.get(url, stream=True)
    if full_result.is_ok and full_result.response is not None:
        resp = full_result.response
        try:
            if resp.status_code < 400:
                content = _read_bounded(resp, max_bytes)
                if content:
                    return {
                        "content": content,
                        "content_type": resp.headers.get("Content-Type"),
                        "status_code": resp.status_code,
                        "final_url": resp.url,
                        "method": "full",
                        "content_length": None,
                    }
        finally:
            getattr(resp, "close", lambda: None)()

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
# CKAN fetch (uses detection helpers imported from plugins.ckan)
# ---------------------------------------------------------------------------


def fetch_ckan_package(
    portal_url: str,
    dataset_id: str,
    *,
    timeout: int = DEFAULT_TIMEOUT,
    client: HttpClient | None = None,
) -> dict[str, Any] | None:
    """Fetch CKAN package_show via API.

    Normalizza automaticamente il base path del portale CKAN,
    quindi funziona con qualsiasi URL: homepage, pagina dataset, endpoint API.

    Args:
        client: HttpClient opzionale. Se fornito, lo usa invece di crearne uno.
    """
    parsed = urlparse(portal_url)
    root = f"{parsed.scheme}://{parsed.netloc}"

    # Normalizza il base path del portale CKAN
    portal_base = _ckan_portal_base(portal_url)

    api_bases: list[str] = [
        f"{portal_base}/api/3/action/package_show",
    ]

    # Fallback: API alla radice (per portali che ignorano il path prefix)
    api_base_root = f"{root}/api/3/action/package_show"
    if api_base_root != api_bases[0]:
        api_bases.append(api_base_root)
    # Ultimo fallback: package_show alla radice
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


def fetch_ckan_datastore_schema(
    portal_url: str,
    resource_id: str,
    *,
    timeout: int = DEFAULT_TIMEOUT,
    client: HttpClient | None = None,
) -> list[dict[str, Any]] | None:
    """Fetch CKAN DataStore schema per una risorsa (``datastore_search?limit=0``).

    Restituisce la lista campi ``[{id, type, info: {label, notes}}, ...]``
    che lo scaffold puo' usare per generare colonne mappate senza scaricare il CSV.

    Args:
        portal_url: URL del portale CKAN (qualsiasi forma).
        resource_id: UUID della risorsa.
        timeout: Timeout HTTP.
        client: HttpClient opzionale.

    Returns:
        Lista di dict con ``id``, ``type``, ``info``, oppure ``None``
        se la risorsa non ha DataStore o la chiamata fallisce.
    """
    parsed = urlparse(portal_url)
    root = f"{parsed.scheme}://{parsed.netloc}"
    portal_base = _ckan_portal_base(portal_url)

    api_bases: list[str] = [
        f"{portal_base}/api/3/action/datastore_search",
    ]
    api_base_root = f"{root}/api/3/action/datastore_search"
    if api_base_root != api_bases[0]:
        api_bases.append(api_base_root)
    api_bases.append(f"{root}/datastore_search")

    client = client or _mk_client(timeout=timeout)
    for api_base in api_bases:
        ds_url = f"{api_base}?resource_id={resource_id}&limit=0"
        try:
            result = client.get(ds_url)
            if not result.is_ok or result.response is None:
                continue
            resp = result.response
            if resp.status_code != 200:
                continue
            data = resp.json()
            if not data.get("success"):
                continue
            fields = data.get("result", {}).get("fields", [])
            if not fields:
                continue
            # Filtra il campo _id (PK autoincrement interno di CKAN)
            return [f for f in fields if f.get("id") != "_id"]
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
    """Estrae risorse scaricabili da un package CKAN.

    ``datastore_active`` indica se la risorsa ha DataStore abilitato:
    lo scaffold puo' usare ``datastore_search?limit=0`` invece di scaricare
    il CSV per ottenere schema colonne e tipi.
    """
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
                "datastore_active": bool(res.get("datastore_active")),
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

    Supporta sia SDMX-ML (ISTAT, parse XML ``generic:ObsKey``) sia SDMX-JSON
    2.0 (Eurostat, flat ``dimension.time.category.index``): se il parse XML
    non trova TIME_PERIOD, ritenta con Accept JSON e legge la dimensione time.

    Args:
        client: HttpClient opzionale. Se fornito, lo usa invece di crearne uno.
    """
    import json
    import xml.etree.ElementTree as ET

    try:
        base = base_url.split("?")[0].rstrip("/")
        if "/dataflow/" in base:
            sdmx_root = base[: base.index("/dataflow/")]
        elif "/data/" in base:
            # Root API da path dati: es. .../sdmx/2.1/data/NAMA_10R_3GDP
            sdmx_root = base[: base.index("/data/")]
        elif base.endswith("/dataflow"):
            sdmx_root = base[: -len("/dataflow")]
        else:
            sdmx_root = base
        url = f"{sdmx_root}/data/{flow_id}?lastNObservations=1"
        client = client or _mk_client(timeout=timeout)

        # Passo 1: XML (SDMX-ML, convention ISTAT)
        result = client.get(url, headers={"Accept": "application/xml"})
        years: list[int] = []
        if result.is_ok and result.response is not None and result.response.status_code == 200:
            root = ET.fromstring(result.response.text)
            time_values: list[str] = []
            for val_el in root.findall(".//generic:ObsKey/generic:Value", _SDMX_NS):
                if val_el.get("id") == "TIME_PERIOD":
                    v = val_el.get("value")
                    if v:
                        time_values.append(v)
            for tv in time_values:
                found = _YEAR_RE.findall(tv)
                years.extend(int(y) for y in found)

        # Passo 2: fallback JSON 2.0 (Eurostat) — nessun TIME_PERIOD XML.
        # Eurostat richiede ?format=json esplicito (il default è JSONSTAT,
        # che richiede lang e risponde 400). Il JSON 2.0 ha la dimensione
        # time flat in dimension.time.category.index.
        if not years:
            result = client.get(
                url,
                headers={"Accept": "application/json"},
                params={"format": "json"},
            )
            if result.is_ok and result.response is not None and result.response.status_code == 200:
                try:
                    payload = json.loads(result.response.text)
                except (json.JSONDecodeError, TypeError):
                    payload = None
                if payload:
                    time_dim = (payload.get("dimension") or {}).get("time") or {}
                    for period in (time_dim.get("category") or {}).get("index") or {}:
                        found = _YEAR_RE.findall(str(period))
                        years.extend(int(y) for y in found)

        if not years:
            return None, None
        return min(years), max(years)
    except Exception:
        return None, None
