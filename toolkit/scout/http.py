"""HTTP transport layer per URL scout — probe, fetch, CKAN, SDMX.

Le classificazioni pure (is_sdmx_url, resolve_preview_kind, etc.) sono
ora in ``lab_connectors.discovery.classify``. Questo modulo mantiene le
funzioni HTTP-specifiche che dipendono da toolkit.plugins.

Condivisa tra CLI, MCP tools e SO.
"""

from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import urlparse

from lab_connectors.discovery.classify import (  # noqa: F401
    CANDIDATE_EXTENSIONS,
    EXTENDED_EXTENSIONS,
    PREVIEW_KINDS as _PREVIEW_KINDS,
    filename_from_content_disposition as _filename_from_content_disposition,
    is_file_like,
    is_html_content,
    is_sdmx_url,
    is_sparql_endpoint,
    resolve_preview_kind,
)
from lab_connectors.http import HttpClient

logger = logging.getLogger("toolkit.scout.http")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_TIMEOUT = 15
DEFAULT_USER_AGENT = "dataciviclab-toolkit/scout-url"

_YEAR_RE = re.compile(r"(?<!\d)(19\d{2}|20[012]\d)(?!\d)")

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
        """Estrae la dimensione totale del file da Content-Range."""
        cr = resp.headers.get("Content-Range", "")
        if cr and "/" in cr:
            total_str = cr.split("/")[-1].strip()
            if total_str.isdigit():
                return int(total_str)
        return None

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
    """GET body HTML. Ritorna dict con html_text, status_code, final_url, content_type."""
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
# CKAN fetch
# ---------------------------------------------------------------------------


def fetch_ckan_package(
    portal_url: str,
    dataset_id: str,
    *,
    timeout: int = DEFAULT_TIMEOUT,
    client: HttpClient | None = None,
) -> dict[str, Any] | None:
    """Fetch CKAN package_show via API con fallback multipli."""
    parsed = urlparse(portal_url)
    root = f"{parsed.scheme}://{parsed.netloc}"
    portal_base = _ckan_portal_base(portal_url)

    api_bases: list[str] = [f"{portal_base}/api/3/action/package_show"]
    api_base_root = f"{root}/api/3/action/package_show"
    if api_base_root != api_bases[0]:
        api_bases.append(api_base_root)
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
    """Fetch CKAN DataStore schema per una risorsa."""
    parsed = urlparse(portal_url)
    root = f"{parsed.scheme}://{parsed.netloc}"
    portal_base = _ckan_portal_base(portal_url)

    api_bases: list[str] = [f"{portal_base}/api/3/action/datastore_search"]
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
            return [f for f in fields if f.get("id") != "_id"]
        except Exception:
            continue
    return None


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
    """Chiama endpoint SDMX per ricavare year_min/year_max da TIME_PERIOD."""
    import json
    import xml.etree.ElementTree as ET

    try:
        base = base_url.split("?")[0].rstrip("/")
        if "/dataflow/" in base:
            sdmx_root = base[: base.index("/dataflow/")]
        elif "/data/" in base:
            sdmx_root = base[: base.index("/data/")]
        elif base.endswith("/dataflow"):
            sdmx_root = base[: -len("/dataflow")]
        else:
            sdmx_root = base
        url = f"{sdmx_root}/data/{flow_id}?lastNObservations=1"
        client = client or _mk_client(timeout=timeout)

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
