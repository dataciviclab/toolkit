from __future__ import annotations

import re
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urljoin, urlparse

import requests

# Public API — functions used by cmd_inspect.py
__all__ = [
    "probe_url",
    "_generate_yaml_scaffold",
    "_detect_ckan",
    "_discover_ckan_resources",
    "_extract_ckan_dataset_id",
    "_is_html",
    "_is_file_like",
    "_candidate_links",
    # Constants used by cmd_inspect
    "_EXTENDED_EXTENSIONS",
    "_DEFAULT_TIMEOUT",
    "_DEFAULT_USER_AGENT",
]


_CANDIDATE_EXTENSIONS = (".csv", ".xlsx", ".xls", ".zip", ".json", ".parquet", ".geojson")
_EXTENDED_EXTENSIONS = _CANDIDATE_EXTENSIONS + (".sdmx", ".tds", ".xml")
_DEFAULT_USER_AGENT = "dataciviclab-toolkit/scout-url"
_DEFAULT_TIMEOUT = 10
_MAX_PRINTED_LINKS = 20

_CKAN_ID_PARAM_RE = re.compile(r"[?&]id=([a-f0-9-]{36,})", re.IGNORECASE)
_CKAN_DATASET_PATH_RE = re.compile(r"/dataset/([^/?#]+)", re.IGNORECASE)
_CKAN_HTML_SIGNATURES = (
    b"data-view-embed",
    b"/api/3/action",
    b"ckan-",
    b'"package_id"',
)


class _AnchorParser(HTMLParser):
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


def _is_html(content_type: str | None) -> bool:
    if not content_type:
        return False
    return "html" in content_type.lower()


def _is_file_like(final_url: str, content_type: str | None, content_disposition: str | None) -> bool:
    lowered_url = final_url.lower()
    if any(ext in lowered_url for ext in _CANDIDATE_EXTENSIONS):
        return True
    if content_disposition and "attachment" in content_disposition.lower():
        return True
    if content_type and not _is_html(content_type):
        lowered_type = content_type.lower()
        return any(
            token in lowered_type
            for token in ("csv", "excel", "spreadsheetml", "zip", "json")
        )
    return False


def _candidate_links(base_url: str, html_text: str) -> list[str]:
    parser = _AnchorParser()
    parser.feed(html_text)
    links: list[str] = []
    seen: set[str] = set()
    for href in parser.hrefs:
        lowered = href.lower()
        if not any(ext in lowered for ext in _CANDIDATE_EXTENSIONS):
            continue
        absolute = urljoin(base_url, href)
        if absolute in seen:
            continue
        seen.add(absolute)
        links.append(absolute)
    return links


def _extract_ckan_dataset_id(url: str, html_text: str = "") -> str | None:
    match = _CKAN_ID_PARAM_RE.search(url)
    if match:
        return match.group(1)
    match = _CKAN_DATASET_PATH_RE.search(url)
    if match:
        slug = match.group(1)
        if len(slug) >= 32 and "-" in slug:
            return slug
        return slug
    if html_text:
        api_match = re.search(r'["\']?(/api/3/action/package_show\?id=[^"\'<>\s]+)', html_text)
        if api_match:
            qs = api_match.group(1).split("?", 1)[-1]
            parsed = parse_qs(qs)
            if "id" in parsed:
                return parsed["id"][0]
    return None


def _detect_ckan(html_content: bytes) -> bool:
    return any(sig in html_content for sig in _CKAN_HTML_SIGNATURES)


def _discover_ckan_resources(
    portal_url: str,
    dataset_id: str,
    *,
    timeout: int = _DEFAULT_TIMEOUT,
    user_agent: str = _DEFAULT_USER_AGENT,
) -> list[dict[str, Any]]:
    parsed = urlparse(portal_url)
    root = f"{parsed.scheme}://{parsed.netloc}"
    api_bases = []
    if parsed.path.startswith("/api/3/action"):
        api_bases.append(f"{root}/api/3/action/package_show")
    elif parsed.path.startswith("/api/3"):
        api_bases.append(f"{root}/api/3/action/package_show")
    elif parsed.path.startswith("/dataset/"):
        api_bases.append(f"{root}/api/3/action/package_show")
    else:
        api_bases.append(f"{root}/api/3/action/package_show")
        api_bases.append(f"{root}/package_show")
    headers = {"User-Agent": user_agent}
    for api_base in api_bases:
        pkg_url = f"{api_base}?id={dataset_id}"
        try:
            resp = requests.get(pkg_url, timeout=timeout, headers=headers)
            if resp.status_code != 200:
                continue
            data = resp.json()
            if not data.get("success"):
                continue
            result = data.get("result") or {}
        except Exception:
            continue
        resources: list[dict[str, Any]] = result.get("resources") or []
        discovered: list[dict[str, Any]] = []
        for res in resources:
            res_url = res.get("url") or ""
            if not res_url or not res_url.startswith("http"):
                continue
            discovered.append({
                "id": res.get("id") or "",
                "name": res.get("name") or res.get("description") or res.get("id") or "",
                "format": (res.get("format") or "").lower(),
                "url": res_url,
            })
        if discovered:
            return discovered
    return []


def _generate_yaml_scaffold(
    probe_result: dict[str, Any],
    ckan_resources: list[dict[str, Any]] | None = None,
    candidate_links: list[str] | None = None,
) -> str:
    url = probe_result["final_url"]
    parsed = urlparse(url)
    slug = Path(parsed.path).stem or "dataset"
    slug = re.sub(r"[^a-z0-9_]", "_", slug.lower())
    slug = re.sub(r"_+", "_", slug).strip("_")
    if not slug:
        slug = "dataset"
    lines = [
        "# Scaffold generato da scout-url --scaffold",
        "# Verifica e modifica prima di usare",
        "",
        'root: "../../out"',
        "schema_version: 1",
        "",
        "dataset:",
        f'  name: "{slug}"',
        "  years: [2024]  # TBD: inferito da URL o sorgente",
        "",
        "raw:",
        "  output_policy: overwrite",
        "  sources:",
    ]

    def _make_source_name(link: str) -> str:
        stem = Path(urlparse(link).path).stem
        return re.sub(r"[^a-z0-9_]", "_", (stem or "resource").lower())

    def _infer_type_from_url(u: str) -> str:
        if "/datastore/dump/" in u or "/datastore_search" in u:
            return "ckan"
        if "sdmx" in u.lower() or "/dataflow/" in u.lower():
            return "sdmx"
        return "http_file"

    if ckan_resources:
        for res in ckan_resources:
            res_name = re.sub(r"[^a-z0-9_]", "_", (res["name"] or "resource").lower())
            res_url = res["url"]
            fmt = res["format"]
            fname = Path(urlparse(res_url).path).name or f"{res_name}.{fmt}"
            portal_base = f"{parsed.scheme}://{parsed.netloc}"
            lines.append(f'    - name: "{res_name}"')
            lines.append('      type: "ckan"')
            lines.append("      args:")
            lines.append(f'        portal_url: "{portal_base}"')
            lines.append(f'        resource_id: "{res.get("id") or ""}"')
            lines.append(f'        filename: "{fname}"')
            lines.append("      primary: true")
    elif candidate_links:
        seen: set[str] = set()
        for link in candidate_links:
            if link in seen:
                continue
            seen.add(link)
            link_name = _make_source_name(link)
            fname = Path(urlparse(link).path).name
            stype = _infer_type_from_url(link)
            lines.append(f'    - name: "{link_name}"')
            lines.append(f'      type: "{stype}"')
            lines.append("      args:")
            lines.append(f'        url: "{link}"')
            if fname:
                lines.append(f'        filename: "{fname}"')
            lines.append("      primary: true")
    else:
        fname = Path(parsed.path).name
        stype = _infer_type_from_url(url)
        lines.append(f'    - name: "{slug}_source"')
        lines.append(f'      type: "{stype}"')
        lines.append("      args:")
        lines.append(f'        url: "{url}"')
        if fname:
            lines.append(f'        filename: "{fname}"')
        lines.append("      primary: true")
    lines.append("")
    return "\n".join(lines)


def probe_url(
    url: str,
    *,
    timeout: int = _DEFAULT_TIMEOUT,
    user_agent: str = _DEFAULT_USER_AGENT,
    capture_html: bool = False,
) -> dict[str, Any]:
    headers = {"User-Agent": user_agent}
    with requests.get(url, allow_redirects=True, timeout=timeout, headers=headers, stream=True) as response:
        content_type = response.headers.get("Content-Type")
        content_disposition = response.headers.get("Content-Disposition")
        final_url = response.url
        is_html = _is_html(content_type)
        raw_html: bytes = b""
        if is_html:
            response.encoding = response.encoding or response.apparent_encoding or "utf-8"
            text = response.text
            candidate_links = _candidate_links(final_url, text)
            kind = "html"
            if capture_html:
                raw_html = text.encode(response.encoding or "utf-8", errors="replace")
        elif _is_file_like(final_url, content_type, content_disposition):
            candidate_links = []
            kind = "file"
        else:
            candidate_links = []
            kind = "opaque"
        result: dict[str, Any] = {
            "requested_url": url,
            "final_url": final_url,
            "status_code": response.status_code,
            "content_type": content_type,
            "content_disposition": content_disposition,
            "kind": kind,
            "candidate_links": candidate_links,
        }
        if capture_html and raw_html:
            result["html_content"] = raw_html
        return result
