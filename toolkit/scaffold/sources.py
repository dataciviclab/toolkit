"""Generazione blocchi raw.sources per dataset.yml.

Funzioni di utilità per slug, estensione, filename e generazione YAML
raw.sources per ogni tipo fonte supportato (http_file, ckan, sdmx).
"""

from __future__ import annotations

import re
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse


def slugify(url: str) -> str:
    """Genera uno slug stabile e univoco per un URL (uuid5 namespace URL).

    Decodifica il percent-encoding dell'URL prima di slugificare,
    in modo che ``posti%20per%20stabilimento.csv`` produca
    ``posti_per_stabilimento_<hash>`` invece di ``posti_20letto_...``.
    """
    parsed = urlparse(url)
    stem = Path(parsed.path).stem or "dataset"
    stem = unquote(stem)  # decodifica %20, %2F, ecc.
    slug = re.sub(r"[^a-z0-9_]", "_", stem.lower())
    slug = re.sub(r"_+", "_", slug).strip("_")
    if not slug:
        slug = "dataset"
    short_hash = uuid.uuid5(uuid.NAMESPACE_URL, url).hex[:6]
    return f"{slug}_{short_hash}"


def infer_ext(url: str, content_type: str) -> str:
    """Inferisce estensione file da URL o Content-Type."""
    url_ext = Path(urlparse(url).path).suffix.lower()
    if url_ext and url_ext not in (".php", ".asp", ".aspx", ".jsp"):
        return url_ext
    ct = content_type.lower()
    if "csv" in ct:
        return ".csv"
    if "json" in ct:
        return ".json"
    if "spreadsheetml" in ct or "excel" in ct:
        return ".xlsx"
    if "xml" in ct:
        return ".xml"
    return ".csv"


def infer_filename(url: str, slug: str) -> str:
    """Inferisce nome file dall'URL."""
    path = urlparse(url).path
    if path.endswith(".php"):
        return Path(path).stem + ".csv"
    name = Path(path).name
    return name or f"{slug}.csv"


# ---------------------------------------------------------------------------
# Source block generators (per tipo)
# ---------------------------------------------------------------------------


def _make_source_name(link: str) -> str:
    stem = Path(urlparse(link).path).stem
    return re.sub(r"[^a-z0-9_]", "_", (stem or "resource").lower())


def block_http_file(url: str, slug: str, fname: str | None = None) -> list[str]:
    """Blocco raw.sources per http_file."""
    parsed = urlparse(url)
    if fname is None:
        fname = Path(parsed.path).name or f"{slug}.csv"
    return [
        f'    - name: "{slug}_source"',
        '      type: "http_file"',
        "      args:",
        f'        url: "{url}"',
        f'        filename: "{fname}"',
        "      primary: true",
    ]


def block_ckan(resources: list[dict[str, Any]], portal_url: str) -> list[str]:
    """Blocchi raw.sources per risorse CKAN."""
    parsed = urlparse(portal_url)
    portal_base = f"{parsed.scheme}://{parsed.netloc}"
    lines: list[str] = []
    for res in resources:
        res_name = re.sub(r"[^a-z0-9_]", "_", (res["name"] or "resource").lower())
        res_url = res["url"]
        fmt = res["format"]
        fname = Path(urlparse(res_url).path).name or f"{res_name}.{fmt}"
        lines.append(f'    - name: "{res_name}"')
        lines.append('      type: "ckan"')
        lines.append("      args:")
        lines.append(f'        portal_url: "{portal_base}"')
        lines.append(f'        resource_id: "{res.get("id") or ""}"')
        lines.append(f'        filename: "{fname}"')
        lines.append("      primary: true")
    return lines


def block_links(links: list[str]) -> list[str]:
    """Blocchi raw.sources per link candidati da pagina HTML."""
    lines: list[str] = []
    seen: set[str] = set()
    for link in links:
        if link in seen:
            continue
        seen.add(link)
        link_name = _make_source_name(link)
        fname = Path(urlparse(link).path).name
        lines.append(f'    - name: "{link_name}"')
        lines.append('      type: "http_file"')
        lines.append("      args:")
        lines.append(f'        url: "{link}"')
        if fname:
            lines.append(f'        filename: "{fname}"')
        lines.append("      primary: true")
    return lines


def block_sdmx(sdmx_info: dict[str, Any] | None, url: str) -> list[str]:
    """Blocchi raw.sources per endpoint SDMX."""
    if sdmx_info and sdmx_info.get("flow_id"):
        return [
            f'    - name: "sdmx_{sdmx_info["flow_id"]}"',
            '      type: "sdmx"',
            "      args:",
            f'        endpoint: "{url}"',
            f'        flow: "{sdmx_info["flow_id"]}"',
            "      primary: true",
        ]
    return block_http_file(url, "sdmx")


def block_sparql(endpoint: str, query_hint: str = "") -> list[str]:
    """Blocchi raw.sources per endpoint SPARQL.

    Genera un source ``sparql`` con endpoint e query.
    Se non viene fornita una query, usa una SELECT base.
    """
    q = query_hint or "SELECT * WHERE { ?s ?p ?o } LIMIT 1000"
    return [
        '    - name: "sparql"',
        '      type: "sparql"',
        "      args:",
        f'        endpoint: "{endpoint}"',
        f'        query: "{q}"',
        "      primary: true",
    ]
