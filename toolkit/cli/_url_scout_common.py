"""Helper condivisi per URL scout — slug, estensione, filename, scaffold YAML.

Tutta logica pura (no HTTP, no I/O). Condivisa tra init --url e inspect url.
"""

from __future__ import annotations

import re
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


def slugify(url: str) -> str:
    """Genera uno slug stabile e univoco per un URL.

    Usa uuid5 (namespace URL) per garantire unicità: stesso URL
    produce sempre lo stesso slug. Utile come dataset name/dir.
    """
    parsed = urlparse(url)
    stem = Path(parsed.path).stem or "dataset"
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
# Scaffold YAML — raw.sources block
# ---------------------------------------------------------------------------


def _make_source_name(link: str) -> str:
    stem = Path(urlparse(link).path).stem
    return re.sub(r"[^a-z0-9_]", "_", (stem or "resource").lower())


def _infer_type_from_url(u: str) -> str:
    if "/datastore/dump/" in u or "/datastore_search" in u:
        return "ckan"
    if "sdmx" in u.lower() or "/dataflow/" in u.lower():
        return "sdmx"
    return "http_file"


def _generate_raw_sources_block(url: str, slug: str, fname: str | None = None) -> list[str]:
    """Genera il blocco YAML raw.sources per un singolo URL.

    Args:
        url: URL della fonte dati.
        slug: Slug del dataset (usato per source name).
        fname: Nome file esplicito. Se None, inferito dall'URL.
    """
    parsed = urlparse(url)
    if fname is None:
        fname = Path(parsed.path).name or f"{slug}.csv"
    stype = _infer_type_from_url(url)
    lines = [
        f'    - name: "{slug}_source"',
        f'      type: "{stype}"',
        "      args:",
        f'        url: "{url}"',
        f'        filename: "{fname}"',
    ]
    lines.append("      primary: true")
    return lines


def generate_yaml_scaffold(
    probe_result: dict[str, Any],
    ckan_resources: list[dict[str, Any]] | None = None,
    candidate_links: list[str] | None = None,
) -> str:
    """Genera scaffold YAML per un URL ispezionato.

    Usato da inspect url --scaffold. Il probe_result deve contenere almeno
    final_url. Opzionalmente si possono passare risorse CKAN o link candidati
    per popolare raw.sources in modo strutturato.
    """
    url = probe_result["final_url"]
    parsed = urlparse(url)
    slug = slugify(url)
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
        lines.extend(_generate_raw_sources_block(url, slug))
    lines.append("")
    return "\n".join(lines)
