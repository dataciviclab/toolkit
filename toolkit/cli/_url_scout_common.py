"""Helper condivisi per URL scout tra init --url e inspect url.

Consolida slug generation, inferenza estensione e filename
per evitare divergenze tra cmd_init.py e cmd_url_inspect.py.
"""

from __future__ import annotations

import re
import uuid
from pathlib import Path
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
