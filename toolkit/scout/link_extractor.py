"""Estrattore e raggruppatore di link dati da pagine HTML.

Contratto centrale per l'estrazione di link a file dati da pagine HTML.
Unifica le precedenti implementazioni in:
- ``toolkit.scout.http.extract_candidate_links`` (toolkit)
- ``collectors.html._extract_data_links`` (source-observatory)

Consumer:
  - ``toolkit.scout.http``  → redirect a questo modulo
  - ``toolkit.scaffold.sources``  → raggruppamento per scaffold
  - ``toolkit.mcp.scout_ops``  → MCP tool
  - ``source-observatory/collectors/html.py``  → inventory HTML
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from html.parser import HTMLParser
from urllib.parse import urljoin

# ---------------------------------------------------------------------------
# Costanti
# ---------------------------------------------------------------------------

# Estensioni di file dati riconosciute. Ordinate per lunghezza decrescente
# per matchare .xlsx prima di .xls e .geojson prima di .json.
DATA_EXTENSIONS = {
    ".csv",
    ".json",
    ".xlsx",
    ".xls",
    ".ods",
    ".zip",
    ".xml",
    ".parquet",
    ".geojson",
}

# Pattern per estrarre prefisso e anni dal filename
_PREFIX_RE = re.compile(r"^([A-Z][A-Z0-9]{1,7})")
_YEAR_RE = re.compile(r"(20[012]\d)")

# ---------------------------------------------------------------------------
# Dataclass
# ---------------------------------------------------------------------------


@dataclass
class DataLink:
    """Un link a un file dati estratto da HTML.

    Attributes:
        url: URL assoluto del file.
        format: Formato in maiuscolo (CSV, XLSX, ZIP, ...).
        title: Titolo estratto da aria-label o title attribute.
        prefix: Prefisso categorico estratto dal filename.
        years: Anni (20xx) trovati nel filename.
        page_url: URL della pagina HTML da cui è stato estratto.
    """

    url: str
    format: str
    title: str = ""
    prefix: str = ""
    years: list[int] = field(default_factory=list)
    page_url: str = ""


@dataclass
class LinkGroup:
    """Gruppo di link che appartengono allo stesso dataset.

    Attributes:
        group_id: Identificatore univoco del gruppo (es. prefisso).
        title: Titolo descrittivo del gruppo.
        prefix: Prefisso comune.
        year_range: Anni minimo e massimo coperti.
        formats: Formati disponibili.
        links: Lista dei DataLink del gruppo.
        count: Numero di link nel gruppo.
    """

    group_id: str
    title: str
    prefix: str
    year_range: list[int]
    formats: set[str]
    links: list[DataLink]
    count: int = 0


# ---------------------------------------------------------------------------
# Parsing HTML
# ---------------------------------------------------------------------------


def _extract_filename(url: str) -> str:
    """Estrae il filename (senza estensione) da un URL."""
    from urllib.parse import unquote, urlparse

    path = urlparse(url).path
    # Rimuovi query string
    name = path.rsplit("/", 1)[-1] if "/" in path else path
    name = unquote(name)
    # Rimuovi estensione
    dot = name.rfind(".")
    if dot > 0:
        name = name[:dot]
    return name


def _extract_prefix(filename: str) -> str:
    """Estrae un prefisso categorico dal filename.

    - Se contiene underscore, prende la prima parte.
    - Altrimenti matcha una run di maiuscole/digits all'inizio.
    - Fallback: primi 6 caratteri.
    """
    if "_" in filename:
        return filename.split("_")[0]
    m = _PREFIX_RE.match(filename)
    if m:
        return m.group(1)
    return filename[:6]


def _extract_years(filename: str) -> list[int]:
    """Estrae tutti gli anni (20xx) presenti nel filename."""
    return [int(y) for y in _YEAR_RE.findall(filename)]


def _resolve_format(url: str) -> str | None:
    """Determina il formato dall'estensione del file nell'URL."""
    lower = url.lower()
    # Ordina per lunghezza decrescente: .xlsx prima di .xls, .geojson prima di .json
    for ext in sorted(DATA_EXTENSIONS, key=len, reverse=True):
        # Cerca l'estensione nel path (non nella query string)
        path = url.split("?")[0].lower()
        if path.endswith(ext):
            return ext.lstrip(".").upper()
        # fallback: cerca estensione in tutto l'URL
        check = lower.rstrip("/")
        if check.endswith(ext):
            return ext.lstrip(".").upper()
    return None


# ---------------------------------------------------------------------------
# Parser HTML
# ---------------------------------------------------------------------------


class _DataLinkParser(HTMLParser):
    """Parser HTML che estrae link a file dati."""

    def __init__(self, base_url: str) -> None:
        super().__init__()
        self.base_url = base_url
        self.links: list[DataLink] = []
        self._seen: set[str] = set()

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() not in ("a", "area"):
            return
        attrs_dict = dict(attrs)
        href = attrs_dict.get("href", "") or attrs_dict.get("xlink:href", "")
        if not href:
            return
        # Salta ancore, mailto, tel, javascript
        if href.startswith(("#", "mailto:", "tel:", "javascript:")):
            return

        full_url = urljoin(self.base_url, href)
        fmt = _resolve_format(full_url)
        if fmt is None:
            return

        # Deduplica per URL
        if full_url in self._seen:
            return
        self._seen.add(full_url)

        title = (attrs_dict.get("aria-label") or attrs_dict.get("title") or "").strip()
        filename = _extract_filename(full_url)
        prefix = _extract_prefix(filename)
        years = _extract_years(filename)

        self.links.append(
            DataLink(
                url=full_url,
                format=fmt,
                title=title,
                prefix=prefix,
                years=years,
            )
        )


# ---------------------------------------------------------------------------
# Funzioni pubbliche
# ---------------------------------------------------------------------------


def extract_data_links(
    base_url: str,
    html: str,
    *,
    data_extensions: set[str] | None = None,
) -> list[DataLink]:
    """Estrae link a file dati da HTML.

    Args:
        base_url: URL di base per risolvere link relativi.
        html: Testo HTML da parsare.
        data_extensions: Estensioni da riconoscere (default: DATA_EXTENSIONS).

    Returns:
        Lista di DataLink, deduplicati per URL, ordinati per apparizione.
    """
    # Salva e ripristina le estensioni globali se parametro fornito
    global DATA_EXTENSIONS
    if data_extensions is not None:
        _original_exts = DATA_EXTENSIONS
        DATA_EXTENSIONS = data_extensions

    try:
        parser = _DataLinkParser(base_url)
        parser.feed(html)
        return parser.links
    except Exception:
        return []
    finally:
        if data_extensions is not None:
            DATA_EXTENSIONS = _original_exts


def group_links(links: list[DataLink]) -> list[LinkGroup]:
    """Raggruppa link in dataset basandosi su prefisso e pattern URL.

    Strategia:
    1. Raggruppa per ``prefix`` (quando presente).
    2. All'interno dello stesso prefix, se gli URL differiscono solo per
       anno/data/versione, appartengono allo stesso dataset.
    3. Link senza prefix vanno in un gruppo ``other``.

    Args:
        links: Lista di DataLink da raggruppare.

    Returns:
        Lista di LinkGroup ordinata per dimensione discendente.
    """
    # Fase 1: raggruppa per prefisso
    by_prefix: dict[str, list[DataLink]] = {}
    for link in links:
        p = link.prefix or "_other"
        by_prefix.setdefault(p, []).append(link)

    groups: list[LinkGroup] = []

    for prefix, group_links in by_prefix.items():
        years: set[int] = set()
        formats: set[str] = set()
        for link in group_links:
            years.update(link.years)
            formats.add(link.format)

        group_id = prefix if prefix != "_other" else "other"
        year_list = sorted(years)
        title = group_links[0].title or prefix

        groups.append(
            LinkGroup(
                group_id=group_id,
                title=title,
                prefix=prefix if prefix != "_other" else "",
                year_range=year_list,
                formats=formats,
                links=group_links,
                count=len(group_links),
            )
        )

    # Ordina per count decrescente
    groups.sort(key=lambda g: g.count, reverse=True)
    return groups


# ---------------------------------------------------------------------------
# Backward compatibility: bridge da extract_candidate_links a DataLink
# ---------------------------------------------------------------------------


def extract_candidate_links(base_url: str, html_text: str) -> list[str]:
    """Versione legacy: ritorna solo URL (nessun metadato).

    Mantenuta per backward compatibility con codice che importa
    ``extract_candidate_links`` da ``toolkit.scout.http``.
    """
    links = extract_data_links(base_url, html_text)
    return [link.url for link in links]
