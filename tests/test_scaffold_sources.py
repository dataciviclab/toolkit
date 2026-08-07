"""Tests per toolkit/scaffold/sources.py — block_sdmx (raw.sources SDMX).

Regressione: il blocco scaffoldato deve esprimere l'endpoint per le agenzie
non-ESTAT (parità col comportamento storico) e NON emetterlo per ESTAT
(profilo dedicato che auto-risolve l'endpoint Eurostat).
"""

from __future__ import annotations

import pytest

from toolkit.scaffold.sources import block_sdmx

pytestmark = pytest.mark.pure_unit


def _sdmx_info(flow_id: str, agency: str | None = None) -> dict:
    info = {"flow_id": flow_id, "year_min": 2000, "year_max": 2024}
    if agency is not None:
        info["agency"] = agency
    return info


def _has(lines: list[str], needle: str) -> bool:
    return any(needle in ln for ln in lines)


def test_block_sdmx_estat_no_endpoint():
    """ESTAT: solo flow + agency — l'endpoint è auto-risolto dal profilo."""
    lines = block_sdmx(
        _sdmx_info("NAMA_10R_3GDP", agency="ESTAT"),
        "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/NAMA_10R_3GDP",
    )
    assert _has(lines, 'type: "sdmx"')
    assert _has(lines, 'flow: "NAMA_10R_3GDP"')
    assert _has(lines, 'agency: "ESTAT"')
    assert not _has(lines, "endpoint:")


def test_block_sdmx_non_estat_keeps_endpoint():
    """Agenzie non-ESTAT (es. ISTAT): endpoint dell'URL scoperta dallo scout."""
    url = "https://esploradati.istat.it/SDMXWS/rest/dataflow/IT1/22_289"
    lines = block_sdmx(_sdmx_info("22_289", agency="IT1"), url)
    assert _has(lines, 'flow: "22_289"')
    assert _has(lines, f'endpoint: "{url}"')
    assert not _has(lines, "agency:")


def test_block_sdmx_no_agency_keeps_endpoint():
    """Agency non inferita: endpoint comunque presente (comportamento storico)."""
    url = "https://sdmx.example.org/rest/dataflow/XX/FLOW1"
    lines = block_sdmx(_sdmx_info("FLOW1"), url)
    assert _has(lines, f'endpoint: "{url}"')


def test_block_sdmx_falls_back_to_http_file():
    """Senza flow_id: fallback a http_file (comportamento storico)."""
    url = "https://example.org/some.csv"
    lines = block_sdmx(None, url)
    assert _has(lines, 'type: "http_file"')
    assert _has(lines, f'url: "{url}"')
