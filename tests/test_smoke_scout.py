"""Smoke test: toolkit scout.

I test di rete (URL pubblici reali) sono dietro ``RUN_NETWORK_SMOKE=1``:
non girano nello step smoke CI (``-m smoke``) per non creare flakiness
dipendente da endpoint esterni. Il test di inferenza puro gira sempre.

Per eseguire l'intero file:
    RUN_NETWORK_SMOKE=1 pytest tests/test_smoke_scout.py -v -m "" --timeout=60
"""

from __future__ import annotations

import os

import pytest

from toolkit.scout.infer import infer_years

needs_network = pytest.mark.skipif(
    not os.environ.get("RUN_NETWORK_SMOKE"),
    reason="network smoke: set RUN_NETWORK_SMOKE=1 per eseguire",
)


@pytest.mark.smoke
def test_scout_infer_years_real() -> None:
    """infer_years su filename reali (pure, no network)."""
    assert infer_years("CivileFlussi20142025.xlsx") == (2014, 2025)
    assert infer_years("Dati20182022.csv") == (2018, 2022)
    assert infer_years("REG_tipo_reddito_2025.csv") == (2025, 2025)


@needs_network
def test_scout_direct_csv() -> None:
    """CSV diretto: probe → file, formato CSV."""
    from toolkit.scout.probe import probe_url

    url = "https://www1.finanze.gov.it/finanze/analisi_stat/public/v_4_0_0/contenuti/REG_tipo_reddito_2025.csv?d=1615465800"
    result = probe_url(url, timeout=30)
    assert result["status_code"] == 200
    assert result["kind"] == "file"


@needs_network
def test_scout_html_with_links() -> None:
    """Pagina HTML con link CSV: probe → html, link trovati."""
    from toolkit.scout.probe import probe_url

    url = "https://www1.finanze.gov.it/finanze/analisi_stat/public/index.php?opendata=yes"
    result = probe_url(url, timeout=30, capture_html=True)
    assert result["status_code"] == 200
    assert result["kind"] == "html"
    assert len(result.get("candidate_links", [])) > 50  # SO registry dice 147


@needs_network
def test_scout_routed_ckan_dataset() -> None:
    """CKAN dataset: probe_routed → ckan, risorse trovate."""
    from toolkit.scout.probe import probe_url_routed

    url = "https://dati.consip.it/dataset/dataset-bandi-e-gare"
    result = probe_url_routed(url, timeout=30)
    assert result["source_type"] == "ckan"
    assert len(result.get("ckan_resources") or []) > 0


@needs_network
def test_scout_routed_ckan_homepage() -> None:
    """CKAN homepage: probe_routed → ckan_portal."""
    from toolkit.scout.probe import probe_url_routed

    url = "https://dati.consip.it"
    result = probe_url_routed(url, timeout=30)
    assert result["source_type"] == "ckan"
    assert result.get("ckan_portal") is True
