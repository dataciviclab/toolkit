"""Smoke test: toolkit scout con URL reali.

Questi test chiamano URL pubblici reali. NON vengono eseguiti in CI —
esclusi via pytest.ini (markers con --strict-markers ma non in --default-
markers) e via ci.yml (--ignore tests/test_smoke_scout.py).

Per eseguirli:
    pytest tests/test_smoke_scout.py -v --timeout=60
"""

from __future__ import annotations

import pytest

from toolkit.scout.infer import infer_years
from toolkit.scout.probe import probe_url, probe_url_routed


@pytest.mark.smoke
def test_scout_direct_csv() -> None:
    """CSV diretto: probe → file, formato CSV."""
    url = "https://www1.finanze.gov.it/finanze/analisi_stat/public/v_4_0_0/contenuti/REG_tipo_reddito_2025.csv?d=1615465800"
    result = probe_url(url, timeout=30)
    assert result["status_code"] == 200
    assert result["kind"] == "file"


@pytest.mark.smoke
def test_scout_html_with_links() -> None:
    """Pagina HTML con link CSV: probe → html, link trovati."""
    url = "https://www1.finanze.gov.it/finanze/analisi_stat/public/index.php?opendata=yes"
    result = probe_url(url, timeout=30, capture_html=True)
    assert result["status_code"] == 200
    assert result["kind"] == "html"
    assert len(result.get("candidate_links", [])) > 50  # SO registry dice 147


@pytest.mark.smoke
def test_scout_routed_ckan_dataset() -> None:
    """CKAN dataset: probe_routed → ckan, risorse trovate."""
    url = "https://dati.consip.it/dataset/dataset-bandi-e-gare"
    result = probe_url_routed(url, timeout=30)
    assert result["source_type"] == "ckan"
    assert len(result.get("ckan_resources") or []) > 0


@pytest.mark.smoke
def test_scout_routed_ckan_homepage() -> None:
    """CKAN homepage: probe_routed → ckan_portal."""
    url = "https://dati.consip.it"
    result = probe_url_routed(url, timeout=30)
    assert result["source_type"] == "ckan"
    assert result.get("ckan_portal") is True


@pytest.mark.smoke
def test_scout_infer_years_real() -> None:
    """infer_years su filename reali (pure, no network)."""
    assert infer_years("CivileFlussi20142025.xlsx") == (2014, 2025)
    assert infer_years("Dati20182022.csv") == (2018, 2022)
    assert infer_years("REG_tipo_reddito_2025.csv") == (2025, 2025)
