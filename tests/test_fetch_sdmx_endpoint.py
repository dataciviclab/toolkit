"""Test per _fetch_sdmx — gestione endpoint per agenzie non-ISTAT/ESTAT.

Regressione review PR #450: lo scaffold emette `endpoint` per le agenzie
non-ESTAT; il fetch deve usarla come base SDMX (root derivato da una URL
con path dataflow/data), senza toccare i default ISTAT né il profilo ESTAT.
"""

from __future__ import annotations

import pytest

from toolkit.raw._fetch_utils import _fetch_sdmx, _sdmx_root_from_url

pytestmark = pytest.mark.contract


class _FakeSdmx:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.fetched: tuple = ()

    def fetch(self, agency, flow, version, filters):
        self.fetched = (agency, flow, version, filters)
        return b"payload", "https://fake.test"


@pytest.fixture
def fake_registry(monkeypatch):
    holder = {}
    import toolkit.raw._fetch_utils as fu

    def _create(name, **kwargs):
        src = _FakeSdmx(**kwargs)
        holder["source"] = src
        return src

    monkeypatch.setattr(fu.registry, "create", _create)
    return holder


def test_sdmx_root_from_url_dataflow():
    assert (
        _sdmx_root_from_url("https://esploradati.istat.it/SDMXWS/rest/dataflow/IT1/22_289")
        == "https://esploradati.istat.it/SDMXWS/rest"
    )


def test_sdmx_root_from_url_data_path():
    assert (
        _sdmx_root_from_url(
            "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/NAMA_10R_3GDP?format=TSV"
        )
        == "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1"
    )


def test_fetch_sdmx_endpoint_third_party(fake_registry):
    """Agenzia terza con endpoint: root derivato usato per data e metadata."""
    _fetch_sdmx(
        "sdmx",
        {},
        {
            "agency": "XX",
            "flow": "FLOW1",
            "endpoint": "https://sdmx.example.org/rest/dataflow/XX/FLOW1",
        },
    )
    src = fake_registry["source"]
    assert src.kwargs["data_base_url"] == "https://sdmx.example.org/rest"
    assert src.kwargs["metadata_base_url"] == "https://sdmx.example.org/rest"
    assert src.fetched[0] == "XX"
    assert src.fetched[1] == "FLOW1"


def test_fetch_sdmx_endpoint_ignored_for_istat(fake_registry):
    """ISTAT: endpoint nel config non altera i default del plugin."""
    _fetch_sdmx(
        "sdmx",
        {},
        {
            "agency": "IT1",
            "flow": "22_289",
            "version": "1.5",
            "endpoint": "https://esploradati.istat.it/SDMXWS/rest/dataflow/IT1/22_289",
        },
    )
    src = fake_registry["source"]
    assert "data_base_url" not in src.kwargs
    assert "metadata_base_url" not in src.kwargs
    assert src.fetched == ("IT1", "22_289", "1.5", None)


def test_fetch_sdmx_endpoint_ignored_for_estat(fake_registry):
    """ESTAT: endpoint nel config non altera l'auto-risoluzione del profilo."""
    _fetch_sdmx(
        "sdmx",
        {},
        {
            "agency": "ESTAT",
            "flow": "NAMA_10R_3GDP",
            "endpoint": "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/NAMA_10R_3GDP",
        },
    )
    src = fake_registry["source"]
    assert "data_base_url" not in src.kwargs
    assert src.fetched[0] == "ESTAT"
