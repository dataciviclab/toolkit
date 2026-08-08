from __future__ import annotations

import pytest
import requests

from lab_connectors.http import HttpClient, HttpResult

from toolkit.core.exceptions import DownloadError
from toolkit.plugins.sdmx import SdmxSource

pytestmark = pytest.mark.adapter


@pytest.fixture(autouse=True)
def _clear_sdmx_cache():
    """Reset SdmxSource cache tra i test per evitare contaminazione.

    Cache ora è per istanza (non più condivisa tra classi), ma la fixture
    resta come safety net per eventuali cache residue in tests che
    riusano lo stesso SdmxSource.
    """


class _FakeResponse:
    def __init__(self, status_code: int, text: str, url: str):
        self.status_code = status_code
        self.text = text
        self.url = url


DATAFLOW_XML = """<?xml version="1.0" encoding="UTF-8"?>
<mes:Structure xmlns:mes="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message"
               xmlns:str="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure">
  <mes:Structures>
    <str:Dataflows>
      <str:Dataflow id="22_289">
        <str:Structure>
          <Ref id="DCIS_POPRES1" version="1.5" agencyID="IT1" package="datastructure" class="DataStructure" />
        </str:Structure>
      </str:Dataflow>
    </str:Dataflows>
  </mes:Structures>
</mes:Structure>
"""

DATA_JSON = """
{
  "dataSets": [
    {
      "series": {
        "0:0:0:0:0:0": {
          "observations": {
            "0": [2634],
            "1": [2621]
          }
        }
      }
    }
  ],
  "structure": {
    "dimensions": {
      "series": [
        {"id": "FREQ", "values": [{"id": "A", "name": "annual"}]},
        {"id": "REF_AREA", "values": [{"id": "001001", "name": "Agliè"}]},
        {"id": "DATA_TYPE", "values": [{"id": "JAN", "name": "population on 1st January"}]},
        {"id": "SEX", "values": [{"id": "9", "name": "total"}]},
        {"id": "AGE", "values": [{"id": "TOTAL", "name": "total"}]},
        {"id": "MARITAL_STATUS", "values": [{"id": "99", "name": "total"}]}
      ],
      "observation": [
        {
          "id": "TIME_PERIOD",
          "values": [
            {"id": "2024", "name": "2024"},
            {"id": "2025", "name": "2025"}
          ]
        }
      ]
    }
  }
}
"""

PREVIEW_JSON_WITH_VALUES = """
{
  "dataSets": [{"series": {}}],
  "structure": {
    "dimensions": {
      "series": [
        {"id": "FREQ", "values": [{"id": "A", "name": "annual"}]},
        {"id": "REF_AREA", "values": [{"id": "001001", "name": "Agliè"}]},
        {"id": "DATA_TYPE", "values": [{"id": "JAN", "name": "population on 1st January"}]},
        {"id": "SEX", "values": [{"id": "9", "name": "total"}]},
        {"id": "AGE", "values": [{"id": "TOTAL", "name": "total"}]},
        {"id": "MARITAL_STATUS", "values": [{"id": "99", "name": "total"}]}
      ],
      "observation": [
        {"id": "TIME_PERIOD", "values": [{"id": "2024", "name": "2024"}]}
      ]
    }
  }
}
"""


def _ok(result, err=None):
    return HttpResult(response=result, err=err)


def test_sdmx_fetch_normalizes_csv(monkeypatch):
    calls = []

    def _fake_get(self, url, **kwargs):
        params = kwargs.get("params")
        headers = kwargs.get("headers", {})
        calls.append((url, params, headers.get("Accept") if headers else None))
        if url.endswith("/dataflow/IT1/22_289"):
            return _ok(_FakeResponse(200, DATAFLOW_XML, url))
        if url.endswith("/data/IT1,22_289,1.5/all"):
            return _ok(_FakeResponse(200, PREVIEW_JSON_WITH_VALUES, url))
        if url.endswith("/data/IT1,22_289,1.5/A.001001.JAN.9.TOTAL.99"):
            return _ok(_FakeResponse(200, DATA_JSON, url))
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr(HttpClient, "get", _fake_get)

    payload, origin = SdmxSource(retries=1).fetch(
        "IT1",
        "22_289",
        "1.5",
        {
            "FREQ": "A",
            "REF_AREA": "001001",
            "DATA_TYPE": "JAN",
            "SEX": "9",
            "AGE": "TOTAL",
            "MARITAL_STATUS": "99",
        },
    )

    text = payload.decode("utf-8")
    assert origin.endswith("/data/IT1,22_289,1.5/A.001001.JAN.9.TOTAL.99")
    assert "FREQ,FREQ_label" in text
    assert "A,annual" in text
    assert "001001,Agliè" in text
    assert "2024,2024,2634" in text
    assert any(call[2] == "application/json" for call in calls)
    assert any(call[1] == {"firstNObservations": "0"} for call in calls)


CSV_RESPONSE = "FREQ,REF_AREA,DATA_TYPE_AGGR,VALUATION,TIME_PERIOD,OBS_VALUE\nA,ITC1,B1GQ_B_W2_S1,V,2023,156210.8\nA,ITC4,B1GQ_B_W2_S1,V,2023,489864.2\n"

XML_RESPONSE = """<?xml version="1.0" encoding="utf-8"?>
<message:GenericData xmlns:message="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message"
  xmlns:generic="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic">
  <message:DataSet>
    <generic:Obs>
      <generic:ObsDimension value="2023"/>
      <generic:ObsValue value="156210.8"/>
    </generic:Obs>
  </message:DataSet>
</message:GenericData>
"""


def test_sdmx_fetch_falls_back_on_nonjson_response(monkeypatch):
    """SDMX endpoint returns 200 with XML body → JSON parse fails → CSV fallback."""
    calls = []

    def _fake_get(self, url, **kwargs):
        headers = kwargs.get("headers", {})
        accept = headers.get("Accept") if headers else None
        calls.append((url, accept))
        if url.endswith("/dataflow/IT1/22_289"):
            return _ok(_FakeResponse(200, DATAFLOW_XML, url))
        if url.endswith("/data/IT1,22_289,1.5/all"):
            return _ok(_FakeResponse(200, PREVIEW_JSON_WITH_VALUES, url))
        if url.endswith("/data/IT1,22_289,1.5/A.001001.JAN.9.TOTAL.99"):
            # Prima chiamata con Accept: application/json → restituisce XML
            if accept == "application/json":
                return _ok(_FakeResponse(200, XML_RESPONSE, url))
            # Seconda chiamata con Accept: text/csv → restituisce CSV
            if accept == "text/csv":
                return _ok(_FakeResponse(200, CSV_RESPONSE, url))
        raise AssertionError(f"Unexpected URL {url} accept={accept}")

    monkeypatch.setattr(HttpClient, "get", _fake_get)

    payload, origin = SdmxSource(retries=1).fetch(
        "IT1",
        "22_289",
        "1.5",
        {
            "FREQ": "A",
            "REF_AREA": "001001",
            "DATA_TYPE": "JAN",
            "SEX": "9",
            "AGE": "TOTAL",
            "MARITAL_STATUS": "99",
        },
    )

    text = payload.decode("utf-8")
    assert origin.endswith("/data/IT1,22_289,1.5/A.001001.JAN.9.TOTAL.99")
    # Deve restituire i dati CSV crudi, non JSON normalizzato con _label
    assert "FREQ,REF_AREA,DATA_TYPE_AGGR" in text
    assert "ITC1,B1GQ_B_W2_S1" in text
    # Verifica che JSON sia stato tentato e CSV sia stato usato come fallback
    json_calls = [c for c in calls if c[1] == "application/json" and "/data/IT1" in str(c[0])]
    csv_calls = [c for c in calls if c[1] == "text/csv"]
    assert len(json_calls) >= 1
    assert len(csv_calls) >= 1


def test_sdmx_fetch_blocks_version_mismatch(monkeypatch):
    def _fake_get(self, url, **kwargs):
        if url.endswith("/dataflow/IT1/22_289"):
            return _ok(_FakeResponse(200, DATAFLOW_XML, url))
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr(HttpClient, "get", _fake_get)

    try:
        SdmxSource().fetch("IT1", "22_289", "1.0", {"FREQ": "A"})
    except DownloadError as exc:
        assert "current version is 1.5" in str(exc)
    else:
        raise AssertionError("Expected DownloadError")


def test_sdmx_fetch_rejects_unknown_filter_dimension(monkeypatch):
    def _fake_get(self, url, **kwargs):
        if url.endswith("/dataflow/IT1/22_289"):
            return _ok(_FakeResponse(200, DATAFLOW_XML, url))
        if url.endswith("/data/IT1,22_289,1.5/all"):
            return _ok(_FakeResponse(200, PREVIEW_JSON_WITH_VALUES, url))
        return _ok(_FakeResponse(404, "not found", url))

    monkeypatch.setattr(HttpClient, "get", _fake_get)

    try:
        SdmxSource().fetch("IT1", "22_289", "1.5", {"NOT_A_VALID_DIM": "X"})
    except DownloadError as exc:
        assert "Unknown SDMX filter dimensions" in str(exc)
        assert "NOT_A_VALID_DIM" in str(exc)
    else:
        raise AssertionError("Expected DownloadError")


def test_sdmx_fetch_falls_back_on_metadata_timeout(monkeypatch):
    calls = []

    def _fake_get(self, url, **kwargs):
        calls.append(url)
        if url == "https://sdmx.istat.it/SDMXWS/rest/dataflow/IT1/22_289":
            return HttpResult(response=None, err=requests.exceptions.Timeout("metadata timeout"))
        if url == "https://esploradati.istat.it/SDMXWS/rest/dataflow/IT1/22_289":
            return _ok(_FakeResponse(200, DATAFLOW_XML, url))
        if url == "https://esploradati.istat.it/SDMXWS/rest/data/IT1,22_289,1.5/all":
            return _ok(_FakeResponse(200, PREVIEW_JSON_WITH_VALUES, url))
        if (
            url
            == "https://esploradati.istat.it/SDMXWS/rest/data/IT1,22_289,1.5/A.001001.JAN.9.TOTAL.99"
        ):
            return _ok(_FakeResponse(200, DATA_JSON, url))
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr(HttpClient, "get", _fake_get)

    payload, origin = SdmxSource(retries=1).fetch(
        "IT1",
        "22_289",
        "1.5",
        {
            "FREQ": "A",
            "REF_AREA": "001001",
            "DATA_TYPE": "JAN",
            "SEX": "9",
            "AGE": "TOTAL",
            "MARITAL_STATUS": "99",
        },
    )

    assert origin.endswith("/data/IT1,22_289,1.5/A.001001.JAN.9.TOTAL.99")
    assert payload.decode("utf-8").startswith("FREQ,FREQ_label")
    assert calls[:2] == [
        "https://sdmx.istat.it/SDMXWS/rest/dataflow/IT1/22_289",
        "https://esploradati.istat.it/SDMXWS/rest/dataflow/IT1/22_289",
    ]


def test_sdmx_fetch_rejects_invalid_filter_value(monkeypatch):
    def _fake_get(self, url, **kwargs):
        if url.endswith("/dataflow/IT1/22_289"):
            return _ok(_FakeResponse(200, DATAFLOW_XML, url))
        if url.endswith("/data/IT1,22_289,1.5/all"):
            return _ok(_FakeResponse(200, PREVIEW_JSON_WITH_VALUES, url))
        return _ok(_FakeResponse(404, "not found", url))

    monkeypatch.setattr(HttpClient, "get", _fake_get)

    try:
        SdmxSource().fetch("IT1", "22_289", "1.5", {"FREQ": "X"})
    except DownloadError as exc:
        assert "Invalid value(s) for SDMX dimension FREQ" in str(exc)
        assert "X" in str(exc)
    else:
        raise AssertionError("Expected DownloadError")


def test_sdmx_fetch_rejects_invalid_filter_value_list(monkeypatch):
    def _fake_get(self, url, **kwargs):
        if url.endswith("/dataflow/IT1/22_289"):
            return _ok(_FakeResponse(200, DATAFLOW_XML, url))
        if url.endswith("/data/IT1,22_289,1.5/all"):
            return _ok(_FakeResponse(200, PREVIEW_JSON_WITH_VALUES, url))
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr(HttpClient, "get", _fake_get)

    try:
        SdmxSource().fetch("IT1", "22_289", "1.5", {"REF_AREA": ["001001", "999999"]})
    except DownloadError as exc:
        assert "Invalid value(s) for SDMX dimension REF_AREA" in str(exc)
        assert "999999" in str(exc)
    else:
        raise AssertionError("Expected DownloadError")


def test_sdmx_fetch_empty_response(monkeypatch):
    empty_data_json = '{"dataSets":[{"series":{}}],"structure":{"dimensions":{"series":[{"id":"FREQ","values":[{"id":"A","name":"annual"}]}]}}}'

    def _fake_get(self, url, **kwargs):
        if url.endswith("/dataflow/IT1/22_289"):
            return _ok(_FakeResponse(200, DATAFLOW_XML, url))
        if url.endswith("/data/IT1,22_289,1.5/all"):
            return _ok(_FakeResponse(200, empty_data_json, url))
        return _ok(_FakeResponse(200, empty_data_json, url))

    monkeypatch.setattr(HttpClient, "get", _fake_get)

    try:
        SdmxSource().fetch("IT1", "22_289", "1.5", {"FREQ": "A"})
    except DownloadError as exc:
        assert "no rows" in str(exc)
    else:
        raise AssertionError("Expected DownloadError")


def test_sdmx_fetch_falls_back_on_data_5xx(monkeypatch):
    calls = []

    def _fake_get(self, url, **kwargs):
        calls.append(url)
        if url == "https://sdmx.istat.it/SDMXWS/rest/dataflow/IT1/22_289":
            return _ok(_FakeResponse(200, DATAFLOW_XML, url))
        if url == "https://esploradati.istat.it/SDMXWS/rest/data/IT1,22_289,1.5/all":
            return _ok(_FakeResponse(500, "boom", url))
        if url == "https://sdmx.istat.it/SDMXWS/rest/data/IT1,22_289,1.5/all":
            return _ok(_FakeResponse(200, PREVIEW_JSON_WITH_VALUES, url))
        if (
            url
            == "https://esploradati.istat.it/SDMXWS/rest/data/IT1,22_289,1.5/A.001001.JAN.9.TOTAL.99"
        ):
            return _ok(_FakeResponse(500, "boom", url))
        if url == "https://sdmx.istat.it/SDMXWS/rest/data/IT1,22_289,1.5/A.001001.JAN.9.TOTAL.99":
            return _ok(_FakeResponse(200, DATA_JSON, url))
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr(HttpClient, "get", _fake_get)

    payload, origin = SdmxSource(
        retries=1,
        data_base_url="https://esploradati.istat.it/SDMXWS/rest",
        metadata_base_url="https://sdmx.istat.it/SDMXWS/rest",
    ).fetch(
        "IT1",
        "22_289",
        "1.5",
        {
            "FREQ": "A",
            "REF_AREA": "001001",
            "DATA_TYPE": "JAN",
            "SEX": "9",
            "AGE": "TOTAL",
            "MARITAL_STATUS": "99",
        },
    )

    assert origin == "https://sdmx.istat.it/SDMXWS/rest/data/IT1,22_289,1.5/A.001001.JAN.9.TOTAL.99"
    assert payload.decode("utf-8").startswith("FREQ,FREQ_label")


def test_sdmx_fetch_does_not_fallback_on_404(monkeypatch):
    def _fake_get(self, url, **kwargs):
        if url == "https://sdmx.istat.it/SDMXWS/rest/dataflow/IT1/22_289":
            return _ok(_FakeResponse(200, DATAFLOW_XML, url))
        if url == "https://esploradati.istat.it/SDMXWS/rest/data/IT1,22_289,1.5/all":
            return _ok(_FakeResponse(200, PREVIEW_JSON_WITH_VALUES, url))
        if (
            url
            == "https://esploradati.istat.it/SDMXWS/rest/data/IT1,22_289,1.5/A.001001.JAN.9.TOTAL.99"
        ):
            return _ok(_FakeResponse(404, "not found", url))
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr(HttpClient, "get", _fake_get)

    try:
        SdmxSource(
            retries=1,
            data_base_url="https://esploradati.istat.it/SDMXWS/rest",
            metadata_base_url="https://sdmx.istat.it/SDMXWS/rest",
        ).fetch(
            "IT1",
            "22_289",
            "1.5",
            {
                "FREQ": "A",
                "REF_AREA": "001001",
                "DATA_TYPE": "JAN",
                "SEX": "9",
                "AGE": "TOTAL",
                "MARITAL_STATUS": "99",
            },
        )
    except DownloadError as exc:
        assert "HTTP 404" in str(exc)
    else:
        raise AssertionError("Expected DownloadError")


def test_sdmx_fetch_does_not_fallback_on_connection_error(monkeypatch):
    calls = []

    def _fake_get(self, url, **kwargs):
        calls.append(url)
        if url == "https://sdmx.istat.it/SDMXWS/rest/dataflow/IT1/22_289":
            return _ok(_FakeResponse(200, DATAFLOW_XML, url))
        if url == "https://esploradati.istat.it/SDMXWS/rest/data/IT1,22_289,1.5/all":
            return _ok(_FakeResponse(200, PREVIEW_JSON_WITH_VALUES, url))
        if (
            url
            == "https://esploradati.istat.it/SDMXWS/rest/data/IT1,22_289,1.5/A.001001.JAN.9.TOTAL.99"
        ):
            return HttpResult(
                response=None, err=requests.exceptions.ConnectionError("tls handshake failed")
            )
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr(HttpClient, "get", _fake_get)

    try:
        SdmxSource(
            retries=1,
            data_base_url="https://esploradati.istat.it/SDMXWS/rest",
            metadata_base_url="https://sdmx.istat.it/SDMXWS/rest",
        ).fetch(
            "IT1",
            "22_289",
            "1.5",
            {
                "FREQ": "A",
                "REF_AREA": "001001",
                "DATA_TYPE": "JAN",
                "SEX": "9",
                "AGE": "TOTAL",
                "MARITAL_STATUS": "99",
            },
        )
    except DownloadError as exc:
        assert "connection error" in str(exc).lower()
    else:
        raise AssertionError("Expected DownloadError")

    assert calls == [
        "https://sdmx.istat.it/SDMXWS/rest/dataflow/IT1/22_289",
        "https://esploradati.istat.it/SDMXWS/rest/data/IT1,22_289,1.5/all",
        "https://esploradati.istat.it/SDMXWS/rest/data/IT1,22_289,1.5/A.001001.JAN.9.TOTAL.99",
    ]


def test_sdmx_cache_is_per_instance(monkeypatch):
    """Cache di struttura NON deve contaminare istanze con base URL diversi."""
    calls = []

    def _fake_get(self, url, **kwargs):
        calls.append(url)
        if url == "https://one.test/rest/dataflow/IT1/22_289":
            return _ok(
                _FakeResponse(200, DATAFLOW_XML.replace('version="1.5"', 'version="1.0"'), url)
            )
        if url == "https://two.test/rest/dataflow/IT1/22_289":
            return _ok(_FakeResponse(200, DATAFLOW_XML, url))  # version 1.5
        if url == "https://one.test/rest/data/IT1,22_289,1.0/all":
            return _ok(_FakeResponse(200, PREVIEW_JSON_WITH_VALUES, url))
        if url == "https://two.test/rest/data/IT1,22_289,1.5/all":
            return _ok(_FakeResponse(200, PREVIEW_JSON_WITH_VALUES, url))
        if url.endswith("/data/IT1,22_289,1.0/A.001001.JAN.9.TOTAL.99"):
            return _ok(_FakeResponse(200, DATA_JSON, url))
        if url.endswith("/data/IT1,22_289,1.5/A.001001.JAN.9.TOTAL.99"):
            return _ok(_FakeResponse(200, DATA_JSON, url))
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr(HttpClient, "get", _fake_get)

    # Istanza 1: endpoint one.test, version 1.0
    src1 = SdmxSource(
        timeout=5,
        retries=0,
        data_base_url="https://one.test/rest",
        metadata_base_url="https://one.test/rest",
    )
    src1.fetch(
        "IT1",
        "22_289",
        "1.0",
        {
            "FREQ": "A",
            "REF_AREA": "001001",
            "DATA_TYPE": "JAN",
            "SEX": "9",
            "AGE": "TOTAL",
            "MARITAL_STATUS": "99",
        },
    )

    # Istanza 2: endpoint two.test, version 1.5
    src2 = SdmxSource(
        timeout=5,
        retries=0,
        data_base_url="https://two.test/rest",
        metadata_base_url="https://two.test/rest",
    )
    src2.fetch(
        "IT1",
        "22_289",
        "1.5",
        {
            "FREQ": "A",
            "REF_AREA": "001001",
            "DATA_TYPE": "JAN",
            "SEX": "9",
            "AGE": "TOTAL",
            "MARITAL_STATUS": "99",
        },
    )

    # Verifica: src1 non deve aver contaminato src2
    # src2 deve aver chiamato il proprio dataflow endpoint (non usato cache di src1)
    expected_calls = [
        "https://one.test/rest/dataflow/IT1/22_289",
        "https://one.test/rest/data/IT1,22_289,1.0/all",
        "https://one.test/rest/data/IT1,22_289,1.0/A.001001.JAN.9.TOTAL.99",
        "https://two.test/rest/dataflow/IT1/22_289",  # ← NON in cache, chiamata reale
        "https://two.test/rest/data/IT1,22_289,1.5/all",
        "https://two.test/rest/data/IT1,22_289,1.5/A.001001.JAN.9.TOTAL.99",
    ]
    assert calls == expected_calls, f"Cache contamination! calls={calls}"


# ── Profilo Eurostat (ESTAT) ─────────────────────────────────────────────────

# SDMX-JSON 2.0 di Eurostat: `dimension.{id}.category.{index,label}`, flat.
ESTAT_CONSTRAINTS_JSON = """
{
  "id": ["freq", "unit", "geo", "time"],
  "size": [1, 7, 1814, 25],
  "dimension": {
    "freq": {"label": "Time frequency",
             "category": {"index": {"A": 0}, "label": {"A": "Annual"}}},
    "unit": {"label": "Unit of measure",
             "category": {"index": {"EUR_HAB": 0, "MIO_EUR": 1},
                          "label": {"EUR_HAB": "Euro per inhabitant", "MIO_EUR": "Million euro"}}},
    "geo": {"label": "Geopolitical entity (reporting)",
            "category": {"index": {"EU27_2020": 0, "IT": 1, "ITC4": 2},
                         "label": {"EU27_2020": "EU (27 countries)", "IT": "Italy", "ITC4": "Nord-Ovest"}}},
    "time": {"label": "Time",
             "category": {"index": {"2000": 0, "2001": 1},
                          "label": {"2000": "2000", "2001": "2001"}}}
  },
  "value": {}
}
"""

# TSV wide Eurostat: header `dims\TIME_PERIOD`, righe serie, `:` = missing,
# flag di qualità come lettera finale dopo spazio (es. `1200 d`).
ESTAT_TSV = (
    "freq,unit,geo\\TIME_PERIOD\t2000 \t2001 \n"
    "A,EUR_HAB,IT\t21900 \t23000 \n"
    "A,MIO_EUR,IT\t: \t1200 d \n"
)

ESTAT_TSV_MONTHLY = "freq,unit,geo\\TIME_PERIOD\t2024-01\t2024-02\nA,EUR_HAB,IT\t10 \t20 \n"


def _estat_fake_get(allowed_urls=None, calls=None):
    """Factory per mock HttpClient.get che serve constraints JSON 2.0 + TSV."""
    allowed_urls = allowed_urls or set()

    def _fake_get(self, url, **kwargs):
        if calls is not None:
            calls.append(url)
        params = kwargs.get("params") or {}
        if url.endswith("/data/NAMA_10R_3GDP/all") and params.get("format") == "JSON":
            return _ok(_FakeResponse(200, ESTAT_CONSTRAINTS_JSON, url))
        if url.endswith("/data/NAMA_10R_3GDP/all") and params.get("format") == "TSV":
            return _ok(_FakeResponse(200, ESTAT_TSV, url))
        if url.endswith("/data/NAMA_10R_3GDP/A.EUR_HAB") and params.get("format") == "TSV":
            return _ok(_FakeResponse(200, ESTAT_TSV, url))
        if url.endswith("/data/NAMA_10R_3GDP/A.EUR_HAB.IT") and params.get("format") == "TSV":
            return _ok(_FakeResponse(200, ESTAT_TSV, url))
        if not allowed_urls:
            raise AssertionError(f"Unexpected URL {url} params={params}")
        if any(url.endswith(u) for u in allowed_urls):
            return _ok(_FakeResponse(200, ESTAT_TSV, url))
        raise AssertionError(f"Unexpected URL {url} params={params}")

    return _fake_get


def test_sdmx_estat_fetch_tsv_normalizes(monkeypatch):
    """ESTAT: TSV wide → CSV long [dims, year, value, flag]; niente version check."""
    calls = []
    monkeypatch.setattr(HttpClient, "get", _estat_fake_get(calls=calls))

    # version vuota: il profilo ESTAT NON deve chiamare dataflow né fallire
    payload, origin = SdmxSource(retries=1).fetch("ESTAT", "NAMA_10R_3GDP", "")

    text = payload.decode("utf-8")
    assert origin.endswith("/data/NAMA_10R_3GDP/all?format=TSV") or origin.endswith(
        "/data/NAMA_10R_3GDP/all"
    )
    assert "freq,unit,geo,year,value,flag" in text
    assert "A,EUR_HAB,IT,2000,21900.0," in text
    assert "A,EUR_HAB,IT,2001,23000.0," in text
    # missing → value vuota; flag preservato
    assert "A,MIO_EUR,IT,2000,," in text
    assert "A,MIO_EUR,IT,2001,1200.0,d" in text
    # Nessuna chiamata a dataflow (niente version check per ESTAT)
    assert not any("/dataflow/" in c for c in calls)


def test_sdmx_estat_filters_build_key(monkeypatch):
    """ESTAT: filtri → key parziale nel path dati (ordine dims dal JSON 2.0)."""
    calls = []
    monkeypatch.setattr(HttpClient, "get", _estat_fake_get(calls=calls))

    payload, origin = SdmxSource(retries=1).fetch(
        "ESTAT", "NAMA_10R_3GDP", "", {"freq": "A", "unit": "EUR_HAB"}
    )

    assert "A,EUR_HAB,IT,2000,21900.0," in payload.decode("utf-8")
    assert any(c.endswith("/data/NAMA_10R_3GDP/A.EUR_HAB") for c in calls)


def test_sdmx_estat_rejects_invalid_filter_value(monkeypatch):
    """ESTAT: filtro con valore non valido → errore esplicito (constraints 2.0)."""
    monkeypatch.setattr(HttpClient, "get", _estat_fake_get())

    with pytest.raises(DownloadError, match=r"Invalid value\(s\) for SDMX dimension freq"):
        SdmxSource(retries=1).fetch("ESTAT", "NAMA_10R_3GDP", "", {"freq": "X"})


def test_sdmx_estat_rejects_unknown_filter_dimension(monkeypatch):
    """ESTAT: dimensione sconosciuta (non nel JSON 2.0) → errore esplicito."""
    monkeypatch.setattr(HttpClient, "get", _estat_fake_get())

    with pytest.raises(DownloadError, match=r"Unknown SDMX filter dimensions"):
        SdmxSource(retries=1).fetch("ESTAT", "NAMA_10R_3GDP", "", {"NOT_A_DIM": "X"})


def test_sdmx_estat_tsv_monthly(monkeypatch):
    """ESTAT: TSV mensile (YYYY-MM) → colonne year + month."""

    def _fake_get(self, url, **kwargs):
        params = kwargs.get("params") or {}
        if url.endswith("/data/NAMA_10R_3GDP/all") and params.get("format") == "JSON":
            return _ok(_FakeResponse(200, ESTAT_CONSTRAINTS_JSON, url))
        if url.endswith("/data/NAMA_10R_3GDP/all") and params.get("format") == "TSV":
            return _ok(_FakeResponse(200, ESTAT_TSV_MONTHLY, url))
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr(HttpClient, "get", _fake_get)

    payload, _origin = SdmxSource(retries=1).fetch("ESTAT", "NAMA_10R_3GDP", "")

    text = payload.decode("utf-8")
    assert "freq,unit,geo,year,month,value,flag" in text
    assert "A,EUR_HAB,IT,2024,1,10.0," in text
    assert "A,EUR_HAB,IT,2024,2,20.0," in text


ESTAT_CODELIST_JSON = """
{
  "version": "1.0",
  "class": "codelist",
  "label": "Geopolitical entity (reporting)",
  "category": {
    "index": {"IT": 0, "ITC4": 1},
    "label": {"IT": "Italy", "ITC4": "Nord-Ovest"}
  },
  "extension": {
    "lang": "EN",
    "id": "GEO",
    "version": "1.0",
    "code-annotation": {
      "IT": [
        {"type": "IS_STANDARD_CODE", "title": "Y"},
        {"type": "LEVEL", "title": "0"}
      ],
      "ITC4": [
        {"type": "IS_STANDARD_CODE", "title": "Y"},
        {"type": "LEVEL", "title": "2"}
      ]
    }
  }
}
"""


def test_sdmx_estat_fetch_codelist(monkeypatch):
    """ESTAT: codelist SDMX-JSON 2.0 → {codes, annotations (LEVEL per NUTS)}."""

    def _fake_get(self, url, **kwargs):
        params = kwargs.get("params") or {}
        if url.endswith("/codelist/ESTAT/GEO") and params.get("format") == "json":
            return _ok(_FakeResponse(200, ESTAT_CODELIST_JSON, url))
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr(HttpClient, "get", _fake_get)

    result = SdmxSource(retries=1).fetch_codelist("GEO")

    assert result["id"] == "GEO"
    assert result["codes"]["IT"] == "Italy"
    assert result["codes"]["ITC4"] == "Nord-Ovest"
    assert result["annotations"]["IT"]["LEVEL"] == "0"
    assert result["annotations"]["ITC4"]["LEVEL"] == "2"
    assert result["annotations"]["IT"]["IS_STANDARD_CODE"] == "Y"


def test_sdmx_fetch_codelist_requires_estat():
    """fetch_codelist: solo profilo ESTAT (ISTAT non espone JSON 2.0)."""
    with pytest.raises(DownloadError, match="solo per il profilo Eurostat"):
        SdmxSource().fetch_codelist("GEO", agency="IT1")


def test_sdmx_estat_large_flow_constraints_413_fallback(monkeypatch):
    """Flow giganti: JSON constraints HTTP 413 → fallback wildcard, TSV ok.

    DEMO_R_D2JAN (pop per sesso/età, 4M+ obs) fa fallire il JSON constraints
    con 413 — il source deve ripiegare su key wildcard ('all') e scaricare il
    TSV, non crashare. Regressione trovata durante la migrazione pop-nuts3.
    """

    def _fake_get_413_then_tsv(self, url, **kwargs):
        params = kwargs.get("params") or {}
        if url.endswith("/data/DEMO_R_D2JAN/all") and params.get("format") == "JSON":
            return _ok(_FakeResponse(413, "Payload Too Large", url))
        if url.endswith("/data/DEMO_R_D2JAN/all") and params.get("format") == "TSV":
            return _ok(_FakeResponse(200, ESTAT_TSV, url))
        raise AssertionError(f"Unexpected URL {url} params={params}")

    monkeypatch.setattr(HttpClient, "get", _fake_get_413_then_tsv)
    src = SdmxSource(timeout=30, retries=0)

    # Constraints: fallback a vuoti (no crash)
    constraints = src._estat_constraints("DEMO_R_D2JAN")
    assert constraints == {}

    # Fetch: la key diventa 'all' e il TSV viene scaricato
    data, _origin = src._estat_fetch_tsv("DEMO_R_D2JAN", "all")
    assert data  # TSV scaricato
