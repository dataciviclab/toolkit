from __future__ import annotations

import requests

from lab_connectors.http import HttpClient, HttpResult
from lab_connectors.testing import http_ok

from toolkit.core.exceptions import DownloadError
from toolkit.plugins.sdmx import SdmxSource


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


def test_sdmx_fetch_normalizes_csv(monkeypatch):
    calls = []

    def _fake_get(self, url, **kwargs):
        params = kwargs.get("params")
        headers = kwargs.get("headers", {})
        calls.append((url, params, headers.get("Accept") if headers else None))
        if url.endswith("/dataflow/IT1/22_289"):
            return http_ok(200, text=DATAFLOW_XML, url=url)
        if url.endswith("/data/IT1,22_289,1.5/all"):
            return http_ok(200, text=PREVIEW_JSON_WITH_VALUES, url=url)
        if url.endswith("/data/IT1,22_289,1.5/A.001001.JAN.9.TOTAL.99"):
            return http_ok(200, text=DATA_JSON, url=url)
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


def test_sdmx_fetch_blocks_version_mismatch(monkeypatch):
    def _fake_get(self, url, **kwargs):
        if url.endswith("/dataflow/IT1/22_289"):
            return http_ok(200, text=DATAFLOW_XML, url=url)
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
            return http_ok(200, text=DATAFLOW_XML, url=url)
        if url.endswith("/data/IT1,22_289,1.5/all"):
            return http_ok(200, text=PREVIEW_JSON_WITH_VALUES, url=url)
        return http_ok(404, text="not found", url=url)

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
            return http_ok(200, text=DATAFLOW_XML, url=url)
        if url == "https://esploradati.istat.it/SDMXWS/rest/data/IT1,22_289,1.5/all":
            return http_ok(200, text=PREVIEW_JSON_WITH_VALUES, url=url)
        if url == "https://esploradati.istat.it/SDMXWS/rest/data/IT1,22_289,1.5/A.001001.JAN.9.TOTAL.99":
            return http_ok(200, text=DATA_JSON, url=url)
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
            return http_ok(200, text=DATAFLOW_XML, url=url)
        if url.endswith("/data/IT1,22_289,1.5/all"):
            return http_ok(200, text=PREVIEW_JSON_WITH_VALUES, url=url)
        return http_ok(404, text="not found", url=url)

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
            return http_ok(200, text=DATAFLOW_XML, url=url)
        if url.endswith("/data/IT1,22_289,1.5/all"):
            return http_ok(200, text=PREVIEW_JSON_WITH_VALUES, url=url)
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
            return http_ok(200, text=DATAFLOW_XML, url=url)
        if url.endswith("/data/IT1,22_289,1.5/all"):
            return http_ok(200, text=empty_data_json, url=url)
        return http_ok(200, text=empty_data_json, url=url)

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
            return http_ok(200, text=DATAFLOW_XML, url=url)
        if url == "https://esploradati.istat.it/SDMXWS/rest/data/IT1,22_289,1.5/all":
            return http_ok(500, text="boom", url=url)
        if url == "https://sdmx.istat.it/SDMXWS/rest/data/IT1,22_289,1.5/all":
            return http_ok(200, text=PREVIEW_JSON_WITH_VALUES, url=url)
        if url == "https://esploradati.istat.it/SDMXWS/rest/data/IT1,22_289,1.5/A.001001.JAN.9.TOTAL.99":
            return http_ok(500, text="boom", url=url)
        if url == "https://sdmx.istat.it/SDMXWS/rest/data/IT1,22_289,1.5/A.001001.JAN.9.TOTAL.99":
            return http_ok(200, text=DATA_JSON, url=url)
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
            return http_ok(200, text=DATAFLOW_XML, url=url)
        if url == "https://esploradati.istat.it/SDMXWS/rest/data/IT1,22_289,1.5/all":
            return http_ok(200, text=PREVIEW_JSON_WITH_VALUES, url=url)
        if url == "https://esploradati.istat.it/SDMXWS/rest/data/IT1,22_289,1.5/A.001001.JAN.9.TOTAL.99":
            return http_ok(404, text="not found", url=url)
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
            return http_ok(200, text=DATAFLOW_XML, url=url)
        if url == "https://esploradati.istat.it/SDMXWS/rest/data/IT1,22_289,1.5/all":
            return http_ok(200, text=PREVIEW_JSON_WITH_VALUES, url=url)
        if url == "https://esploradati.istat.it/SDMXWS/rest/data/IT1,22_289,1.5/A.001001.JAN.9.TOTAL.99":
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
