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

PREVIEW_JSON = """
{
  "dataSets": [{"series": {}}],
  "structure": {
    "dimensions": {
      "series": [
        {"id": "FREQ"},
        {"id": "REF_AREA"},
        {"id": "DATA_TYPE"},
        {"id": "SEX"},
        {"id": "AGE"},
        {"id": "MARITAL_STATUS"}
      ]
    }
  }
}
"""


def test_sdmx_fetch_normalizes_csv(monkeypatch):
    calls = []

    def _fake_get(url, params=None, timeout=None, headers=None):
        calls.append((url, params, headers.get("Accept") if headers else None))
        if url.endswith("/dataflow/IT1/22_289"):
            return _FakeResponse(200, DATAFLOW_XML, url)
        if url.endswith("/data/22_289/all"):
            return _FakeResponse(200, PREVIEW_JSON, url)
        if url.endswith("/data/22_289/A.001001.JAN.9.TOTAL.99"):
            return _FakeResponse(200, DATA_JSON, url)
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr("toolkit.plugins.sdmx.requests.get", _fake_get)

    payload, origin = SdmxSource().fetch(
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
    assert origin.endswith("/data/22_289/A.001001.JAN.9.TOTAL.99")
    assert "FREQ,FREQ_label" in text
    assert "A,annual" in text
    assert "001001,Agliè" in text
    assert "2024,2024,2634" in text
    assert any(call[2] == "application/json" for call in calls)
    assert any(call[1] == {"firstNObservations": "0"} for call in calls)


def test_sdmx_fetch_blocks_version_mismatch(monkeypatch):
    def _fake_get(url, params=None, timeout=None, headers=None):
        if url.endswith("/dataflow/IT1/22_289"):
            return _FakeResponse(200, DATAFLOW_XML, url)
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr("toolkit.plugins.sdmx.requests.get", _fake_get)

    try:
        SdmxSource().fetch("IT1", "22_289", "1.0", {"FREQ": "A"})
    except DownloadError as exc:
        assert "current version is 1.5" in str(exc)
    else:
        raise AssertionError("Expected DownloadError")


def test_sdmx_fetch_rejects_unknown_filter_dimension(monkeypatch):
    def _fake_get(url, params=None, timeout=None, headers=None):
        if url.endswith("/dataflow/IT1/22_289"):
            return _FakeResponse(200, DATAFLOW_XML, url)
        if url.endswith("/data/22_289/all"):
            return _FakeResponse(200, PREVIEW_JSON, url)
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr("toolkit.plugins.sdmx.requests.get", _fake_get)

    try:
        SdmxSource().fetch("IT1", "22_289", "1.5", {"TIME_PERIOD": "2024"})
    except DownloadError as exc:
        assert "Unknown SDMX filter dimensions" in str(exc)
    else:
        raise AssertionError("Expected DownloadError")
