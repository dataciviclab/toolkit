from __future__ import annotations

import json

import pytest

from lab_connectors.http import HttpResult
from toolkit.core.exceptions import DownloadError
from toolkit.plugins.dbnomics import DbnomicsSource

pytestmark = pytest.mark.adapter


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict, url: str = ""):
        self.status_code = status_code
        self._payload = payload
        self.text = json.dumps(payload)
        self.url = url
        self.headers = {}

    def json(self):
        return self._payload


def _ok(payload: dict) -> HttpResult:
    return HttpResult(response=_FakeResponse(200, payload), err=None, ssl_fallback_used=None)


class TestDbnomicsSource:
    def test_fetch_single_page_to_csv(self, monkeypatch):
        src = DbnomicsSource(page_size=10)
        payload = {
            "series": {
                "num_found": 2,
                "docs": [
                    {
                        "series_code": "A.IT.B1GQ_B_W2_S1.V.N.2025M1",
                        "dimensions": {
                            "REF_AREA": "IT",
                            "DATA_TYPE_AGGR": "B1GQ_B_W2_S1",
                            "FREQ": "A",
                        },
                        "period": ["2022", "2023"],
                        "value": [2100000.0, 2200000.0],
                    },
                    {
                        "series_code": "A.ITC1.B1GQ_B_W2_S1.V.N.2025M1",
                        "dimensions": {
                            "REF_AREA": "ITC1",
                            "DATA_TYPE_AGGR": "B1GQ_B_W2_S1",
                            "FREQ": "A",
                        },
                        "period": ["2022"],
                        "value": [140000.0],
                    },
                ],
            }
        }
        monkeypatch.setattr(src._client, "get", lambda *a, **k: _ok(payload))

        content = src.fetch(
            "93_498",
            provider="IT1".replace("IT1", "ISTAT"),
            dimensions={"REF_AREA": ["IT", "ITC1"]},
        )
        text = content.decode()
        lines = text.strip().splitlines()
        assert lines[0].startswith(
            "series_code,provider,dataset,REF_AREA,DATA_TYPE_AGGR,FREQ,TIME_PERIOD,OBS_VALUE"
        )
        assert len(lines) == 4  # header + 3 rows
        assert "2200000.0" in text
        assert "ISTAT" in text

    def test_fetch_pagination(self, monkeypatch):
        src = DbnomicsSource(page_size=1)
        page0 = {
            "series": {
                "num_found": 2,
                "docs": [
                    {
                        "series_code": "S1",
                        "dimensions": {"REF_AREA": "IT"},
                        "period": ["2020"],
                        "value": [1.0],
                    }
                ],
            }
        }
        page1 = {
            "series": {
                "num_found": 2,
                "docs": [
                    {
                        "series_code": "S2",
                        "dimensions": {"REF_AREA": "IT"},
                        "period": ["2021"],
                        "value": [2.0],
                    }
                ],
            }
        }
        calls = {"n": 0}

        def fake_get(url, params=None, **kwargs):
            assert params is not None
            off = int(params.get("offset", 0))
            calls["n"] += 1
            return _ok(page0 if off == 0 else page1)

        monkeypatch.setattr(src._client, "get", fake_get)
        content = src.fetch("123_456", dimensions={"REF_AREA": ["IT"]})
        text = content.decode()
        assert calls["n"] == 2
        assert "S1" in text and "S2" in text
        assert "2020" in text and "2021" in text

    def test_provider_dataset_slash(self, monkeypatch):
        src = DbnomicsSource()
        payload = {
            "series": {
                "num_found": 1,
                "docs": [
                    {
                        "series_code": "X",
                        "dimensions": {},
                        "period": ["2020"],
                        "value": [1.0],
                    }
                ],
            }
        }
        captured = {}

        def fake_get(url, params=None, **kwargs):
            captured["url"] = url
            return _ok(payload)

        monkeypatch.setattr(src._client, "get", fake_get)
        src.fetch("ISTAT/93_498")
        assert "/series/ISTAT/93_498" in captured["url"]
        assert captured["url"].count("/series/") == 1

    def test_missing_dataset_raises(self):
        src = DbnomicsSource()
        with pytest.raises(DownloadError):
            src.fetch("")

    def test_empty_results_raise(self, monkeypatch):
        src = DbnomicsSource()
        monkeypatch.setattr(
            src._client,
            "get",
            lambda *a, **k: _ok({"series": {"num_found": 0, "docs": []}}),
        )
        with pytest.raises(DownloadError):
            src.fetch("93_498", dimensions={"REF_AREA": ["ZZ"]})

    def test_na_values_skipped(self, monkeypatch):
        src = DbnomicsSource()
        payload = {
            "series": {
                "num_found": 1,
                "docs": [
                    {
                        "series_code": "S",
                        "dimensions": {"REF_AREA": "IT"},
                        "period": ["2020", "2021"],
                        "value": ["NA", 5.0],
                    }
                ],
            }
        }
        monkeypatch.setattr(src._client, "get", lambda *a, **k: _ok(payload))
        text = src.fetch("93_498").decode()
        assert "2020" not in text.split("\n", 1)[-1]  # only value row 2021
        assert "5.0" in text
