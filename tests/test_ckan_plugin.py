from toolkit.core.exceptions import DownloadError
from toolkit.plugins.ckan import CkanSource


class _FakeResponse:
    def __init__(self, status_code: int, *, json_data=None, content: bytes = b"", url: str = "https://example.org"):
        self.status_code = status_code
        self._json_data = json_data
        self.content = content
        self.url = url

    def json(self):
        return self._json_data


def test_ckan_fetch_resource_show_forces_https(monkeypatch):
    calls = []

    def _fake_get(url, params=None, timeout=None, headers=None):
        calls.append((url, params))
        if "resource_show" in url:
            return _FakeResponse(
                200,
                json_data={
                    "success": True,
                    "result": {"url": "http://portal.example.org/export/data.csv"},
                },
                url=f"{url}?id=abc",
            )
        return _FakeResponse(
            200,
            content=b"a,b\n1,2\n",
            url="https://portal.example.org/export/data.csv",
        )

    monkeypatch.setattr("toolkit.plugins.ckan.requests.get", _fake_get)

    payload, origin = CkanSource().fetch("https://portal.example.org/api/3", resource_id="abc")

    assert payload == b"a,b\n1,2\n"
    assert origin == "https://portal.example.org/export/data.csv"
    assert calls[1][0] == "https://portal.example.org/export/data.csv"


def test_ckan_fetch_falls_back_to_package_show(monkeypatch):
    calls = []

    def _fake_get(url, params=None, timeout=None, headers=None):
        calls.append((url, params))
        if "resource_show" in url:
            return _FakeResponse(404, json_data={}, url=f"{url}?id=33344")
        if "package_show" in url:
            return _FakeResponse(
                200,
                json_data={
                    "success": True,
                    "result": {
                        "resources": [
                            {
                                "id": 33344,
                                "name": "csv dump",
                                "format": "CSV",
                                "url": "http://portal.example.org/api/3/datastore/dump/dataset.csv",
                            }
                        ]
                    },
                },
                url=f"{url}?id=dataset-id",
            )
        return _FakeResponse(
            200,
            content=b"a,b\n1,2\n",
            url="https://portal.example.org/api/3/datastore/dump/dataset.csv",
        )

    monkeypatch.setattr("toolkit.plugins.ckan.requests.get", _fake_get)

    payload, origin = CkanSource().fetch(
        "https://portal.example.org/api/3",
        resource_id="33344",
        dataset_id="dataset-id",
    )

    assert payload == b"a,b\n1,2\n"
    assert origin == "https://portal.example.org/api/3/datastore/dump/dataset.csv"
    assert any("package_show" in call[0] for call in calls)


def test_ckan_fetch_requires_identifier():
    try:
        CkanSource().fetch("https://portal.example.org/api/3")
    except DownloadError as exc:
        assert "resource_id or dataset_id" in str(exc)
    else:
        raise AssertionError("Expected DownloadError")


def test_ckan_fetch_rejects_package_fallback_when_resource_id_missing(monkeypatch):
    def _fake_get(url, params=None, timeout=None, headers=None):
        if "resource_show" in url:
            return _FakeResponse(404, json_data={}, url=f"{url}?id=99999")
        if "package_show" in url:
            return _FakeResponse(
                200,
                json_data={
                    "success": True,
                    "result": {
                        "resources": [
                            {
                                "id": 33344,
                                "name": "csv dump",
                                "format": "CSV",
                                "url": "http://portal.example.org/api/3/datastore/dump/dataset.csv",
                            }
                        ]
                    },
                },
                url=f"{url}?id=dataset-id",
            )
        raise AssertionError(f"Unexpected download request to {url}")

    monkeypatch.setattr("toolkit.plugins.ckan.requests.get", _fake_get)

    try:
        CkanSource().fetch(
            "https://portal.example.org/api/3",
            resource_id="99999",
            dataset_id="dataset-id",
        )
    except DownloadError as exc:
        assert "resource_id=99999" in str(exc)
    else:
        raise AssertionError("Expected DownloadError")


def test_ckan_download_bytes_retries_then_succeeds(monkeypatch):
    calls = {"n": 0}

    def _fake_get(url, params=None, timeout=None, headers=None):
        calls["n"] += 1
        if "resource_show" in url:
            return _FakeResponse(
                200,
                json_data={
                    "success": True,
                    "result": {"url": "https://portal.example.org/export/retry.csv"},
                },
                url=f"{url}?id=abc",
            )
        if calls["n"] < 3:
            raise RuntimeError("temporary network error")
        return _FakeResponse(200, content=b"ok-after-retry", url=url)

    monkeypatch.setattr("toolkit.plugins.ckan.requests.get", _fake_get)

    payload, origin = CkanSource(retries=3).fetch(
        "https://portal.example.org/api/3", resource_id="abc"
    )

    assert payload == b"ok-after-retry"
    assert origin == "https://portal.example.org/export/retry.csv"
    assert calls["n"] == 3


def test_ckan_download_bytes_raises_on_http_error(monkeypatch):
    def _fake_get(url, params=None, timeout=None, headers=None):
        if "resource_show" in url:
            return _FakeResponse(
                200,
                json_data={
                    "success": True,
                    "result": {"url": "https://portal.example.org/export/unavailable.csv"},
                },
                url=f"{url}?id=abc",
            )
        return _FakeResponse(503, content=b"", url=url)

    monkeypatch.setattr("toolkit.plugins.ckan.requests.get", _fake_get)

    try:
        CkanSource(retries=1).fetch("https://portal.example.org/api/3", resource_id="abc")
    except DownloadError as exc:
        assert "HTTP 503" in str(exc)
    else:
        raise AssertionError("Expected DownloadError")
