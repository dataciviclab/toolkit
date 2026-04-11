from __future__ import annotations

import requests

import pytest

from toolkit.core.exceptions import DownloadError
from toolkit.plugins.http_file import HttpFileSource


class _FakeResponse:
    def __init__(self, status_code: int = 200, content: bytes = b"ok") -> None:
        self.status_code = status_code
        self.content = content


def test_http_file_fetch_success(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_get(url: str, timeout: int, headers: dict[str, str]):
        calls.append({"url": url, "timeout": timeout, "headers": headers})
        return _FakeResponse(200, b"payload")

    monkeypatch.setattr("toolkit.plugins.http_file.requests.get", fake_get)

    source = HttpFileSource(timeout=15, retries=2, user_agent="ua-test")
    payload = source.fetch("https://example.test/data.csv")

    assert payload == b"payload"
    assert len(calls) == 1
    assert calls[0]["url"] == "https://example.test/data.csv"
    assert calls[0]["timeout"] == 15
    assert calls[0]["headers"] == {"User-Agent": "ua-test"}


def test_http_file_fetch_retries_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    state = {"n": 0}

    def fake_get(url: str, timeout: int, headers: dict[str, str]):
        state["n"] += 1
        if state["n"] < 3:
            raise requests.exceptions.Timeout("timeout")
        return _FakeResponse(200, b"ok-after-retry")

    monkeypatch.setattr("toolkit.plugins.http_file.requests.get", fake_get)

    source = HttpFileSource(retries=3)
    payload = source.fetch("https://example.test/retry")

    assert payload == b"ok-after-retry"
    assert state["n"] == 3


def test_http_file_fetch_raises_after_retries_exhausted(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_get(url: str, timeout: int, headers: dict[str, str]):
        raise requests.exceptions.ConnectionError("conn-down")

    monkeypatch.setattr("toolkit.plugins.http_file.requests.get", fake_get)

    source = HttpFileSource(retries=2)
    with pytest.raises(DownloadError, match="conn-down"):
        source.fetch("https://example.test/fail")


def test_http_file_fetch_http_status_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_get(url: str, timeout: int, headers: dict[str, str]):
        return _FakeResponse(503, b"service unavailable")

    monkeypatch.setattr("toolkit.plugins.http_file.requests.get", fake_get)

    source = HttpFileSource(retries=1)
    with pytest.raises(DownloadError, match="HTTP 503"):
        source.fetch("https://example.test/unavailable")
