"""Tests for HttpFileSource — adapter over lab_connectors.http.

Tests mock the public boundary (HttpClient.get / HttpResult), not
internal HTTP details. Retry and SSL fallback logic lives in
lab-connectors and is tested there.
"""
from __future__ import annotations

import pytest

from lab_connectors.http import HttpClient, HttpResult

from toolkit.core.exceptions import DownloadError
from toolkit.plugins.http_file import HttpFileSource


class _FakeResponse:
    """Minimal response stub duck-typing requests.Response properties."""
    def __init__(self, status_code: int = 200, content: bytes = b"ok") -> None:
        self.status_code = status_code
        self.content = content


def test_fetch_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """HttpResult ok → fetch returns bytes."""
    results: list[tuple[str, dict]] = []

    def fake_get(self, url: str, **kwargs):
        results.append((url, kwargs))
        return HttpResult(response=_FakeResponse(200, b"payload"), err=None)

    monkeypatch.setattr(HttpClient, "get", fake_get)

    source = HttpFileSource(timeout=15, retries=2, user_agent="ua-test")
    payload = source.fetch("https://example.test/data.csv")

    assert payload == b"payload"
    assert len(results) == 1
    assert results[0][0] == "https://example.test/data.csv"


def test_fetch_http_status_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Non-200 HTTP status → DownloadError with status code in message."""
    def fake_get(self, url: str, **kwargs):
        return HttpResult(response=_FakeResponse(503, b"service unavailable"), err=None)

    monkeypatch.setattr(HttpClient, "get", fake_get)

    source = HttpFileSource(retries=1)
    with pytest.raises(DownloadError, match="HTTP 503"):
        source.fetch("https://example.test/unavailable")


def test_fetch_connection_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """HttpResult with err → DownloadError."""
    def fake_get(self, url: str, **kwargs):
        return HttpResult(response=None, err=ConnectionError("connection refused"))

    monkeypatch.setattr(HttpClient, "get", fake_get)

    source = HttpFileSource(retries=2)
    with pytest.raises(DownloadError, match="connection refused"):
        source.fetch("https://example.test/fail")


def test_fetch_passes_params() -> None:
    """HttpFileSource init params are forwarded to HttpClient constructor."""
    source = HttpFileSource(timeout=42, retries=5, user_agent="custom-agent/1.0")
    assert source._client.timeout == 42
    assert source._client.max_retries == 5
    assert source._client.user_agent == "custom-agent/1.0"

    # Default user_agent when not specified
    source2 = HttpFileSource()
    assert source2._client.user_agent == "dataciviclab-toolkit/0.1"


@pytest.mark.adapter
def test_integration_fetch_real_url() -> None:
    """Real HTTPS fetch against a stable public URL (no mocks)."""
    source = HttpFileSource(timeout=30, retries=1)
    data = source.fetch("https://raw.githubusercontent.com/dataciviclab/toolkit/main/README.md")
    assert len(data) > 100
    assert b"DataCivicLab Toolkit" in data


@pytest.mark.policy
def test_ssl_fallback_semantics_preserved(monkeypatch: pytest.MonkeyPatch) -> None:
    """ssl_fallback_used=True in HttpResult is transparent to caller."""
    def fake_get(self, url: str, **kwargs):
        return HttpResult(
            response=_FakeResponse(200, b"ssl-fallback-data"),
            err=None,
            ssl_fallback_used=True,
        )

    monkeypatch.setattr(HttpClient, "get", fake_get)

    source = HttpFileSource(retries=1)
    payload = source.fetch("https://example.test/ssl-expired")

    assert payload == b"ssl-fallback-data"


@pytest.mark.policy
def test_ssl_fallback_failure_propagates(monkeypatch: pytest.MonkeyPatch) -> None:
    """HttpResult with ssl_fallback_used=False and err → DownloadError."""
    def fake_get(self, url: str, **kwargs):
        return HttpResult(
            response=None,
            err=ConnectionError("fallback failed"),
            ssl_fallback_used=False,
        )

    monkeypatch.setattr(HttpClient, "get", fake_get)

    source = HttpFileSource(retries=1)
    with pytest.raises(DownloadError, match="fallback failed"):
        source.fetch("https://example.test/ssl-fail")
