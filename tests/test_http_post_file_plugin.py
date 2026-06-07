"""Tests for HttpPostFileSource — adapter over lab_connectors.http.

Tests mock the public boundary (HttpClient.post / HttpResult), not
internal HTTP details. Retry and SSL fallback logic lives in
lab-connectors and is tested there.
"""

from __future__ import annotations

import pytest

from lab_connectors.http import HttpClient, HttpResult

from toolkit.core.exceptions import DownloadError
from toolkit.plugins.http_post_file import HttpPostFileSource


class _FakeResponse:
    """Minimal response stub duck-typing requests.Response properties."""

    def __init__(self, status_code: int = 200, content: bytes = b"ok") -> None:
        self.status_code = status_code
        self.content = content


@pytest.mark.contract
def test_fetch_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """HttpResult ok → fetch returns bytes."""
    results: list[tuple[str, dict | None, dict]] = []

    def fake_post(self, url: str, data: dict | None = None, **kwargs):
        results.append((url, data, kwargs))
        return HttpResult(response=_FakeResponse(200, b"payload"), err=None)

    monkeypatch.setattr(HttpClient, "post", fake_post)

    source = HttpPostFileSource(timeout=15, retries=2, user_agent="ua-test")
    payload = source.fetch("https://example.test/download", data={"key": "val"})

    assert payload == b"payload"
    assert len(results) == 1
    assert results[0][0] == "https://example.test/download"
    assert results[0][1] == {"key": "val"}


@pytest.mark.contract
def test_fetch_without_data(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fetch without data passes None."""
    results: list[tuple[str, dict | None, dict]] = []

    def fake_post(self, url: str, data: dict | None = None, **kwargs):
        results.append((url, data, kwargs))
        return HttpResult(response=_FakeResponse(200, b"no-data"), err=None)

    monkeypatch.setattr(HttpClient, "post", fake_post)

    source = HttpPostFileSource()
    payload = source.fetch("https://example.test/download")

    assert payload == b"no-data"
    assert len(results) == 1
    assert results[0][1] is None


@pytest.mark.contract
def test_fetch_http_status_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Non-200 HTTP status → DownloadError with status code in message."""

    def fake_post(self, url: str, data: dict | None = None, **kwargs):
        return HttpResult(response=_FakeResponse(503, b"service unavailable"), err=None)

    monkeypatch.setattr(HttpClient, "post", fake_post)

    source = HttpPostFileSource(retries=1)
    with pytest.raises(DownloadError, match="HTTP 503"):
        source.fetch("https://example.test/unavailable")


@pytest.mark.contract
def test_fetch_connection_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """HttpResult with err → DownloadError."""

    def fake_post(self, url: str, data: dict | None = None, **kwargs):
        return HttpResult(response=None, err=ConnectionError("connection refused"))

    monkeypatch.setattr(HttpClient, "post", fake_post)

    source = HttpPostFileSource(retries=2)
    with pytest.raises(DownloadError, match="connection refused"):
        source.fetch("https://example.test/fail")


@pytest.mark.contract
def test_fetch_passes_params() -> None:
    """HttpPostFileSource init params are forwarded to HttpClient constructor."""
    source = HttpPostFileSource(timeout=42, retries=5, user_agent="custom-agent/1.0")
    assert source._client.timeout == 42
    assert source._client.max_retries == 5
    assert source._client.user_agent == "custom-agent/1.0"

    # Default user_agent when not specified
    source2 = HttpPostFileSource()
    assert source2._client.user_agent == "dataciviclab-toolkit/0.1"


@pytest.mark.policy
def test_ssl_fallback_semantics_preserved(monkeypatch: pytest.MonkeyPatch) -> None:
    """ssl_fallback_used=True in HttpResult is transparent to caller."""

    def fake_post(self, url: str, data: dict | None = None, **kwargs):
        return HttpResult(
            response=_FakeResponse(200, b"ssl-fallback-data"),
            err=None,
            ssl_fallback_used=True,
        )

    monkeypatch.setattr(HttpClient, "post", fake_post)

    source = HttpPostFileSource(retries=1)
    payload = source.fetch("https://example.test/ssl-expired")

    assert payload == b"ssl-fallback-data"


@pytest.mark.policy
def test_ssl_fallback_failure_propagates(monkeypatch: pytest.MonkeyPatch) -> None:
    """HttpResult with ssl_fallback_used=False and err → DownloadError."""

    def fake_post(self, url: str, data: dict | None = None, **kwargs):
        return HttpResult(
            response=None,
            err=ConnectionError("fallback failed"),
            ssl_fallback_used=False,
        )

    monkeypatch.setattr(HttpClient, "post", fake_post)

    source = HttpPostFileSource(retries=1)
    with pytest.raises(DownloadError, match="fallback failed"):
        source.fetch("https://example.test/ssl-fail")


@pytest.mark.contract
class TestNonTruncableSampleBytes:
    """sample_bytes viene ignorato per formati binari non troncabili (POST).

    Regression: --smoke e --sample-bytes troncavano ZIP/parquet/xlsx
    producendo file corrotti in CI. Vedi toolkit#312.
    """

    @pytest.mark.contract
    @pytest.mark.parametrize(
        "url,should_ignore",
        [
            ("https://example.test/download.parquet", True),
            ("https://example.test/download.zip", True),
            ("https://example.test/download.xlsx", True),
            ("https://example.test/download.csv", False),
            ("https://example.test/download", False),
        ],
    )
    def test_sample_bytes_ignored_for_binary_post(
        self, url, should_ignore, monkeypatch: pytest.MonkeyPatch
    ):
        """POST fetch: sample_bytes ignorato per estensioni non troncabili."""
        results: list[tuple] = []

        def fake_post(self, url: str, data: dict | None = None, **kwargs):
            headers = kwargs.get("headers")
            has_range = headers is not None and "Range" in headers
            results.append((url, data, has_range))
            content = b"a" * 10000 + b"\n"
            return HttpResult(response=_FakeResponse(200, content), err=None)

        monkeypatch.setattr(HttpClient, "post", fake_post)
        source = HttpPostFileSource(retries=1)

        payload = source.fetch(url, data={"key": "val"}, sample_bytes=5000)

        if should_ignore:
            assert len(payload) == 10001  # full file
            assert not results[0][2]  # no Range header
        else:
            assert len(payload) <= 5000  # troncato
