"""Tests for HttpPostFileSource — adapter over lab_connectors.http.

Tests mock the public boundary (HttpClient.post / HttpResult), not
internal HTTP details. Retry and SSL fallback logic lives in
lab-connectors and is tested there.
"""

from __future__ import annotations

import pytest

from lab_connectors.http import HttpResult
from lab_connectors.testing import FakeHttpClient, fake_response

from toolkit.core.exceptions import DownloadError
from toolkit.plugins.http_post_file import HttpPostFileSource


@pytest.mark.contract
def test_fetch_success() -> None:
    """HttpResult ok → fetch returns bytes."""
    fake = FakeHttpClient()
    fake.responses["https://example.test/download"] = HttpResult(
        response=fake_response(200, text="payload"),
        err=None,
    )

    source = HttpPostFileSource(timeout=15, retries=2, user_agent="ua-test")
    source._client = fake

    payload = source.fetch("https://example.test/download", data={"key": "val"})

    assert payload == b"payload"
    assert len(fake.requests) == 1
    assert fake.requests[0][0] == "POST"
    assert fake.requests[0][1] == "https://example.test/download"
    assert fake.requests[0][2].get("data") == {"key": "val"}


@pytest.mark.contract
def test_fetch_without_data() -> None:
    """Fetch without data passes None."""
    fake = FakeHttpClient()
    fake.responses["https://example.test/download"] = HttpResult(
        response=fake_response(200, text="no-data"),
        err=None,
    )

    source = HttpPostFileSource()
    source._client = fake

    payload = source.fetch("https://example.test/download")

    assert payload == b"no-data"
    assert len(fake.requests) == 1
    assert fake.requests[0][2].get("data") is None


@pytest.mark.contract
def test_fetch_http_status_error() -> None:
    """Non-200 HTTP status → DownloadError with status code in message."""
    fake = FakeHttpClient()
    fake.responses["https://example.test/unavailable"] = HttpResult(
        response=fake_response(503),
        err=None,
    )

    source = HttpPostFileSource(retries=1)
    source._client = fake

    with pytest.raises(DownloadError, match="HTTP 503"):
        source.fetch("https://example.test/unavailable")


@pytest.mark.contract
def test_fetch_connection_error() -> None:
    """HttpResult with err → DownloadError."""
    fake = FakeHttpClient()
    fake.responses["https://example.test/fail"] = HttpResult(
        response=None,
        err=ConnectionError("connection refused"),
    )

    source = HttpPostFileSource(retries=2)
    source._client = fake

    with pytest.raises(DownloadError, match="connection refused"):
        source.fetch("https://example.test/fail")


@pytest.mark.contract
def test_fetch_passes_params() -> None:
    """HttpPostFileSource init params are forwarded to HttpClient constructor."""
    source = HttpPostFileSource(timeout=42, retries=5, user_agent="custom-agent/1.0")
    assert source._client.timeout == 42
    assert source._client.max_retries == 5
    assert source._client.user_agent == "custom-agent/1.0"

    source2 = HttpPostFileSource()
    assert source2._client.user_agent == "dataciviclab-toolkit/0.1"


@pytest.mark.policy
def test_ssl_fallback_semantics_preserved() -> None:
    """ssl_fallback_used=True in HttpResult is transparent to caller."""
    fake = FakeHttpClient()
    fake.responses["https://example.test/ssl-expired"] = HttpResult(
        response=fake_response(200, text="ssl-fallback-data"),
        err=None,
        ssl_fallback_used=True,
    )

    source = HttpPostFileSource(retries=1)
    source._client = fake

    payload = source.fetch("https://example.test/ssl-expired")
    assert payload == b"ssl-fallback-data"


@pytest.mark.policy
def test_ssl_fallback_failure_propagates() -> None:
    """HttpResult with ssl_fallback_used=False and err → DownloadError."""
    fake = FakeHttpClient()
    fake.responses["https://example.test/ssl-fail"] = HttpResult(
        response=None,
        err=ConnectionError("fallback failed"),
        ssl_fallback_used=False,
    )

    source = HttpPostFileSource(retries=1)
    source._client = fake

    with pytest.raises(DownloadError, match="fallback failed"):
        source.fetch("https://example.test/ssl-fail")


@pytest.mark.contract
class TestNonTruncableSampleBytes:
    """sample_bytes ignorato per formati binari non troncabili (POST)."""

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
    def test_sample_bytes_ignored_for_binary_post(self, url, should_ignore):
        """POST fetch: sample_bytes ignorato per estensioni non troncabili."""
        fake = FakeHttpClient()
        content = (b"a" * 10000 + b"\n").decode()
        fake.responses[url] = HttpResult(
            response=fake_response(200, text=content),
            err=None,
        )

        source = HttpPostFileSource(retries=1)
        source._client = fake

        payload = source.fetch(url, data={"key": "val"}, sample_bytes=5000)

        if should_ignore:
            assert len(payload) == 10001
            headers = fake.requests[0][2].get("headers", {}) or {}
            assert "Range" not in headers
        else:
            assert len(payload) <= 5000
