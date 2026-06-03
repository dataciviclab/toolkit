"""Tests for HttpFileSource — adapter over lab_connectors.http.

Tests use ``FakeHttpClient`` from ``lab_connectors.testing`` instead of
monkeypatching ``HttpClient.get``. Retry, backoff, and SSL fallback
logic lives in lab-connectors and is tested there.
"""
from __future__ import annotations

import pytest

from lab_connectors.http import HttpResult
from lab_connectors.testing import FakeHttpClient, fake_response

from toolkit.core.exceptions import DownloadError
from toolkit.plugins.http_file import HttpFileSource


@pytest.mark.contract
def test_fetch_success() -> None:
    """HttpResult ok → fetch returns bytes."""
    fake = FakeHttpClient()
    fake.responses["https://example.test/data.csv"] = HttpResult(
        response=fake_response(200, "payload"), err=None,
    )

    source = HttpFileSource(timeout=15, retries=2, user_agent="ua-test")
    source._client = fake  # inject fake

    payload = source.fetch("https://example.test/data.csv")

    assert payload == b"payload"
    assert len(fake.requests) == 1
    assert fake.requests[0][0] == "GET"
    assert fake.requests[0][1] == "https://example.test/data.csv"


@pytest.mark.contract
def test_fetch_http_status_error() -> None:
    """Non-200 HTTP status → DownloadError with status code in message."""
    fake = FakeHttpClient()
    fake.responses["https://example.test/unavailable"] = HttpResult(
        response=fake_response(503, "service unavailable"), err=None,
    )

    source = HttpFileSource(retries=1)
    source._client = fake

    with pytest.raises(DownloadError, match="HTTP 503"):
        source.fetch("https://example.test/unavailable")


@pytest.mark.contract
def test_fetch_connection_error() -> None:
    """HttpResult with err → DownloadError."""
    fake = FakeHttpClient()
    fake.responses["https://example.test/fail"] = HttpResult(
        response=None, err=ConnectionError("connection refused"),
    )

    source = HttpFileSource(retries=2)
    source._client = fake

    with pytest.raises(DownloadError, match="connection refused"):
        source.fetch("https://example.test/fail")


@pytest.mark.contract
def test_fetch_passes_params() -> None:
    """HttpFileSource init params are forwarded to HttpClient constructor."""
    source = HttpFileSource(timeout=42, retries=5, user_agent="custom-agent/1.0")
    assert source._client.timeout == 42
    assert source._client.max_retries == 5
    assert source._client.user_agent == "custom-agent/1.0"

    # Default user_agent when not specified
    source2 = HttpFileSource()
    assert source2._client.user_agent == "dataciviclab-toolkit/0.1"


@pytest.mark.policy
def test_ssl_fallback_semantics_preserved() -> None:
    """ssl_fallback_used=True in HttpResult is transparent to caller."""
    fake = FakeHttpClient()
    fake.responses["https://example.test/ssl-expired"] = HttpResult(
        response=fake_response(200, "ssl-fallback-data"),
        err=None,
        ssl_fallback_used=True,
    )

    source = HttpFileSource(retries=1)
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

    source = HttpFileSource(retries=1)
    source._client = fake

    with pytest.raises(DownloadError, match="fallback failed"):
        source.fetch("https://example.test/ssl-fail")


@pytest.mark.contract
class TestNonTruncableSampleBytes:
    """sample_bytes viene ignorato per formati binari non troncabili.

    Regression: --smoke e --sample-bytes troncavano ZIP/parquet/xlsx
    producendo file corrotti in CI. Vedi toolkit#312.
    """

    @pytest.mark.contract
    @pytest.mark.parametrize("url,should_ignore", [
        ("https://example.test/data.parquet", True),
        ("https://example.test/data.zip", True),
        ("https://example.test/data.xlsx", True),
        ("https://example.test/data.xls", True),
        ("https://example.test/data.gz", True),
        ("https://example.test/data.csv", False),
        ("https://example.test/data.tsv", False),
        ("https://example.test/data.txt", False),
        ("https://example.test/data.json", False),
        ("https://example.test/data", False),  # no extension
        ("https://example.test/api/v1/download?format=csv", False),  # query params
    ])
    def test_sample_bytes_respected_for_truncable_only(self, url, should_ignore):
        data = "a" * 10000 + "\n"
        fake = FakeHttpClient()
        fake.responses[url] = HttpResult(
            response=fake_response(200, data), err=None,
        )

        source = HttpFileSource(retries=1)
        source._client = fake

        payload = source.fetch(url, sample_bytes=5000)

        if should_ignore:
            # Formato non troncabile: deve scaricare tutto il file
            assert len(payload) == 10001
            assert "Range" not in str(fake.requests[0][2].get("headers", {}))
        else:
            # CSV/JSON: sample_bytes rispettato con troncamento locale
            assert len(payload) <= 5000
