"""Tests for HttpFileSource — adapter over lab_connectors.http.

Tests use ``FakeHttpClient`` from ``lab_connectors.testing`` instead of
monkeypatching ``HttpClient.get``. Retry, backoff, and SSL fallback
logic lives in lab-connectors and is tested there.
"""

from __future__ import annotations

import os
from unittest import mock

import pytest

from lab_connectors.http import HttpResult, HttpFallbackError
from lab_connectors.testing import FakeHttpClient, fake_response

from toolkit.core.exceptions import DownloadError
from toolkit.plugins.http_file import (
    HttpFileSource,
    _get_proxy_from_env,
    _parse_curl_status,
    _strip_curl_status,
    _try_wget,
)


@pytest.mark.contract
def test_fetch_success() -> None:
    """HttpResult ok → fetch returns bytes."""
    fake = FakeHttpClient()
    fake.responses["https://example.test/data.csv"] = HttpResult(
        response=fake_response(200, "payload"),
        err=None,
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
        response=fake_response(503, "service unavailable"),
        err=None,
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
        response=None,
        err=ConnectionError("connection refused"),
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
    @pytest.mark.parametrize(
        "url,should_ignore",
        [
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
        ],
    )
    def test_sample_bytes_respected_for_truncable_only(self, url, should_ignore):
        data = "a" * 10000 + "\n"
        fake = FakeHttpClient()
        fake.responses[url] = HttpResult(
            response=fake_response(200, data),
            err=None,
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


# --- curl fallback helpers ---


class TestParseCurlStatus:
    @pytest.mark.pure_unit
    def test_status_at_end(self):
        assert _parse_curl_status(b"body\n200", b"") == 200

    @pytest.mark.pure_unit
    def test_status_with_trailing_newline(self):
        assert _parse_curl_status(b"body\n200\n", b"") == 200

    @pytest.mark.pure_unit
    def test_no_status_raises(self):
        with pytest.raises(DownloadError, match="no HTTP status"):
            _parse_curl_status(b"body\n", b"curl: (7) Failed to connect")


@pytest.mark.pure_unit
class TestStripCurlStatus:
    @pytest.mark.pure_unit
    def test_strips_trailing_status(self):
        assert _strip_curl_status(b"body\n200", 200) == b"body"

    @pytest.mark.pure_unit
    def test_noop_when_not_suffixed(self):
        assert _strip_curl_status(b"body", 200) == b"body"


class TestCurlFallbackIntegration:
    """Quando is_ssl_fallback_failed=True e HTTPS_PROXY è impostato,
    fetch deve chiamare _fetch_via_curl invece di lanciare DownloadError."""

    @pytest.mark.contract
    def test_fallback_triggered_with_proxy(self):
        """ssl_fallback_failed + proxy → curl fallback chiamato."""
        err = HttpFallbackError(
            primary_error=ConnectionError("SSL err"), fallback_error=ConnectionError("fallback err")
        )
        fake = FakeHttpClient()
        fake.responses["https://example.test/data.csv"] = HttpResult(
            response=None,
            err=err,
            ssl_fallback_used=False,
        )

        source = HttpFileSource(retries=1)
        source._client = fake

        with mock.patch.dict(os.environ, {"HTTPS_PROXY": "http://proxy:8888"}):
            with mock.patch(
                "toolkit.plugins.http_file._fetch_via_curl",
                return_value=b"curl-data",
            ) as mock_curl:
                payload = source.fetch("https://example.test/data.csv")

        assert payload == b"curl-data"
        mock_curl.assert_called_once()

    @pytest.mark.contract
    def test_no_fallback_without_proxy(self):
        """ssl_fallback_failed ma senza proxy → DownloadError (no curl)."""
        err = HttpFallbackError(
            primary_error=ConnectionError("SSL err"), fallback_error=ConnectionError("fallback err")
        )
        fake = FakeHttpClient()
        fake.responses["https://example.test/data.csv"] = HttpResult(
            response=None,
            err=err,
            ssl_fallback_used=False,
        )

        source = HttpFileSource(retries=1)
        source._client = fake

        with mock.patch.dict(os.environ, {}, clear=True):
            with pytest.raises(DownloadError, match="fallback failed"):
                source.fetch("https://example.test/data.csv")

    @pytest.mark.contract
    @pytest.mark.regression  # toolkit#413
    def test_fallback_on_timeout_with_proxy(self):
        """Timeout/ConnError + proxy → curl fallback chiamato (non solo SSL)."""
        fake = FakeHttpClient()
        fake.responses["https://example.test/data.csv"] = HttpResult(
            response=None,
            err=TimeoutError("connect timed out"),
            ssl_fallback_used=False,
        )
        source = HttpFileSource(retries=1)
        source._client = fake
        with mock.patch.dict(os.environ, {"HTTPS_PROXY": "http://proxy:8888"}):
            with mock.patch(
                "toolkit.plugins.http_file._fetch_via_curl",
                return_value=b"curl-data",
            ) as mock_curl:
                payload = source.fetch("https://example.test/data.csv")
        assert payload == b"curl-data"
        mock_curl.assert_called_once()

    @pytest.mark.contract
    @pytest.mark.regression  # toolkit#414
    def test_get_proxy_env_blocked_source(self):
        """_get_proxy_from_env legge BLOCKED_SOURCE_PROXY."""
        with mock.patch.dict(os.environ, {"BLOCKED_SOURCE_PROXY": "http://proxy:8888"}, clear=True):
            assert _get_proxy_from_env() == "http://proxy:8888"


class TestWgetFallback:
    """Fallback wget per fonti che bloccano requests anche via proxy."""

    @pytest.mark.contract
    def test_http_403_uses_wget(self):
        """HTTP 403 → wget diretto, senza passare dal proxy."""
        fake = FakeHttpClient()
        fake.responses["https://example.test/data.csv"] = HttpResult(
            response=fake_response(403, "forbidden"),
            err=None,
        )
        source = HttpFileSource(retries=1)
        source._client = fake

        with mock.patch(
            "toolkit.plugins.http_file._try_wget",
            return_value=b"wget-data",
        ) as mock_wget:
            payload = source.fetch("https://example.test/data.csv")

        assert payload == b"wget-data"
        mock_wget.assert_called_once()

    @pytest.mark.contract
    def test_http_403_wget_failure_falls_back_to_curl_proxy(self):
        """wget fallisce + proxy → curl via proxy."""
        fake = FakeHttpClient()
        fake.responses["https://example.test/data.csv"] = HttpResult(
            response=fake_response(403, "forbidden"),
            err=None,
        )
        source = HttpFileSource(retries=1)
        source._client = fake

        with (
            mock.patch("toolkit.plugins.http_file._try_wget", return_value=None),
            mock.patch(
                "toolkit.plugins.http_file._get_proxy_from_env",
                return_value="http://proxy:8888",
            ),
            mock.patch(
                "toolkit.plugins.http_file._fetch_via_curl",
                return_value=b"curl-data",
            ) as mock_curl,
        ):
            payload = source.fetch("https://example.test/data.csv")

        assert payload == b"curl-data"
        mock_curl.assert_called_once()

    @pytest.mark.contract
    def test_http_403_without_fallback_raises(self):
        """wget assente e nessun proxy → DownloadError HTTP 403."""
        fake = FakeHttpClient()
        fake.responses["https://example.test/data.csv"] = HttpResult(
            response=fake_response(403, "forbidden"),
            err=None,
        )
        source = HttpFileSource(retries=1)
        source._client = fake

        with (
            mock.patch("toolkit.plugins.http_file._try_wget", return_value=None),
            mock.patch("toolkit.plugins.http_file._get_proxy_from_env", return_value=None),
            pytest.raises(DownloadError, match="HTTP 403"),
        ):
            source.fetch("https://example.test/data.csv")


class TestTryWget:
    @pytest.mark.pure_unit
    def test_skips_sample_bytes(self):
        """sample_bytes non è supportato da wget: nessun download."""
        assert _try_wget("https://example.test/data.csv", 10, "ua", sample_bytes=100) is None

    @pytest.mark.pure_unit
    def test_missing_wget_returns_none(self):
        with mock.patch("toolkit.plugins.http_file.shutil.which", return_value=None):
            assert _try_wget("https://example.test/data.csv", 10, "ua") is None

    @pytest.mark.pure_unit
    def test_success_returns_stdout(self):
        completed = mock.Mock(returncode=0, stdout=b"payload", stderr=b"")
        with (
            mock.patch("toolkit.plugins.http_file.shutil.which", return_value="/usr/bin/wget"),
            mock.patch("toolkit.plugins.http_file.subprocess.run", return_value=completed),
        ):
            assert _try_wget("https://example.test/data.csv", 10, "ua") == b"payload"

    @pytest.mark.pure_unit
    def test_failure_returns_none(self):
        completed = mock.Mock(returncode=8, stdout=b"", stderr=b"403 Forbidden")
        with (
            mock.patch("toolkit.plugins.http_file.shutil.which", return_value="/usr/bin/wget"),
            mock.patch("toolkit.plugins.http_file.subprocess.run", return_value=completed),
        ):
            assert _try_wget("https://example.test/data.csv", 10, "ua") is None
