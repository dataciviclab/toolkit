from __future__ import annotations

import logging
import os
import subprocess

from lab_connectors.http import HttpClient

from toolkit.core.exceptions import DownloadError
from toolkit.plugins._http_utils import is_non_truncable_url, truncate_at_line

logger = logging.getLogger("toolkit.plugins.http_file")


class HttpFileSource:
    """Download a file via HTTP(S) with SSL fallback for expired/invalid certs.

    Adapter over lab_connectors.http.HttpClient that translates
    HttpResult into toolkit's DownloadError contract.
    """

    def __init__(self, timeout: int = 60, retries: int = 2, user_agent: str | None = None):
        self.timeout = timeout
        self.retries = retries
        self.user_agent = user_agent or "dataciviclab-toolkit/0.1"
        self._client = HttpClient(
            timeout=timeout,
            max_retries=retries,
            user_agent=self.user_agent,
        )

    def fetch(self, url: str, sample_bytes: int | None = None) -> bytes:
        if sample_bytes is not None and is_non_truncable_url(url):
            logger.info(
                "sample_bytes=%s ignorato per formato non troncabile: %s",
                sample_bytes,
                url,
            )
            sample_bytes = None

        headers = None
        if sample_bytes is not None:
            headers = {"Range": f"bytes=0-{sample_bytes - 1}"}
        result = self._client.get(url, headers=headers)
        if result.is_ok and result.response is not None:
            if result.response.status_code not in (200, 206):
                raise DownloadError(f"HTTP {result.response.status_code} for {url}")
            content = result.response.content
            if sample_bytes is not None:
                content = truncate_at_line(content, sample_bytes)
            return content

        # Fallback a curl via proxy quando Python requests fallisce con SSL
        # (noto: GitHub Actions + tinyproxy da SSLV3_ALERT_HANDSHAKE_FAILURE
        #  con dati.salute.gov.it — curl invece funziona)
        if result.is_ssl_fallback_failed:
            proxy = _get_proxy_from_env()
            if proxy:
                logger.warning(
                    "SSL fallback failed for %s — riprovo con curl via proxy %s",
                    url,
                    proxy,
                )
                return _fetch_via_curl(url, proxy, self.timeout, self.user_agent, sample_bytes)

        err = result.err
        raise DownloadError(str(err) if err else f"Failed to fetch {url}")


def _get_proxy_from_env() -> str | None:
    """Legge HTTPS_PROXY dall'ambiente (lowercase/uppercase)."""
    return os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")


def _fetch_via_curl(
    url: str,
    proxy: str,
    timeout: int,
    user_agent: str,
    sample_bytes: int | None = None,
) -> bytes:
    """Scarica URL tramite curl con proxy.

    Fallback usato quando Python requests fallisce con SSLError attraverso
    un proxy HTTP (es. GitHub Actions + tinyproxy vs dati.salute.gov.it).
    """
    cmd = [
        "curl",
        "-s",  # silent
        "-S",  # show errors
        "--max-time",
        str(timeout),
        "-x",
        proxy,
        "-A",
        user_agent,
    ]
    if sample_bytes is not None:
        cmd += ["-r", f"0-{sample_bytes - 1}"]
    cmd += [url]

    try:
        r = subprocess.run(cmd, capture_output=True, timeout=timeout + 5)
    except subprocess.TimeoutExpired:
        raise DownloadError(f"curl timeout after {timeout}s for {url} (proxy: {proxy})")

    if r.returncode != 0:
        stderr = r.stderr.decode("utf-8", errors="replace").strip()
        raise DownloadError(
            f"curl exit={r.returncode} per {url} (proxy: {proxy}): {stderr or 'unknown error'}"
        )

    content = r.stdout
    if sample_bytes is not None:
        content = truncate_at_line(content, sample_bytes)
    logger.info("curl fallback OK — %d bytes da %s (via %s)", len(content), url, proxy)
    return content
