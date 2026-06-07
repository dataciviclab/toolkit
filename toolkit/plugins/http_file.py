from __future__ import annotations

import logging
import os
import subprocess
from urllib.parse import urlparse, urlunparse

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
                    "SSL fallback failed for %s — riprovo con curl via proxy",
                    url,
                )
                return _fetch_via_curl(url, proxy, self.timeout, self.user_agent, sample_bytes)

        err = result.err
        raise DownloadError(str(err) if err else f"Failed to fetch {url}")


def _sanitize_proxy_url(proxy: str) -> str:
    """Rimuove credenziali da URL proxy per log sicuri."""
    parsed = urlparse(proxy)
    if parsed.password:
        return urlunparse(
            parsed._replace(
                netloc=f"{parsed.username}:***@{parsed.hostname}"
                f"{':' + str(parsed.port) if parsed.port else ''}"
            )
        )
    if parsed.username:
        return urlunparse(
            parsed._replace(
                netloc=f"{parsed.username}@{parsed.hostname}"
                f"{':' + str(parsed.port) if parsed.port else ''}"
            )
        )
    return proxy


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
    Equivalente semantico di requests: segue redirect, controlla HTTP 200/206.
    """
    # Usa --write-out per catturare lo HTTP status code (ultima riga stdout)
    cmd = [
        "curl",
        "-sS",  # silent, show errors
        "-L",  # follow redirects
        "--fail-with-body",  # exit non-zero su HTTP 400+, body preservato
        "--max-time",
        str(timeout),
        "-x",
        proxy,
        "-A",
        user_agent,
        "-w",
        "\n%{http_code}",
    ]
    if sample_bytes is not None:
        cmd += ["-r", f"0-{sample_bytes - 1}"]
    cmd += [url]

    try:
        r = subprocess.run(cmd, capture_output=True, timeout=timeout + 5)
    except subprocess.TimeoutExpired:
        raise DownloadError(f"curl timeout after {timeout}s for {url}")

    # Estrai HTTP status code dall'ultima riga di stdout
    stdout = r.stdout
    status_code = _parse_curl_status(stdout, r.stderr)
    body = _strip_curl_status(stdout, status_code)

    # --fail-with-body + exit 0 garantisce 2xx, ma verifichiamo 200/206
    if status_code not in (200, 206):
        stderr = r.stderr.decode("utf-8", errors="replace").strip()
        raise DownloadError(f"curl HTTP {status_code} per {url}: {stderr or 'unexpected status'}")

    content = body
    if sample_bytes is not None:
        content = truncate_at_line(content, sample_bytes)
    logger.info("curl fallback OK — %d bytes da %s", len(content), url)
    return content


def _parse_curl_status(stdout: bytes, stderr: bytes) -> int:
    """Estrae HTTP status dall'ultima riga di stdout di curl."""
    lines = stdout.split(b"\n")
    for candidate in reversed(lines):
        candidate = candidate.strip()
        if candidate.isdigit():
            return int(candidate)
    # Se non trovato, curl non ha prodotto output (startup error)
    err_text = stderr.decode("utf-8", errors="replace").strip()
    raise DownloadError(f"curl: no HTTP status in output: {err_text or 'empty response'}")


def _strip_curl_status(stdout: bytes, status_code: int) -> bytes:
    """Rimuove la riga di status_code aggiunta da -w."""
    suffix = f"\n{status_code}".encode()
    if stdout.endswith(suffix):
        return stdout[: -len(suffix)]
    return stdout
