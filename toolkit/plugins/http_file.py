from __future__ import annotations

import logging
import os
import shutil
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
            status = result.response.status_code
            if status not in (200, 206):
                # AEM/Akamai e simili bloccano requests (anche via proxy) ma
                # accettano wget: unico fallback verificato su open-pnrr.
                if status in (403, 407):
                    content = _try_wget(url, self.timeout, self.user_agent, sample_bytes)
                    if content is not None:
                        return content
                    proxy = _get_proxy_from_env()
                    if proxy:
                        logger.warning(
                            "HTTP %s for %s — wget fallito, riprovo con curl via proxy",
                            status,
                            url,
                        )
                        return _fetch_via_curl(
                            url, proxy, self.timeout, self.user_agent, sample_bytes
                        )
                raise DownloadError(f"HTTP {status} for {url}")
            content = result.response.content
            if sample_bytes is not None:
                content = truncate_at_line(content, sample_bytes)
            return content

        # Fallback a curl via proxy quando requests fallisce (SSL o timeout)
        # (noto: GitHub Actions + proxy HTTP → requests non invia CONNECT,
        #  curl -x invece funziona. Squid/tinyproxy: stesso comportamento.)
        if not result.is_ok:
            proxy = _get_proxy_from_env()
            if proxy:
                logger.warning(
                    "Requests failed for %s (%s) — riprovo con curl via proxy",
                    url,
                    result.err
                    or f"HTTP {result.response.status_code if result.response else 'no response'}",
                )
                return _fetch_via_curl(url, proxy, self.timeout, self.user_agent, sample_bytes)

        err = result.err
        raise DownloadError(str(err) if err else f"Failed to fetch {url}")


def _get_proxy_from_env() -> str | None:
    """Legge HTTPS_PROXY o BLOCKED_SOURCE_PROXY dall'ambiente."""
    return (
        os.environ.get("HTTPS_PROXY")
        or os.environ.get("https_proxy")
        or os.environ.get("BLOCKED_SOURCE_PROXY")
    )


def _try_wget(
    url: str,
    timeout: int,
    user_agent: str,
    sample_bytes: int | None = None,
) -> bytes | None:
    """Scarica l'URL con wget diretto; ``None`` se non disponibile o fallisce.

    Fallback per fonti che bloccano requests (anche via proxy) ma accettano
    wget, es. AEM/Akamai su open-pnrr. Non supporta Range: con ``sample_bytes``
    viene saltato per non scaricare il file intero.
    """
    if sample_bytes is not None:
        return None
    if shutil.which("wget") is None:
        return None

    cmd = [
        "wget",
        "-nv",
        f"--user-agent={user_agent}",
        f"--timeout={timeout}",
        "--tries=1",
        "-O",
        "-",
        "--",
        url,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=timeout + 5)
    except (OSError, subprocess.TimeoutExpired) as exc:
        logger.warning("wget fallback fallito per %s: %s", url, exc)
        return None

    if result.returncode != 0 or not result.stdout:
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        logger.warning(
            "wget fallback fallito per %s: exit=%s %s",
            url,
            result.returncode,
            stderr or "empty output",
        )
        return None

    logger.info("wget fallback OK — %d bytes da %s", len(result.stdout), url)
    return result.stdout


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
    cmd += ["--", url]

    try:
        r = subprocess.run(cmd, capture_output=True, timeout=timeout + 5)
    except subprocess.TimeoutExpired:
        raise DownloadError(f"curl timeout after {timeout}s for {url}")

    # Curl exit status: errori di rete, I/O, write failure, trasferimento
    # parziale ecc. --fail-with-body copre solo HTTP 4xx/5xx, non tutto.
    if r.returncode != 0:
        stderr = r.stderr.decode("utf-8", errors="replace").strip()
        raise DownloadError(f"curl exit={r.returncode} per {url}: {stderr or 'unknown error'}")

    # Estrai HTTP status code dall'ultima riga di stdout
    stdout = r.stdout
    status_code = _parse_curl_status(stdout, r.stderr)
    body = _strip_curl_status(stdout, status_code)

    # Verifichiamo 200/206 (--fail-with-body già blocca 4xx/5xx ma teniamo il check)
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
