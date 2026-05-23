from __future__ import annotations

import logging

from lab_connectors.http import HttpClient

from toolkit.core.exceptions import DownloadError

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
        headers = None
        if sample_bytes is not None:
            headers = {"Range": f"bytes=0-{sample_bytes - 1}"}
        result = self._client.get(url, headers=headers)
        if result.is_ok and result.response is not None:
            if result.response.status_code not in (200, 206):
                raise DownloadError(f"HTTP {result.response.status_code} for {url}")
            content = result.response.content
            # Troncamento locale: server che ignorano Range (200 invece di 206)
            # restituiscono tutto il file. Taglia per garantire il limite byte.
            if sample_bytes is not None and len(content) > sample_bytes:
                content = content[:sample_bytes]
            return content
        err = result.err
        raise DownloadError(str(err) if err else f"Failed to fetch {url}")
