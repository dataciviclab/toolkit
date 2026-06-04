from __future__ import annotations

import logging

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
        err = result.err
        raise DownloadError(str(err) if err else f"Failed to fetch {url}")
