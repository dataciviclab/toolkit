"""HTTP POST file source plugin.

Downloads a file via HTTP POST with form-encoded body data.
Built on lab_connectors.http.HttpClient which provides SSL fallback
and configurable retry.

Usage in dataset.yml::

    raw:
      sources:
        - type: http_post_file
          args:
            url: "https://example.com/download"
            post_data:
              filename: "report.csv"
              category: "data"
"""

from __future__ import annotations

import logging
from pathlib import Path
from urllib.parse import urlparse

from lab_connectors.http import HttpClient

from toolkit.core.exceptions import DownloadError

logger = logging.getLogger("toolkit.plugins.http_post_file")

# Vedi http_file.py per la spiegazione.
_NON_TRUNCABLE_EXTS: set[str] = {
    ".parquet", ".zip", ".xlsx", ".xls", ".gz", ".bz2", ".7z", ".rar",
}


class HttpPostFileSource:
    """Download a file via HTTP POST with form-encoded data.

    Adapter over lab_connectors.http.HttpClient that translates
    HttpResult into toolkit's DownloadError contract.

    .. caution::
       Retry on POST is safe only for **idempotent** endpoints
       (file download queries). For state-mutating endpoints,
       set ``max_retries=0`` in ``raw.sources[].client``.
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

    def fetch(self, url: str, data: dict | None = None, sample_bytes: int | None = None) -> bytes:
        """Execute a POST request and return response bytes.

        Args:
            url: Target URL.
            data: Form-encoded POST body (dict of key-value pairs).
            sample_bytes: If set, adds ``Range: bytes=0-N`` header for
                partial download (non-standard on POST, alcuni server lo
                supportano). Ignorato per formati binari non troncabili.

        Returns:
            Raw response bytes.

        Raises:
            DownloadError: on network error or non-200 HTTP status.

        """
        if sample_bytes is not None:
            path = urlparse(url).path
            suffix = Path(path).suffix.lower()
            if suffix in _NON_TRUNCABLE_EXTS:
                logger.info(
                    "sample_bytes=%s ignorato per formato non troncabile (POST): %s",
                    sample_bytes,
                    url,
                )
                sample_bytes = None

        headers = None
        if sample_bytes is not None:
            headers = {"Range": f"bytes=0-{sample_bytes - 1}"}
        # File download via POST is idempotent — safe to retry
        result = self._client.post(url, data=data, headers=headers, retries=self.retries)
        if result.is_ok and result.response is not None:
            if result.response.status_code not in (200, 206):
                raise DownloadError(f"HTTP {result.response.status_code} for {url}")
            content = result.response.content
            if sample_bytes is not None and len(content) > sample_bytes:
                content = content[:sample_bytes]
                last_newline = content.rfind(b"\n")
                if last_newline > 0:
                    content = content[: last_newline + 1]
            return content
        err = result.err
        raise DownloadError(str(err) if err else f"Failed to fetch {url}")
