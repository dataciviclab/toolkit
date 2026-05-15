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

from lab_connectors.http import HttpClient

from toolkit.core.exceptions import DownloadError

logger = logging.getLogger("toolkit.plugins.http_post_file")


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

    def fetch(self, url: str, data: dict | None = None) -> bytes:
        """Execute a POST request and return response bytes.

        Args:
            url: Target URL.
            data: Form-encoded POST body (dict of key-value pairs).

        Returns:
            Raw response bytes.

        Raises:
            DownloadError: on network error or non-200 HTTP status.

        """
        result = self._client.post(url, data=data)
        if result.is_ok and result.response is not None:
            if result.response.status_code != 200:
                raise DownloadError(f"HTTP {result.response.status_code} for {url}")
            return result.response.content
        err = result.err
        raise DownloadError(str(err) if err else f"Failed to fetch {url}")
