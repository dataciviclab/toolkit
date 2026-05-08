from __future__ import annotations

import logging

from toolkit.core.exceptions import DownloadError

logger = logging.getLogger("toolkit.plugins.http_file")


class HttpFileSource:
    """Download a file via HTTP(S) with SSL fallback for expired/invalid certs.

    Adapter over lab_connectors.http.HttpClient that translates
    HttpResult into toolkit's DownloadError contract.

    HttpClient is imported lazily so the module loads without lab-connectors
    installed (e.g. in CI smoke tests that never instantiate this class).
    """

    def __init__(self, timeout: int = 60, retries: int = 2, user_agent: str | None = None):
        from lab_connectors.http import HttpClient

        self.timeout = timeout
        self.retries = retries
        self.user_agent = user_agent or "dataciviclab-toolkit/0.1"
        self._client = HttpClient(
            timeout=timeout,
            max_retries=retries,
            user_agent=self.user_agent,
        )

    def fetch(self, url: str) -> bytes:
        result = self._client.get(url)
        if result.is_ok and result.response is not None:
            if result.response.status_code != 200:
                raise DownloadError(f"HTTP {result.response.status_code} for {url}")
            return result.response.content
        err = result.err
        raise DownloadError(str(err) if err else f"Failed to fetch {url}")
