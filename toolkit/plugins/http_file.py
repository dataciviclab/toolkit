import requests

from toolkit.core.exceptions import DownloadError


class HttpFileSource:
    """Download a file via HTTP(S)."""

    def __init__(self, timeout: int = 60, retries: int = 2, user_agent: str | None = None):
        self.timeout = timeout
        self.retries = retries
        self.user_agent = user_agent or "dataciviclab-toolkit/0.1"

    def fetch(self, url: str) -> bytes:
        headers = {"User-Agent": self.user_agent}
        last_err: Exception | None = None
        for _ in range(max(1, self.retries)):
            try:
                r = requests.get(url, timeout=self.timeout, headers=headers)
                if r.status_code != 200:
                    raise DownloadError(f"HTTP {r.status_code} for {url}")
                return r.content
            except Exception as e:
                last_err = e

        raise DownloadError(str(last_err) if last_err else f"Failed to fetch {url}")
