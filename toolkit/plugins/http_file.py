import requests
import urllib3
from urllib3.exceptions import InsecureRequestWarning

from toolkit.core.exceptions import DownloadError


class HttpFileSource:
    """Download a file via HTTP(S) with SSL fallback for expired/invalid certs."""

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
            except requests.exceptions.SSLError:
                # SSL fallback: make a fresh request with verify=False
                urllib3.disable_warnings(InsecureRequestWarning)
                try:
                    # Create fresh session to avoid urllib3 retry state carry-over
                    session = requests.Session()
                    r = session.get(url, timeout=self.timeout, headers=headers, verify=False)
                    if r.status_code != 200:
                        raise DownloadError(f"HTTP {r.status_code} for {url}")
                    return r.content
                except Exception as e:
                    last_err = e
                    continue  # try again in next outer iteration
            except Exception as e:
                last_err = e
                continue  # try again in next outer iteration

        raise DownloadError(str(last_err) if last_err else f"Failed to fetch {url}")
