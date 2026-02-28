from __future__ import annotations

import pandas as pd
import requests

from toolkit.core.exceptions import DownloadError


class HtmlTableSource:
    """Fetch an HTML page, parse the first table (or by index), return CSV bytes."""

    def __init__(self, timeout: int = 60, retries: int = 2, user_agent: str | None = None):
        self.timeout = timeout
        self.retries = retries
        self.user_agent = user_agent or "dataciviclab-toolkit/0.1"

    def fetch(self, url: str, *, table_index: int = 0) -> bytes:
        headers = {"User-Agent": self.user_agent}
        last_err: Exception | None = None

        for _ in range(self.retries + 1):
            try:
                r = requests.get(url, headers=headers, timeout=self.timeout)
                r.raise_for_status()
                tables = pd.read_html(r.text)
                if not tables:
                    return b""
                idx = max(0, min(table_index, len(tables) - 1))
                return tables[idx].to_csv(index=False).encode("utf-8")
            except Exception as e:
                last_err = e

        raise DownloadError(str(last_err) if last_err else f"Failed to parse HTML tables from {url}")
