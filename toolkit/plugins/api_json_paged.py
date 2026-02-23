from __future__ import annotations

import json
import time
from typing import Any

import requests

from toolkit.core.exceptions import DownloadError
from toolkit.core.registry import registry


class ApiJsonPagedSource:
    """
    Fetch paginated JSON from an API and return CSV-bytes (header included).

    Assumptions (simple & flexible):
    - Each page returns either:
      A) a list of dicts, OR
      B) a dict containing a list under `items_path` (default "items")
    - Pagination increases by integer page: page=1..N (customizable)
    """

    def __init__(
        self,
        timeout: int = 60,
        retries: int = 2,
        sleep_seconds: float = 0.0,
        user_agent: str | None = None,
    ):
        self.timeout = timeout
        self.retries = retries
        self.sleep_seconds = sleep_seconds
        self.user_agent = user_agent or "dataciviclab-toolkit/0.1"

    def fetch(
        self,
        base_url: str,
        params: dict[str, Any] | None = None,
        *,
        page_param: str = "page",
        start_page: int = 1,
        items_path: str = "items",
        max_pages: int = 10_000,
    ) -> bytes:
        headers = {"User-Agent": self.user_agent}
        params = dict(params or {})
        all_items: list[dict[str, Any]] = []

        page = start_page
        last_err: Exception | None = None

        while page <= max_pages:
            p = dict(params)
            p[page_param] = page

            ok = False
            for _ in range(self.retries + 1):
                try:
                    r = requests.get(base_url, params=p, headers=headers, timeout=self.timeout)
                    r.raise_for_status()
                    payload = r.json()

                    if isinstance(payload, list):
                        items = payload
                    elif isinstance(payload, dict):
                        items = payload.get(items_path, [])
                    else:
                        items = []

                    if not items:
                        ok = True
                        page = max_pages + 1  # break outer
                        break

                    # keep only dict rows
                    all_items.extend([x for x in items if isinstance(x, dict)])

                    ok = True
                    break
                except Exception as e:
                    last_err = e
                    time.sleep(0.25)

            if not ok:
                raise DownloadError(str(last_err) if last_err else f"Failed to fetch {base_url}")

            page += 1
            if self.sleep_seconds:
                time.sleep(self.sleep_seconds)

        # Convert to CSV bytes (simple, deterministic)
        if not all_items:
            return b""

        # stable column order: union of keys, sorted
        cols = sorted({k for row in all_items for k in row.keys()})

        def esc(v: Any) -> str:
            if v is None:
                return ""
            if isinstance(v, (dict, list)):
                v = json.dumps(v, ensure_ascii=False)
            s = str(v)
            # CSV escaping
            if any(ch in s for ch in [",", "\n", "\r", '"']):
                s = '"' + s.replace('"', '""') + '"'
            return s

        lines = []
        lines.append(",".join(cols))
        for row in all_items:
            lines.append(",".join(esc(row.get(c)) for c in cols))

        return ("\n".join(lines) + "\n").encode("utf-8")


# Register plugin (so `stype: api_json_paged` works)
registry.register("api_json_paged", lambda **client: ApiJsonPagedSource(**client))