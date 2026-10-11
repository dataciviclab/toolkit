"""DBnomics source plugin.

Fetch da https://api.db.nomics.world/v22/series/{provider}/{dataset}
e produce un CSV lungo (una riga = osservazione).

Motivazione: ISTAT SDMX è rate-limited (5 req/min, ban 1–2gg) e spesso
timeout; DBnomics è il mirror pubblico dello stesso SDMX — discovery e
pipeline possono usare il canale senza bruciare il budget ISTAT.

Output colonne (contratto lungo):
  series_code, provider, dataset, <dim1>…<dimN>, TIME_PERIOD, OBS_VALUE

Le dimensioni variano per serie: il CSV usa l'unione delle chiavi
presenti; celle mancanti vuote.
"""

from __future__ import annotations

import csv
import io
import json
import logging
from typing import Any

from lab_connectors.http import HttpClient

from toolkit.core.exceptions import DownloadError

logger = logging.getLogger(__name__)

DBNOMICS_BASE = "https://api.db.nomics.world/v22"
DEFAULT_PROVIDER = "ISTAT"


class DbnomicsSource:
    """Scarica serie da DBnomics API v22 e le normalizza in CSV lungo."""

    def __init__(
        self,
        timeout: int = 120,
        retries: int = 2,
        user_agent: str | None = None,
        page_size: int = 1000,
        max_pages: int = 200,
        base_url: str | None = None,
    ):
        self.timeout = timeout
        self.retries = retries
        self.user_agent = user_agent or "dataciviclab-toolkit-dbnomics/0.1"
        self.page_size = max(1, min(int(page_size), 1000))  # API cap 1000
        self.max_pages = max(1, int(max_pages))
        self.base_url = (base_url or DBNOMICS_BASE).rstrip("/")
        self._client = HttpClient(
            timeout=timeout,
            max_retries=retries,
            user_agent=self.user_agent,
        )

    def fetch(
        self,
        dataset: str,
        provider: str = DEFAULT_PROVIDER,
        dimensions: dict[str, Any] | None = None,
        observations: bool = True,
        page_size: int | None = None,
        max_pages: int | None = None,
    ) -> bytes:
        dataset = (dataset or "").strip()
        if not dataset:
            raise DownloadError("dbnomics: dataset obbligatorio (es. 93_498 o ISTAT/93_498)")

        provider, dataset_id = self._split_dataset(dataset, provider)
        page = max(1, min(int(page_size or self.page_size), 1000))
        pages_cap = max(1, int(max_pages or self.max_pages))

        dims_json = self._serialize_dimensions(dimensions)

        rows: list[dict[str, Any]] = []
        offset = 0
        num_found: int | None = None

        while offset < pages_cap * page:
            params = {
                "observations": "1" if observations else "0",
                "limit": str(page),
                "offset": str(offset),
            }
            if dims_json:
                params["dimensions"] = dims_json

            payload = self._get_json(f"{provider}/{dataset_id}", params)

            series_block = payload.get("series") or {}
            docs = series_block.get("docs") or []
            if num_found is None:
                num_found = int(series_block.get("num_found") or 0)
                logger.info(
                    "dbnomics %s/%s num_found=%s page=%s",
                    provider,
                    dataset_id,
                    num_found,
                    page,
                )
            if not docs:
                break

            for doc in docs:
                rows.extend(self._series_to_rows(doc, provider, dataset_id))

            offset += len(docs)
            if len(docs) < page or offset >= num_found:
                break

        if not rows:
            raise DownloadError(
                f"dbnomics: nessuna osservazione per {provider}/{dataset_id}"
                + (f" dimensions={dims_json}" if dims_json else "")
            )

        return self._rows_to_csv(rows)

    def _split_dataset(self, dataset: str, provider: str) -> tuple[str, str]:
        if "/" in dataset:
            p, d = dataset.split("/", 1)
            return p, d
        return (provider or DEFAULT_PROVIDER).strip() or DEFAULT_PROVIDER, dataset

    def _serialize_dimensions(self, dimensions: dict[str, Any] | None) -> str:
        if not dimensions:
            return ""
        normalized: dict[str, list[str]] = {}
        for key, value in dimensions.items():
            if value is None:
                continue
            if isinstance(value, (list, tuple)):
                items = [str(v) for v in value if v is not None and str(v) != ""]
            else:
                items = [str(value)]
            if items:
                normalized[str(key)] = items
        if not normalized:
            return ""
        return json.dumps(normalized, separators=(",", ":"))

    def _get_json(self, path: str, params: dict[str, str]) -> dict:
        # path = "{provider}/{dataset}" → GET /v22/series/{provider}/{dataset}
        url = f"{self.base_url}/series/{path.lstrip('/')}"
        result = self._client.get(url, params=params)
        if not result.is_ok or result.response is None:
            err = result.err
            raise DownloadError(f"dbnomics request failed: {err or 'no response'} for {url}")
        response = result.response
        if response.status_code >= 400:
            raise DownloadError(f"dbnomics HTTP {response.status_code} for {url}")
        try:
            return json.loads(response.text)
        except json.JSONDecodeError as exc:
            preview = response.text[:200].replace("\n", " ")
            raise DownloadError(f"dbnomics invalid JSON from {url}: {preview!r}") from exc

    def _series_to_rows(self, doc: dict, provider: str, dataset: str) -> list[dict[str, Any]]:
        dims = doc.get("dimensions") or {}
        series_code = doc.get("series_code") or ""
        periods = doc.get("period") or []
        values = doc.get("value")

        if isinstance(values, dict):
            items = list(values.items())
        elif isinstance(values, list) and periods:
            items = list(zip(periods, values))
        else:
            items = []

        rows: list[dict[str, Any]] = []
        for period, value in items:
            if value is None:
                continue
            # DBnomics usa talvolta "NA" per missing
            if isinstance(value, str) and value.strip().upper() in {"", "NA", "N/A"}:
                continue
            row: dict[str, Any] = {
                "series_code": series_code,
                "provider": provider,
                "dataset": dataset,
            }
            for dim_id, dim_val in dims.items():
                row[str(dim_id)] = dim_val
            row["TIME_PERIOD"] = period
            try:
                row["OBS_VALUE"] = float(value)
            except (TypeError, ValueError):
                row["OBS_VALUE"] = value
            rows.append(row)
        return rows

    def _rows_to_csv(self, rows: list[dict[str, Any]]) -> bytes:
        # Unione ordinata delle colonne: fisse prima, poi dim in ordine di comparsa
        fixed = ["series_code", "provider", "dataset"]
        dim_cols: list[str] = []
        seen = set(fixed) | {"TIME_PERIOD", "OBS_VALUE"}
        for row in rows:
            for key in row:
                if key not in seen:
                    dim_cols.append(key)
                    seen.add(key)
        fieldnames = fixed + dim_cols + ["TIME_PERIOD", "OBS_VALUE"]

        buffer = io.StringIO(newline="")
        writer = csv.DictWriter(buffer, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
        return buffer.getvalue().encode("utf-8")
