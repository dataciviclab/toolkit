"""SPARQL source plugin.

Fetches tabular data from a SPARQL endpoint via HTTP POST.
Supports direct CSV responses and SPARQL Results JSON (converted to CSV).
"""

from __future__ import annotations

import csv
import io
from typing import Any

import requests

from toolkit.core.exceptions import DownloadError


class SparqlSource:
    """Query a SPARQL endpoint and return results as CSV bytes."""

    def __init__(self, timeout: int = 60):
        self.timeout = timeout

    def fetch(
        self,
        endpoint: str,
        query: str,
        accept_format: str = "csv",
    ) -> tuple[bytes, str]:
        """Execute a SPARQL query and return CSV data.

        Args:
            endpoint: SPARQL endpoint URL.
            query: SPARQL SELECT query string.
            accept_format: 'csv' for direct CSV, 'sparql-results+json' for JSON conversion.

        Returns:
            (csv_bytes, endpoint) tuple.

        Raises:
            DownloadError: on network error, non-200 response, or empty results.
        """
        if not endpoint:
            raise DownloadError("SPARQL source requires endpoint URL")
        if not query:
            raise DownloadError("SPARQL source requires a query")

        headers: dict[str, str] = {
            "Accept": "application/sparql-results+json" if accept_format == "sparql-results+json" else "text/csv",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        params: dict[str, Any] = {"query": query}

        try:
            r = requests.post(
                endpoint,
                data=params,
                headers=headers,
                timeout=self.timeout,
            )
        except Exception as e:
            raise DownloadError(f"SPARQL request failed for {endpoint}: {e}") from e

        if r.status_code != 200:
            raise DownloadError(
                f"SPARQL endpoint returned HTTP {r.status_code} for {endpoint}: {r.text[:200]}"
            )

        content_type = r.headers.get("Content-Type", "")

        if "text/csv" in content_type or accept_format == "csv":
            payload: bytes | str = r.content
            if isinstance(payload, bytes):
                payload = payload.decode("utf-8", errors="replace")
            return payload.encode("utf-8"), endpoint

        if "sparql-results+json" in content_type or accept_format == "sparql-results+json":
            csv_bytes = _sparql_json_to_csv(r.text)
            return csv_bytes, endpoint

        # Fallback: treat as text
        text = r.text
        return text.encode("utf-8"), endpoint


def _sparql_json_to_csv(json_text: str) -> bytes:
    """Convert SPARQL Results JSON to CSV bytes."""
    import json

    try:
        payload = json.loads(json_text)
    except json.JSONDecodeError as e:
        raise DownloadError(f"Invalid SPARQL JSON response: {e}") from e

    bindings: list[dict[str, Any]] = (
        (payload.get("results") or {}).get("bindings") or []
    )
    if not isinstance(bindings, list):
        raise DownloadError("SPARQL JSON payload has unexpected structure")

    if not bindings:
        raise DownloadError("SPARQL query returned no results")

    # Collect all variable names from the first binding
    var_names: list[str] = list(bindings[0].keys())
    rows: list[dict[str, str]] = []

    for binding in bindings:
        row: dict[str, str] = {}
        for var in var_names:
            cell = binding.get(var)
            if cell and isinstance(cell, dict):
                # SPARQL JSON binding: {"value": "...", "type": "..."}
                row[var] = str(cell.get("value", ""))
            else:
                row[var] = str(cell if cell is not None else "")
        rows.append(row)

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=var_names)
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue().encode("utf-8")
