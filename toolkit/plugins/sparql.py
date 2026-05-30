"""SPARQL source plugin.

Fetches tabular data from a SPARQL endpoint via HTTP POST.
Supports direct CSV responses and SPARQL Results JSON (converted to CSV).
"""

from __future__ import annotations

import csv
import io
import json
from typing import Any

from lab_connectors.http import HttpClient

from toolkit.core.exceptions import DownloadError


class SparqlSource:
    """Query a SPARQL endpoint and return results as CSV bytes.

    Uses HttpClient.post() with retries=2 (safe because SPARQL POST
    is idempotent: same query → same result).
    """

    def __init__(self, timeout: int = 60):
        self._client = HttpClient(timeout=timeout)

    def fetch(
        self,
        endpoint: str,
        query: str,
        accept_format: str = "csv",
        pages: int = 1,
        step: int = 10000,
    ) -> tuple[bytes, str]:
        """Execute a SPARQL query and return CSV data.

        Quando l'endpoint SPARQL ha un limite di righe per risposta (WAF),
        usa ``pages`` e ``step`` per fare piu' query con OFFSET incrementale
        e concatenare i risultati in un unico CSV.

        Args:
            endpoint: SPARQL endpoint URL.
            query: SPARQL SELECT query string.
            accept_format: 'csv' for direct CSV, 'sparql-results+json' for JSON conversion.
            pages: Numero di pagine da fetchare (default 1 = nessuna paginazione).
            step: Righe per pagina (default 10000).

        Returns:
            (csv_bytes, endpoint) tuple.

        Raises:
            DownloadError: on network error, non-200 response, or empty results.
        """
        if not endpoint:
            raise DownloadError("SPARQL source requires endpoint URL")
        if not query:
            raise DownloadError("SPARQL source requires a query")
        if accept_format not in {"csv", "sparql-results+json"}:
            raise DownloadError(
                f"Unsupported accept_format '{accept_format}'. "
                "Supported values: 'csv', 'sparql-results+json'."
            )

        # Se e' richiesta paginazione, appende OFFSET alla query
        # (dopo LIMIT, che deve essere presente nella query)
        def _do_fetch(offset: int = 0) -> bytes:
            q = query
            if offset > 0:
                q = f"{q.rstrip().rstrip(';')} OFFSET {offset}"
            headers: dict[str, str] = {
                "Accept": "application/sparql-results+json" if accept_format == "sparql-results+json" else "text/csv",
                "Content-Type": "application/x-www-form-urlencoded",
            }
            params: dict[str, Any] = {"query": q}
            result = self._client.post(endpoint, data=params, headers=headers, retries=2)
            if result.is_error:
                raise DownloadError(f"SPARQL request failed for {endpoint}: {result.err}") from result.err
            r = result.response
            if r.status_code != 200:
                raise DownloadError(f"SPARQL endpoint returned HTTP {r.status_code} for {endpoint}: {r.text[:200]}")
            content_type = r.headers.get("Content-Type", "")
            if "text/csv" in content_type:
                return r.content
            if "sparql-results+json" in content_type:
                return _sparql_json_to_csv(r.text)
            if "text/plain" in content_type:
                stripped = r.text.strip()
                if stripped.startswith("{"):
                    try:
                        return _sparql_json_to_csv(r.text)
                    except (DownloadError, json.JSONDecodeError):
                        pass
                else:
                    return r.content
            raise DownloadError(
                f"Unsupported Content-Type '{content_type}' for SPARQL fetch. "
                "Expected 'text/csv' or 'application/sparql-results+json'."
            )

        # Prima pagina
        all_bytes = _do_fetch(offset=0)

        # Paginazione: pagine successive, concatena CSV (saltando l'header)
        for page in range(1, pages):
            try:
                page_bytes = _do_fetch(offset=page * step)
                # Trova la prima riga (header) e concatena solo i dati
                header_end = all_bytes.find(b"\n")
                if header_end >= 0:
                    # Mantieni l'header della prima pagina, aggiungi solo i dati
                    data_start = page_bytes.find(b"\n")
                    if data_start >= 0 and len(page_bytes) > data_start + 1:
                        all_bytes = all_bytes + page_bytes[data_start + 1:]
                else:
                    all_bytes = all_bytes + page_bytes
            except DownloadError:
                # Endpoint non ha piu' pagine → esci
                break

        return all_bytes, endpoint

        headers: dict[str, str] = {
            "Accept": "application/sparql-results+json" if accept_format == "sparql-results+json" else "text/csv",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        params: dict[str, Any] = {"query": query}

        result = self._client.post(
            endpoint,
            data=params,
            headers=headers,
            retries=2,
        )
        if result.is_error:
            raise DownloadError(
                f"SPARQL request failed for {endpoint}: {result.err}"
            ) from result.err

        r = result.response
        if r.status_code != 200:
            raise DownloadError(
                f"SPARQL endpoint returned HTTP {r.status_code} for {endpoint}: {r.text[:200]}"
            )

        content_type = r.headers.get("Content-Type", "")

        if "text/csv" in content_type:
            return r.content, endpoint

        if "sparql-results+json" in content_type:
            csv_bytes = _sparql_json_to_csv(r.text)
            return csv_bytes, endpoint

        # Fallback per Content-Type text/plain: alcuni endpoint SPARQL
        # (es. dati.camera.it) rispondono con Content-Type sbagliato
        # ma corpo CSV o JSON valido. Altri Content-Type (es. application/xml)
        # rimangono errore.
        if "text/plain" in content_type:
            stripped = r.text.strip()
            if stripped.startswith("{"):
                try:
                    return _sparql_json_to_csv(r.text), endpoint
                except (DownloadError, json.JSONDecodeError):
                    pass
            else:
                # Assume CSV — se non è CSV, fallirà in CLEAN con errore chiaro
                return r.content, endpoint

        raise DownloadError(
            f"Unsupported Content-Type '{content_type}' for SPARQL fetch. "
            "Expected 'text/csv' or 'application/sparql-results+json'."
        )

    def probe(
        self,
        endpoint: str,
        query: str,
        limit: int = 100,
    ) -> dict[str, Any]:
        """Probe a SPARQL endpoint: execute query and return schema + stats.

        Does NOT save any file. Use for inspection before creating a candidate.

        Args:
            endpoint: SPARQL endpoint URL.
            query: SPARQL SELECT query. A LIMIT will be appended if not present.
            limit: Maximum rows to fetch for stats (default 100). sample_rows is
                capped at 5 regardless of this value.

        Returns:
            dict with keys: variables, row_count, null_counts, distinct_counts,
            sample_rows, warnings, query_time_ms, endpoint.
        """
        import time

        if not endpoint:
            raise DownloadError("SPARQL probe requires endpoint URL")
        if not query:
            raise DownloadError("SPARQL probe requires a query")

        # Ensure LIMIT is present in query
        safe_query = query.strip()
        if "limit" not in safe_query.lower():
            safe_query = f"{safe_query.rstrip(';')} LIMIT {limit}"

        headers: dict[str, str] = {
            "Accept": "application/sparql-results+json",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        params: dict[str, Any] = {"query": safe_query}

        start = time.monotonic()
        result = self._client.post(
            endpoint,
            data=params,
            headers=headers,
            retries=0,
        )
        if result.is_error:
            raise DownloadError(
                f"SPARQL probe failed for {endpoint}: {result.err}"
            ) from result.err

        r = result.response
        query_time_ms = int((time.monotonic() - start) * 1000)

        if r.status_code != 200:
            raise DownloadError(
                f"SPARQL endpoint returned HTTP {r.status_code} for {endpoint}: {r.text[:200]}"
            )

        try:
            payload = json.loads(r.text)
        except json.JSONDecodeError as e:
            raise DownloadError(f"Invalid SPARQL JSON response during probe: {e}") from e

        bindings: list[dict[str, Any]] = (
            (payload.get("results") or {}).get("bindings") or []
        )
        if not isinstance(bindings, list):
            raise DownloadError("SPARQL probe: unexpected payload structure")

        vars_list: list[str] = (payload.get("head") or {}).get("vars") or []
        if not bindings and not vars_list:
            raise DownloadError("SPARQL probe: query returned no variables")

        # If vars_list is empty but we have bindings, infer from first binding
        if not vars_list and bindings:
            vars_list = list(bindings[0].keys())

        row_count = len(bindings)

        # Compute null and distinct counts per variable
        null_counts: dict[str, int] = {v: 0 for v in vars_list}
        distinct_counts: dict[str, set[str]] = {v: set() for v in vars_list}
        sample_rows: list[dict[str, str]] = []

        for i, binding in enumerate(bindings):
            row: dict[str, str] = {}
            for var in vars_list:
                cell = binding.get(var)
                value: str
                if cell and isinstance(cell, dict):
                    raw_value = cell.get("value")
                    # SPARQL JSON can have null values as JSON null
                    if raw_value is None:
                        value = ""
                    else:
                        value = str(raw_value)
                else:
                    value = str(cell if cell is not None else "")
                row[var] = value
                if not value:
                    null_counts[var] = null_counts.get(var, 0) + 1
                else:
                    distinct_counts[var].add(value)
            if i < 5:
                sample_rows.append(row)

        warnings: list[str] = []
        for var in vars_list:
            if null_counts.get(var, 0) > 0:
                warnings.append(f"variable '{var}' has {null_counts[var]} null/unbound value(s)")

        return {
            "endpoint": endpoint,
            "variables": vars_list,
            "row_count": row_count,
            "null_counts": null_counts,
            "distinct_counts": {v: len(s) for v, s in distinct_counts.items()},
            "sample_rows": sample_rows,
            "warnings": warnings,
            "query_time_ms": query_time_ms,
        }


def _sparql_json_to_csv(json_text: str) -> bytes:
    """Convert SPARQL Results JSON to CSV bytes.

    Assumes all bindings share the same set of variables (homogeneous schema).
    Extra keys in later bindings or missing keys produce empty values silently.
    """
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

    # Use head.vars as canonical column list; bindings may omit unbound variables.
    var_names: list[str] = (payload.get("head") or {}).get("vars") or list(bindings[0].keys())
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
