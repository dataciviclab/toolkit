"""SPARQL source plugin.

Fetches tabular data from a SPARQL endpoint via HTTP (POST + GET fallback).
Supports direct CSV responses and SPARQL Results JSON (converted to CSV).
"""

from __future__ import annotations

import csv
import io
import json
import urllib.parse
from typing import Any

from lab_connectors.http import HttpClient

from toolkit.core.exceptions import DownloadError


class SparqlSource:
    """Query a SPARQL endpoint and return results as CSV bytes.

    Tenta POST form-encoded (standard SPARQL protocol).
    Se POST fallisce (403, timeout), prova GET URL-encoded (Virtuoso, WAF).
    """

    def __init__(self, timeout: int = 60):
        self._client = HttpClient(timeout=timeout)

    def _do_fetch(self, endpoint: str, q: str, accept_format: str) -> bytes:
        """Esegue una singola query SPARQL, restituisce CSV bytes.

        Tenta POST → GET fallback.
        Supporta CSV diretto e SPARQL Results JSON (convertito a CSV).
        """
        is_json = accept_format == "sparql-results+json"
        headers: dict[str, str] = {
            "Accept": (
                "application/sparql-results+json"
                if is_json
                else "text/csv,text/plain;q=0.5"
            ),
        }
        post_data = {"query": q}

        # --- Tentativo 1: POST ---
        result = self._client.post(
            endpoint, post_data, headers=headers, retries=2,
        )
        if result.is_ok:
            return self._parse_response(result.response, is_json)

        # --- Tentativo 2: GET fallback ---
        url = f"{endpoint}?query={urllib.parse.quote(q)}"
        get_headers = {
            "Accept": (
                "application/sparql-results+xml,"
                "application/sparql-results+json,application/json,text/csv"
            ),
        }
        result = self._client.get(url, headers=get_headers)
        if result.is_ok:
            return self._parse_response(result.response, is_json)

        raise DownloadError(
            f"SPARQL request failed for {endpoint}: "
            f"POST → {result.err or 'unknown'}"
        )

    def _parse_response(self, r: Any, prefer_json: bool) -> bytes:
        """Parsa la risposta HTTP in CSV bytes."""
        content_type = (r.headers.get("Content-Type") or "").lower()

        if r.status_code != 200:
            raise DownloadError(
                f"SPARQL endpoint returned HTTP {r.status_code} "
                f"for {r.url}: {r.text[:200]}"
            )

        # CSV diretto
        if "text/csv" in content_type:
            return r.content

        # SPARQL Results JSON (standard o fallback)
        if prefer_json or "sparql-results+json" in content_type or "json" in content_type:
            return _sparql_json_to_csv(r.text)

        # text/plain: potrebbe essere CSV o JSON con Content-Type sbagliato
        if "text/plain" in content_type:
            stripped = r.text.strip()
            if stripped.startswith("{"):
                try:
                    return _sparql_json_to_csv(r.text)
                except (DownloadError, json.JSONDecodeError):
                    pass
            else:
                # Assume CSV — se non è CSV, fallirà in CLEAN con errore chiaro
                return r.content

        # XML SPARQL Results — non supportato
        if "sparql-results+xml" in content_type:
            raise DownloadError(
                "SPARQL endpoint returned XML results. "
                "Request JSON or CSV format."
            )

        raise DownloadError(
            f"Unsupported Content-Type '{content_type}' for SPARQL fetch. "
            "Expected 'text/csv' or 'application/sparql-results+json'."
        )

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

        # Se e' richiesta paginazione, assicura che la query abbia LIMIT
        if pages > 1:
            if "limit" not in query.lower():
                query = f"{query.rstrip().rstrip(';')} LIMIT {step}"

        # Prima pagina
        all_bytes = self._do_fetch(endpoint, query, accept_format)

        # Paginazione: pagine successive, concatena CSV (saltando l'header)
        for page in range(1, pages):
            try:
                q = query
                if "offset" not in q.lower():
                    q = f"{q.rstrip().rstrip(';')} OFFSET {page * step}"
                else:
                    q = f"{q.rstrip().rstrip(';')} OFFSET {page * step}"

                page_bytes = self._do_fetch(endpoint, q, accept_format)

                # Se la pagina e' vuota (solo header, nessun dato), fermati
                data_start = page_bytes.find(b"\n")
                if data_start < 0 or len(page_bytes) <= data_start + 1:
                    break
                # Concatena solo i dati (salta l'header)
                header_end = all_bytes.find(b"\n")
                if header_end >= 0:
                    all_bytes = all_bytes + page_bytes[data_start + 1:]
                else:
                    all_bytes = all_bytes + page_bytes
            except DownloadError:
                # Endpoint non ha piu' pagine (es. OFFSET oltre la fine) → esci
                break

        return all_bytes, endpoint

    def _fetch_bindings(
        self, endpoint: str, query: str,
    ) -> list[dict[str, Any]]:
        """Esegue query SPARQL e restituisce i bindings JSON (per probe).

        Separato da _do_fetch per consentire ai test di mockare
        solo il recupero bindings senza toccare la logica CSV.
        """
        from lab_connectors.http.sparql import execute_sparql

        try:
            t = self._client.timeout
            timeout_val = t[0] if isinstance(t, tuple) else t
            return execute_sparql(endpoint, query, timeout=int(timeout_val))
        except RuntimeError as e:
            raise DownloadError(str(e)) from e

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

        start = time.monotonic()
        bindings = self._fetch_bindings(endpoint, safe_query)
        query_time_ms = int((time.monotonic() - start) * 1000)

        if not bindings:
            raise DownloadError("SPARQL probe: query returned no results")

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
                value: str = (cell.get("value") or "") if cell else ""
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
                warnings.append(
                    f"variable '{var}' has {null_counts[var]} null/unbound value(s)"
                )

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

    var_names: list[str] = (
        (payload.get("head") or {}).get("vars") or list(bindings[0].keys())
    )
    rows: list[dict[str, str]] = []

    for binding in bindings:
        row: dict[str, str] = {}
        for var in var_names:
            cell = binding.get(var)
            if cell and isinstance(cell, dict):
                row[var] = str(cell.get("value", ""))
            else:
                row[var] = str(cell if cell is not None else "")
        rows.append(row)

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=var_names)
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue().encode("utf-8")
