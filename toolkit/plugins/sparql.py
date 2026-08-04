"""SPARQL source plugin.

Fetches tabular data from a SPARQL endpoint via HTTP (POST + GET fallback).
Supports direct CSV responses and SPARQL Results JSON (converted to CSV).
"""

from __future__ import annotations

import csv
import io
import json
import time
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
        # Retry dedicati per le pagine successive (la prima ha retries in _do_fetch).
        # Un errore su una pagina NON è "fine dati": va ritentato e, se persiste,
        # propagato per far fallire il run invece di troncare silenziosamente.
        self._page_retries = 3
        self._page_retry_delay = 5.0

    def _do_fetch(self, endpoint: str, q: str, accept_format: str) -> bytes:
        """Esegue una singola query SPARQL, restituisce CSV bytes.

        Tenta POST → GET fallback.
        Supporta CSV diretto e SPARQL Results JSON (convertito a CSV).
        """
        is_json = accept_format == "sparql-results+json"
        headers: dict[str, str] = {
            "Accept": (
                "application/sparql-results+json" if is_json else "text/csv,text/plain;q=0.5"
            ),
        }
        post_data = {"query": q}

        # --- Tentativo 1: POST ---
        result = self._client.post(
            endpoint,
            post_data,
            headers=headers,
            retries=2,
        )
        # is_ok è True anche su errori HTTP (response presente, err None) —
        # ma una risposta 4xx/5xx NON è un risultato SPARQL valido.
        # Fallback GET quando il POST non produce dati validi:
        #   - 4xx (WAF/Virtuoso rifiutano POST, es. 403 Senato)
        #   - errore di rete/timeout (post_status None)
        # Su 5xx NON si fa fallback (errore server reale, non va mascherato).
        post_status = (
            result.response.status_code if result.is_ok and result.response is not None else None
        )
        if post_status is not None and post_status < 400:
            return self._parse_response(result.response, is_json)

        # --- Tentativo 2: GET fallback (4xx o errore di rete/timeout) ---
        if post_status is None or 400 <= post_status < 500:
            url = f"{endpoint}?query={urllib.parse.quote(q)}"
            get_headers = {
                "Accept": (
                    "application/sparql-results+xml,"
                    "application/sparql-results+json,application/json,text/csv"
                ),
            }
            result = self._client.get(url, headers=get_headers)
            get_status = (
                result.response.status_code
                if result.is_ok and result.response is not None
                else None
            )
            if get_status is not None and get_status < 400:
                return self._parse_response(result.response, is_json)
            if get_status is not None and get_status >= 500:
                body = (result.response.text or "")[:200] if result.response is not None else ""
                raise DownloadError(
                    f"SPARQL GET fallback returned HTTP {get_status} for {endpoint}: {body}"
                )

        if post_status is not None and post_status >= 500:
            body = (result.response.text or "")[:200] if result.response is not None else ""
            raise DownloadError(
                f"SPARQL endpoint returned HTTP {post_status} for {endpoint}: {body}"
            )

        raise DownloadError(
            f"SPARQL request failed for {endpoint}: POST → {result.err or 'unknown'}"
        )

    def _parse_response(self, r: Any, prefer_json: bool) -> bytes:
        """Parsa la risposta HTTP in CSV bytes."""
        content_type = (r.headers.get("Content-Type") or "").lower()

        if r.status_code != 200:
            raise DownloadError(
                f"SPARQL endpoint returned HTTP {r.status_code} for {r.url}: {r.text[:200]}"
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
            raise DownloadError("SPARQL endpoint returned XML results. Request JSON or CSV format.")

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
        pagination: dict | None = None,
    ) -> tuple[bytes, str]:
        """Execute a SPARQL query and return CSV data.

        Quando l'endpoint SPARQL ha un limite di righe per risposta (WAF),
        usa ``pages`` e ``step`` per fare piu' query con OFFSET incrementale
        e concatenare i risultati in un unico CSV.

        ``pagination`` supporta due modalita':
        - ``{"mode": "offset"}`` (default): paginazione OFFSET con ``pages``/``step``.
          Funziona solo se l'endpoint accetta OFFSET con ORDER BY (NON tutti
          gli endpoint Virtuoso lo accettano — es. dati.camera.it rifiuta con
          errore SR353 quando ORDER BY + OFFSET supera 10k righe).
        - ``{"mode": "keyset", "key": "?var"}``: paginazione keyset. La query
          DEVE avere gia' ``ORDER BY ?var``. Le pagine successive iniettano
          ``FILTER(?var > <ultimo_valore>)`` al posto di OFFSET — deterministico
          e compatibile con i limiti Virtuoso.

        Args:
            endpoint: SPARQL endpoint URL.
            query: SPARQL SELECT query string.
            accept_format: 'csv' for direct CSV, 'sparql-results+json' for JSON conversion.
            pages: Numero di pagine da fetchare (default 1 = nessuna paginazione).
            step: Righe per pagina (default 10000).
            pagination: Config paginazione keyset/offset (vedi sopra).

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

        pagination_mode = (pagination or {}).get("mode", "offset")
        keyset_key = (pagination or {}).get("key")
        if pagination_mode == "keyset" and not keyset_key:
            raise DownloadError(
                'pagination mode=keyset requires \'key\' (es. {"mode": "keyset", "key": "?deputato"})'
            )

        # Se e' richiesta paginazione, assicura che la query abbia LIMIT
        if pages > 1 or pagination_mode == "keyset":
            if "limit" not in query.lower():
                query = f"{query.rstrip().rstrip(';')} LIMIT {step}"

        # Prima pagina — con retry+backoff dedicati: su endpoint instabili
        # (es. Camera 503/500 intermittenti) la prima query pesante riceve
        # spesso il 503 iniziale; _do_fetch ha retries ma senza delay sufficiente.
        all_bytes = self._fetch_page_with_retry(endpoint, query, accept_format)

        # Fine dati legittima sulla prima pagina: header vuoto o pagina corta
        # (< step righe di dati). Su endpoint come Camera l'OFFSET oltre la fine
        # risponde 503 invece di 200 con 0 righe: la fine va rilevata qui,
        # non da un errore HTTP.
        data_start = all_bytes.find(b"\n")
        if data_start < 0 or len(all_bytes) <= data_start + 1:
            return all_bytes, endpoint
        if _csv_data_rows(all_bytes) < step:
            return all_bytes, endpoint

        if pagination_mode == "keyset":
            all_bytes = self._fetch_keyset_pages(
                endpoint,
                query,
                accept_format,
                keyset_key,
                step,
                all_bytes,
            )
            return all_bytes, endpoint

        # Paginazione OFFSET: pagine successive, concatena CSV (saltando l'header)
        for page in range(1, pages):
            q = query
            if "offset" not in q.lower():
                q = f"{q.rstrip().rstrip(';')} OFFSET {page * step}"
            else:
                q = f"{q.rstrip().rstrip(';')} OFFSET {page * step}"

            page_bytes = self._fetch_page_with_retry(endpoint, q, accept_format)

            # Se la pagina e' vuota (solo header, nessun dato), fermati —
            # unica condizione legittima di fine dati.
            data_start = page_bytes.find(b"\n")
            if data_start < 0 or len(page_bytes) <= data_start + 1:
                break
            # Pagina corta (< step righe di dati) = ultima pagina: fermati
            # senza fare OFFSET oltre la fine (che su alcuni endpoint — es.
            # Camera — risponde 503 invece di 200 con 0 righe).
            if _csv_data_rows(page_bytes) < step:
                all_bytes = all_bytes + page_bytes[data_start + 1 :]
                break
            # Concatena solo i dati (salta l'header)
            header_end = all_bytes.find(b"\n")
            if header_end >= 0:
                all_bytes = all_bytes + page_bytes[data_start + 1 :]
            else:
                all_bytes = all_bytes + page_bytes

        return all_bytes, endpoint

    def _fetch_page_with_retry(
        self,
        endpoint: str,
        q: str,
        accept_format: str,
    ) -> bytes:
        """Fetch di una pagina con retry; propaga l'errore dopo i tentativi.

        Un errore di rete su una pagina NON e' "fine dati": ritenta e, se
        persiste, propaga per far fallire il run invece di troncare il CSV
        (regression: issue #449).
        """
        last_error: DownloadError | None = None
        for attempt in range(self._page_retries):
            try:
                return self._do_fetch(endpoint, q, accept_format)
            except DownloadError as exc:
                last_error = exc
                if attempt < self._page_retries - 1:
                    time.sleep(self._page_retry_delay)
        raise last_error  # type: ignore[misc]

    def _fetch_keyset_pages(
        self,
        endpoint: str,
        query: str,
        accept_format: str,
        key: str,
        step: int,
        all_bytes: bytes,
    ) -> bytes:
        """Paginazione keyset: inietta FILTER(?key > <last>) nelle pagine successive.

        Deterministico e compatibile con endpoint Virtuoso che rifiutano
        ORDER BY + OFFSET (SR353) — es. dati.camera.it.
        """
        if not key.startswith("?"):
            key = f"?{key}"
        # normalizza: niente ';' finale per poter concatenare la clausola
        base_query = query.rstrip().rstrip(";")

        while True:
            last_value = _csv_last_value(all_bytes, key.lstrip("?"))
            if last_value is None:
                break
            # Inietta il FILTER prima del ORDER BY / LIMIT (se presenti)
            q = base_query
            filter_clause = f"FILTER({key} > <{last_value}>)"
            for kw in ("ORDER BY", "LIMIT", "OFFSET"):
                idx = q.upper().rfind(kw)
                if idx != -1:
                    q = q[:idx] + filter_clause + " " + q[idx:]
                    break
            else:
                q = q + " " + filter_clause

            page_bytes = self._fetch_page_with_retry(endpoint, q, accept_format)

            data_start = page_bytes.find(b"\n")
            if data_start < 0 or len(page_bytes) <= data_start + 1:
                break  # pagina vuota = fine dati
            # Pagina corta (< step) = ultima pagina: concatena e ferma
            if _csv_data_rows(page_bytes) < step:
                all_bytes = all_bytes + page_bytes[data_start + 1 :]
                break
            header_end = all_bytes.find(b"\n")
            if header_end >= 0:
                all_bytes = all_bytes + page_bytes[data_start + 1 :]
            else:
                all_bytes = all_bytes + page_bytes

        return all_bytes

    def _fetch_bindings(
        self,
        endpoint: str,
        query: str,
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

    bindings: list[dict[str, Any]] = (payload.get("results") or {}).get("bindings") or []
    if not isinstance(bindings, list):
        raise DownloadError("SPARQL JSON payload has unexpected structure")

    if not bindings:
        raise DownloadError("SPARQL query returned no results")

    var_names: list[str] = (payload.get("head") or {}).get("vars") or list(bindings[0].keys())
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


def _csv_data_rows(csv_bytes: bytes) -> int:
    """Conta le righe di dati (escludendo l'header) di un CSV bytes.

    Usa csv.reader per gestire correttamente i quoted newline (valori che
    contengono \\n dentro le virgolette — es. biografie, testi lunghi).
    """
    text = csv_bytes.decode("utf-8", errors="replace")
    reader = csv.reader(io.StringIO(text))
    try:
        next(reader)  # header
    except StopIteration:
        return 0
    return sum(1 for _ in reader)


def _csv_last_value(csv_bytes: bytes, key_col: str) -> str | None:
    """Ultimo valore della colonna chiave in un CSV bytes (per keyset pagination).

    Ritorna None se la colonna non esiste o il CSV è vuoto.
    """
    text = csv_bytes.decode("utf-8", errors="replace")
    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration:
        return None
    try:
        idx = header.index(key_col)
    except ValueError:
        raise DownloadError(
            f"keyset pagination: colonna '{key_col}' non trovata nell'output SPARQL "
            f"(colonne: {header})"
        )
    last = None
    for row in reader:
        if len(row) > idx and row[idx]:
            last = row[idx]
    return last
