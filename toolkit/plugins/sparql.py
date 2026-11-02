"""SPARQL source plugin.

Fetches tabular data from a SPARQL endpoint via HTTP (POST + GET fallback).
Supports direct CSV responses and SPARQL Results JSON (converted to CSV).
"""

from __future__ import annotations

import csv
import io
import json
import logging
import re
import time
import urllib.parse
from typing import Any

from lab_connectors.http import HttpClient
from lab_connectors.http.sparql import curl_sparql as _curl_sparql

from toolkit.core.exceptions import DownloadError

log = logging.getLogger(__name__)

# User-Agent usato dal plugin: gli endpoint con WAF (es. dati.camera.it)
# bloccano gli UA "da libreria" (`DataCivicLab-HttpClient/0.1`). È lo stesso
# UA con cui gli script di estrazione funzionavano da anni.
_SPARQL_USER_AGENT = "Mozilla/5.0 DataCivicLab/1.0"

# Placeholder sostituito dal FILTER di keyset. Serve per le query in cui il
# filtro deve stare in una sottoquery dedicata: su Virtuoso un FILTER nella
# stessa WHERE di un OPTIONAL può essere ignorato dall'ottimizzatore.
_KEYSET_PLACEHOLDER = "{keyset}"

# IRI SPARQL (`<http://...>`): evita di confondere le graffe dentro un IRI
# con quelle di un group graph pattern.
_RE_IRI = re.compile(r"<[A-Za-z][A-Za-z0-9+.\-]*:[^<>{}()\s]*>")

# Keyword WHERE isolata (non `?whereVar`).
_RE_WORD_WHERE = re.compile(r"\bWHERE\b", re.IGNORECASE)


class SparqlSource:
    """Query a SPARQL endpoint and return results as CSV bytes.

    Tenta POST form-encoded (standard SPARQL protocol).
    Se POST fallisce (403, timeout), prova GET URL-encoded (Virtuoso, WAF).
    """

    def __init__(self, timeout: int = 60):
        self._client = HttpClient(timeout=timeout, user_agent=_SPARQL_USER_AGENT)
        # Retry dedicati per le pagine successive (la prima ha retries in _do_fetch).
        # Un errore su una pagina NON è "fine dati": va ritentato e, se persiste,
        # propagato per far fallire il run invece di troncare silenziosamente.
        # Lo stesso vale per una pagina VUOTA: i WAF sotto rate-limit servono
        # 200 vuoti, che non vanno confusi con la fine reale della paginazione.
        self._page_retries = 3
        self._page_retry_delay = 5.0
        # Ultima spiaggia per WAF che bloccano il TLS fingerprint delle
        # librerie Python (es. dati.senato.it): curl passa.
        self._curl_fallback = True

    def _do_fetch(self, endpoint: str, q: str, accept_format: str) -> bytes:
        """Esegue una singola query SPARQL, restituisce CSV bytes.

        Tenta POST → GET fallback → curl (WAF con TLS fingerprinting).
        Supporta CSV diretto e SPARQL Results JSON (convertito a CSV).
        """
        is_json = accept_format == "sparql-results+json"
        headers: dict[str, str] = {
            "Accept": (
                "application/sparql-results+json" if is_json else "text/csv,text/plain;q=0.5"
            ),
        }
        broad_accept = (
            "application/sparql-results+xml,"
            "application/sparql-results+json,application/json,text/csv"
        )
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
            try:
                return self._parse_response(result.response, is_json)
            except DownloadError:
                # 200 con corpo HTML = pagina WAF, non un risultato SPARQL
                if not _is_html_response(result.response):
                    raise
                log.warning("SPARQL POST 200 ma corpo HTML su %s — probabile WAF", endpoint)

        # --- Tentativo 2: GET fallback (4xx o errore di rete/timeout) ---
        if post_status is None or 400 <= post_status < 500:
            url = f"{endpoint}?query={urllib.parse.quote(q)}"
            get_headers = {"Accept": broad_accept}
            result = self._client.get(url, headers=get_headers)
            get_status = (
                result.response.status_code
                if result.is_ok and result.response is not None
                else None
            )
            if get_status is not None and get_status < 400:
                try:
                    return self._parse_response(result.response, is_json)
                except DownloadError:
                    if not _is_html_response(result.response):
                        raise
                    log.warning("SPARQL GET 200 ma corpo HTML su %s — probabile WAF", endpoint)
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

        # --- Tentativo 3: curl (WAF con TLS fingerprinting: es. dati.senato.it) ---
        # Le librerie Python vengono bloccate prima della risposta HTTP; curl
        # passa. Usato solo come ultima spiaggia dopo POST e GET.
        if self._curl_fallback:
            timeout = _timeout_seconds(self._client)
            last_body_error: DownloadError | None = None
            for method in ("POST", "GET"):
                out = _curl_sparql(endpoint, q, broad_accept, timeout, method=method)
                if out is None:
                    continue
                status, payload, content_type = out
                if status != 200:
                    log.debug("SPARQL curl %s → HTTP %s su %s", method, status, endpoint)
                    continue
                try:
                    result_bytes = self._parse_body(payload, content_type, is_json)
                except DownloadError as exc:
                    # 200 con corpo non valido (HTML/WAF o vuoto): prova l'altro
                    # trasporto invece di arrendersi al primo tentativo.
                    last_body_error = exc
                    log.warning(
                        "SPARQL curl %s: corpo non valido su %s — %s", method, endpoint, exc
                    )
                    continue
                log.info("SPARQL curl fallback (%s) OK su %s", method, endpoint)
                return result_bytes

            if last_body_error is not None:
                raise last_body_error

        raise DownloadError(
            f"SPARQL request failed for {endpoint}: POST → {result.err or 'unknown'}"
        )

    def _parse_response(self, r: Any, prefer_json: bool) -> bytes:
        """Parsa la risposta HTTP in CSV bytes."""
        if r.status_code != 200:
            raise DownloadError(
                f"SPARQL endpoint returned HTTP {r.status_code} for {r.url}: {r.text[:200]}"
            )
        return self._parse_body(r.content, r.headers.get("Content-Type") or "", prefer_json)

    def _parse_body(self, body: bytes, content_type: str, prefer_json: bool) -> bytes:
        """Parsa un corpo SPARQL (bytes + content-type) in CSV bytes.

        Un body vuoto o HTML non è un risultato SPARQL: viene segnalato con un
        errore esplicito (con preview) invece di essere interpretato come JSON.
        """
        ctype = (content_type or "").lower()
        text = body.decode("utf-8", errors="replace")

        if not text.strip():
            raise DownloadError(
                "SPARQL endpoint returned an empty body (HTTP 200) — "
                "probabile throttle/WAF, non un risultato valido"
            )

        # CSV diretto
        if "text/csv" in ctype:
            return body

        # XML SPARQL Results — parse via lab-connectors (content-type prima di prefer_json)
        if "sparql-results+xml" in ctype:
            from lab_connectors.http.sparql import _parse_sparql_xml

            return _bindings_to_csv(_parse_sparql_xml(text))

        # Pagina HTML (WAF/challenge o errore Virtuoso): non è JSON, non va
        # forzata nel parser solo perché la query chiedeva JSON.
        if "html" in ctype:
            raise DownloadError(
                f"SPARQL endpoint returned HTML instead of results (probabile WAF): {text[:200]!r}"
            )

        # SPARQL Results JSON (content-type esplicito)
        if "sparql-results+json" in ctype or "json" in ctype:
            return _sparql_json_to_csv(text)

        # Content-Type ambiguo (assente o text/plain): qui `prefer_json` decide.
        if not ctype or "text/plain" in ctype:
            if prefer_json or text.lstrip().startswith("{"):
                return _sparql_json_to_csv(text)
            # Assume CSV — se non è CSV, fallirà in CLEAN con errore chiaro
            return body

        raise DownloadError(
            f"Unsupported Content-Type '{ctype}' for SPARQL fetch. "
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
          ``FILTER(?var > <ultimo_valore>)`` dentro la WHERE — deterministico
          e compatibile con i limiti Virtuoso. ``pages`` funge da tetto di
          sicurezza (se > 1); la chiave funziona al meglio se URI, ma il
          FILTER genera il letterale corretto anche per numeri e stringhe.

          Opzioni aggiuntive del blocco keyset:
          - ``key_expr``: espressione usata nel FILTER/ORDER BY quando il
            binding non è la variabile diretta (es. ``"STR(?atto)"``).
            Default: ``key``.
          - ``key_column``: colonna CSV da cui leggere l'ultimo valore.
            Default: ``key`` senza ``?``.
          - ``key_literal``: ``auto`` (default), ``uri``, ``string`` o
            ``number``. Serve quando ``key_expr`` è ``STR(?var)``: il valore
            va citato come stringa, non come IRI.

          Se la query contiene il placeholder ``{keyset}``, il FILTER viene
          sostituito lì (pagina 1: stringa vuota). È il modo per isolare il
          filtro in una sottoquery: su Virtuoso un FILTER nella stessa WHERE
          di un OPTIONAL può essere ignorato dall'ottimizzatore. Senza
          placeholder il FILTER viene inserito nella WHERE principale.

          ``step`` deve coincidere con il ``LIMIT`` effettivo della query (o
          essere più grande): altrimenti la fine dati viene rilevata solo da
          pagina vuota e il tetto ``pages`` può troncare il risultato.

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
        keyset_expr = (pagination or {}).get("key_expr") or keyset_key
        keyset_column = (pagination or {}).get("key_column")
        keyset_literal = (pagination or {}).get("key_literal", "auto")
        if pagination_mode == "keyset" and not keyset_key:
            raise DownloadError(
                'pagination mode=keyset requires \'key\' (es. {"mode": "keyset", "key": "?deputato"})'
            )
        assert keyset_key is None or isinstance(keyset_key, str)
        if keyset_literal not in {"auto", "uri", "string", "number"}:
            raise DownloadError(
                f"Unsupported key_literal '{keyset_literal}'. "
                "Supported values: auto, uri, string, number."
            )

        # La prima pagina usa la query senza placeholder; le successive
        # ripartono dal template per sostituire {keyset} con il FILTER.
        keyset_template = query

        # Se e' richiesta paginazione, assicura che la query abbia LIMIT
        if pages > 1 or pagination_mode == "keyset":
            if _KEYSET_PLACEHOLDER in query:
                query = query.replace(_KEYSET_PLACEHOLDER, "")
            if "limit" not in query.lower():
                query = f"{query.rstrip().rstrip(';')} LIMIT {step}"

        # Prima pagina — con retry+backoff dedicati: su endpoint instabili
        # (es. Camera 503/500 intermittenti) la prima query pesante riceve
        # spesso il 503 iniziale; _do_fetch ha retries ma senza delay sufficiente.
        # In paginazione (OFFSET con pages>1 o keyset) il 200 vuoto sulla prima
        # pagina è un sintomo di throttle WAF: va ritentato come le pagine 2+.
        # Con pages==1 (query singola) un risultato vuoto è un errore reale:
        # fallisce subito, non va mascherato.
        if pages > 1 or pagination_mode == "keyset":
            all_bytes = self._fetch_page_resilient(endpoint, query, accept_format)
        else:
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
            assert keyset_key is not None and isinstance(keyset_key, str)
            all_bytes = self._fetch_keyset_pages(
                endpoint,
                keyset_template,
                accept_format,
                keyset_key,
                step,
                all_bytes,
                max_pages=pages,
                key_expr=keyset_expr,
                key_column=keyset_column,
                key_literal=keyset_literal,
            )
            return all_bytes, endpoint

        # Paginazione OFFSET: pagine successive, concatena CSV (saltando l'header)
        for page in range(1, pages):
            q = f"{query.rstrip().rstrip(';')} OFFSET {page * step}"

            page_bytes = self._fetch_page_resilient(endpoint, q, accept_format)

            # Se la pagina e' vuota (solo header, nessun dato) anche dopo i
            # retry anti-throttle → fine reale della paginazione.
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
                    # Backoff esponenziale: un WAF che serve HTML/vuoto dopo un
                    # burst ha bisogno di respiro, non di un retry immediato.
                    time.sleep(self._page_retry_delay * (2**attempt))
        raise last_error  # type: ignore[misc]

    def _fetch_page_resilient(
        self,
        endpoint: str,
        q: str,
        accept_format: str,
    ) -> bytes:
        """Fetch di una pagina con retry su pagina VUOTA (throttle WAF).

        I WAF sotto rate-limit rispondono 200 vuoti (header-only CSV o
        "no results" JSON) che NON vanno confusi con la fine reale della
        paginazione. Ritenta con backoff prima di dichiarare la fine.
        Restituisce bytes, possibilmente vuoti (= fine reale dopo i retry).
        """
        page_bytes = self._fetch_page_with_retry(endpoint, q, accept_format)
        for attempt in range(self._page_retries):
            if not self._is_empty_page(page_bytes):
                return page_bytes
            wait = self._page_retry_delay * (2**attempt)
            log.warning(
                "SPARQL pagina vuota su %s — probabile throttle, retry in %ds",
                endpoint,
                wait,
            )
            time.sleep(wait)
            page_bytes = self._fetch_page_with_retry(endpoint, q, accept_format)
        return page_bytes

    @staticmethod
    def _is_empty_page(page_bytes: bytes) -> bool:
        """True se la pagina è vuota (solo header CSV, nessun dato)."""
        data_start = page_bytes.find(b"\n")
        return data_start < 0 or len(page_bytes) <= data_start + 1

    def _fetch_keyset_pages(
        self,
        endpoint: str,
        query: str,
        accept_format: str,
        key: str,
        step: int,
        all_bytes: bytes,
        max_pages: int = 1,
        key_expr: str | None = None,
        key_column: str | None = None,
        key_literal: str = "auto",
    ) -> bytes:
        """Paginazione keyset: inietta FILTER(?key > <last>) nelle pagine successive.

        Deterministico e compatibile con endpoint Virtuoso che rifiutano
        ORDER BY + OFFSET (SR353) — es. dati.camera.it.

        ``max_pages`` è il tetto di sicurezza (il contratto ``pages``): se <= 1
        (default, nessun limite esplicito) si usa un cap alto per evitare loop
        infiniti se la chiave non è monotona o l'endpoint degrada il FILTER.

        Il FILTER va dentro la WHERE (o al posto di ``{keyset}``): appenderlo
        dopo la graffa di chiusura produce una query che Virtuoso rifiuta.
        """
        if not key.startswith("?"):
            key = f"?{key}"
        expr = key_expr or key
        column = key_column or key.lstrip("?")
        # normalizza: niente ';' finale per poter concatenare la clausola
        base_query = query.rstrip().rstrip(";")
        has_placeholder = _KEYSET_PLACEHOLDER in base_query
        page_n = 0
        safety_max = max_pages if max_pages > 1 else 1000

        while True:
            page_n += 1
            if page_n > safety_max:
                raise DownloadError(
                    "SPARQL keyset pagination exceeded max pages "
                    f"({safety_max}) — key non monotona o FILTER degradato?"
                )
            last_value = _csv_last_value(all_bytes, column)
            if last_value is None:
                break
            filter_clause = f"FILTER({expr} > {_keyset_literal(last_value, key_literal)})"
            if has_placeholder:
                q = base_query.replace(_KEYSET_PLACEHOLDER, filter_clause)
            else:
                q = _inject_filter_into_where(base_query, filter_clause)

            page_bytes = self._fetch_page_resilient(endpoint, q, accept_format)

            data_start = page_bytes.find(b"\n")
            if data_start < 0 or len(page_bytes) <= data_start + 1:
                break  # pagina vuota dopo i retry anti-throttle = fine dati
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


def _timeout_seconds(client: Any) -> int:
    """Timeout numerico dall'HttpClient (che può usare una tupla)."""
    t = getattr(client, "timeout", 60)
    return int(t[0] if isinstance(t, tuple) else t)


def _is_html_response(resp: Any) -> bool:
    """True se la risposta è una pagina HTML (tipica dei WAF), non SPARQL."""
    ctype = (resp.headers.get("Content-Type") or "").lower()
    if "html" in ctype:
        return True
    body = (resp.text or "").lstrip()[:200].lower()
    return body.startswith("<!doctype html") or body.startswith("<html")


def _skip_quoted(query: str, i: int) -> int:
    """Salta una stringa SPARQL (singola, doppia, o triple-quoted).

    ``i`` punta al primo carattere di quote; ritorna l'indice dopo la chiusura.
    Gestisce gli escape con backslash. Se la stringa non è chiusa, ritorna la
    fine della query.
    """
    quote = query[i]
    delim = quote * 3 if query.startswith(quote * 3, i) else quote
    i += len(delim)
    n = len(query)
    while i < n:
        if query[i] == "\\":
            i += 2
            continue
        if query.startswith(delim, i):
            return i + len(delim)
        i += 1
    return n


def _find_main_where_open(query: str) -> int | None:
    """Indice della ``{`` che apre la WHERE principale, o None.

    Scansiona ignorando stringhe, IRI e commenti. Le WHERE annidate
    (sottoquery) sono escluse: interessa solo il group graph pattern esterno.
    Se la query omette la keyword ``WHERE`` (forma abbreviata SPARQL valida),
    usa la prima graffa di primo livello.
    """
    i = 0
    n = len(query)
    depth = 0
    first_open: int | None = None
    while i < n:
        ch = query[i]
        if ch in "\"'":
            i = _skip_quoted(query, i)
            continue
        if ch == "<":
            m = _RE_IRI.match(query, i)
            if m:
                i = m.end()
                continue
        if ch == "#":
            nl = query.find("\n", i)
            i = n if nl < 0 else nl + 1
            continue
        if ch == "{":
            if depth == 0 and first_open is None:
                first_open = i
            depth += 1
            i += 1
            continue
        if ch == "}":
            depth -= 1
            i += 1
            continue
        if depth == 0:
            m = _RE_WORD_WHERE.match(query, i)
            if m:
                j = m.end()
                while j < n:
                    c = query[j]
                    if c in " \t\r\n":
                        j += 1
                        continue
                    if c == "#":
                        nl = query.find("\n", j)
                        j = n if nl < 0 else nl + 1
                        continue
                    break
                if j < n and query[j] == "{":
                    return j
                i = m.end()
                continue
        i += 1
    return first_open


def _match_brace(query: str, open_idx: int) -> int | None:
    """Indice della ``}`` che chiude la graffa aperta in ``open_idx``."""
    depth = 0
    i = open_idx
    n = len(query)
    while i < n:
        ch = query[i]
        if ch in "\"'":
            i = _skip_quoted(query, i)
            continue
        if ch == "<":
            m = _RE_IRI.match(query, i)
            if m:
                i = m.end()
                continue
        if ch == "#":
            nl = query.find("\n", i)
            i = n if nl < 0 else nl + 1
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return None


def _inject_filter_into_where(query: str, filter_clause: str) -> str:
    """Inserisce ``filter_clause`` dentro la WHERE principale.

    ``FILTER`` deve stare dentro un group graph pattern: appenderlo dopo la
    ``}`` di chiusura (es. prima di ``ORDER BY``) produce una query non valida
    che Virtuoso rifiuta. È la causa per cui i dataset keyset finivano in
    script dedicati.
    """
    open_idx = _find_main_where_open(query)
    if open_idx is None:
        raise DownloadError(
            "keyset pagination: query senza WHERE. Usa una SELECT con WHERE "
            f"oppure il placeholder {_KEYSET_PLACEHOLDER}."
        )
    close_idx = _match_brace(query, open_idx)
    if close_idx is None:
        raise DownloadError("keyset pagination: parentesi graffe non bilanciate nella query")
    return f"{query[:close_idx]}\n  {filter_clause}\n{query[close_idx:]}"


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
    return _bindings_to_csv(bindings, var_names)


def _bindings_to_csv(bindings: list[dict[str, Any]], var_names: list[str] | None = None) -> bytes:
    """Convert SPARQL bindings (JSON or XML format) to CSV bytes.

    Each binding is ``{var: {"type": str, "value": str}}``.
    If *var_names* is not provided, columns are derived from the first binding.
    """
    if not bindings:
        raise DownloadError("SPARQL query returned no results")

    if var_names is None:
        var_names = list(bindings[0].keys())

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


def _keyset_literal(value: str, mode: str = "auto") -> str:
    """Rappresenta un valore come letterale SPARQL per il FILTER keyset.

    ``mode``:
    - ``auto`` (default): URI (http/https) → ``<uri>``, numero → nudo,
      altrimenti stringa quotata.
    - ``uri``: sempre ``<uri>``.
    - ``number``: sempre nudo (errore se non numerico).
    - ``string``: sempre quotato — necessario quando il FILTER usa
      ``STR(?var)``: il valore è una stringa, non un IRI.
    """
    v = value.strip()
    if mode == "uri":
        return f"<{v}>"
    if mode == "number":
        if not re.fullmatch(r"-?\d+(\.\d+)?", v):
            raise DownloadError(
                f"keyset key_literal='number' ma l'ultimo valore non è numerico: {v!r}"
            )
        return v
    if mode == "string":
        return _quote_literal(v)
    if v.startswith(("http://", "https://")):
        return f"<{v}>"
    if re.fullmatch(r"-?\d+(\.\d+)?", v):
        return v
    return _quote_literal(v)


def _quote_literal(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'
