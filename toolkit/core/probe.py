"""ProbePool — orchestrazione probe HTTP parallele per il toolkit.

Strati:
  lab_connectors/http/client.py  →  HttpClient (con circuit breaker opzionale)
  toolkit/scout/http.py          →  probe_url_headers (probe con format detection)
  toolkit/core/probe.py          →  ProbePool (orchestrazione parallela)

Uso::

    from toolkit.core.probe import ProbePool

    pool = ProbePool(workers=8, circuit_threshold=3)
    futures = []
    for url in urls:
        futures.append(pool.submit(url, dataset="mio_dataset"))

    for result in pool.as_completed(futures, timeout=120):
        print(result.dataset, result.status_code, result.reachable)
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Callable

from lab_connectors.http import CircuitOpenError, HttpClient

logger = logging.getLogger("toolkit.core.probe")


@dataclass
class ProbeResult:
    """Risultato di una singola probe.

    Attributes:
        url: URL sottoposto a probe.
        dataset: Nome del dataset associato (per report).
        status_code: HTTP status code (0 se irraggiungibile).
        reachable: True se la fonte e' raggiungibile (HTTP < 400).
        content_type: Content-Type rilevato (se disponibile).
        error: Messaggio di errore (se irraggiungibile o circuit aperto).
        duration_seconds: Durata della probe in secondi.
        circuit_open: True se la probe e' stata saltata per circuit breaker.

    """

    url: str
    dataset: str = ""
    status_code: int = 0
    reachable: bool = False
    content_type: str | None = None
    error: str | None = None
    duration_seconds: float = 0.0
    circuit_open: bool = False


class ProbePool:
    """Pool di probe HTTP parallele con circuit breaker.

    Combina un ``ThreadPoolExecutor`` con ``HttpClient`` (circuit breaker
    opzionale) e ``probe_url_headers`` (format detection).  Ogni probe
    viene eseguita in un thread separato.

    Args:
        workers: Numero massimo di worker thread (default 8).
        circuit_threshold: Soglia circuit breaker per-host.
            0 = disabilitato (default).
        default_timeout: Timeout HTTP predefinito in secondi (default 5).
        client: ``HttpClient`` opzionale.  Se non fornito, ne crea uno
            con ``circuit_threshold`` e ``default_timeout``.
        get_timeout: Callable opzionale ``(url, dataset) -> int`` per
            timeout personalizzato per fonte (da config dataset.yml).
            Default: restituisce ``default_timeout``.

    Note:
        Il client HTTP è condiviso tra tutte le probe (per circuit breaker).
        Se ``get_timeout`` restituisce un timeout diverso da ``default_timeout``,
        la probe usa lo stesso client (quindi lo stesso timeout di connessione),
        ma la chiamata a ``probe_url_headers`` non riceve override esplicito.
        Per timeout diversi, crea un ``ProbePool`` separato.

    """

    def __init__(
        self,
        workers: int = 8,
        circuit_threshold: int = 0,
        default_timeout: int = 5,
        client: HttpClient | None = None,
        get_timeout: Callable[[str, str], int] | None = None,
    ) -> None:
        self._default_timeout = default_timeout
        self._get_timeout = get_timeout or (lambda url, ds: default_timeout)
        self._lock = threading.Lock()
        self._client = client
        self._owns_client = client is None
        self._circuit_threshold = circuit_threshold
        self._pool = ThreadPoolExecutor(max_workers=workers)

    def _get_client(self, timeout: int) -> HttpClient:
        """Restituisce (e crea se necessario) il client HTTP.

        Thread-safe: la creazione del client è protetta da lock.
        """
        if self._client is not None:
            return self._client
        with self._lock:
            if self._client is None:
                self._client = HttpClient(
                    timeout=timeout,
                    circuit_threshold=self._circuit_threshold,
                )
        return self._client

    def submit(
        self,
        url: str,
        *,
        dataset: str = "",
        timeout: int | None = None,
    ) -> Future:
        """Invia una probe HTTP al pool.

        Args:
            url: URL da probe.
            dataset: Nome del dataset (per report).
            timeout: Timeout HTTP in secondi.  Se None, usa
                ``get_timeout(url, dataset)`` o ``default_timeout``.

        Returns:
            ``concurrent.futures.Future`` il cui ``.result()`` restituisce
            un ``ProbeResult``.

        """
        from toolkit.scout.http import probe_url_headers

        effective_timeout = timeout if timeout is not None else self._get_timeout(url, dataset)

        def _run() -> ProbeResult:
            start = time.perf_counter()
            try:
                client = self._get_client(effective_timeout)
                probe = probe_url_headers(url, timeout=effective_timeout, client=client)
                elapsed = time.perf_counter() - start
                sc = probe.get("status_code", 0)
                reachable = 200 <= sc < 400
                return ProbeResult(
                    url=url,
                    dataset=dataset,
                    status_code=sc,
                    reachable=reachable,
                    content_type=probe.get("content_type"),
                    duration_seconds=elapsed,
                )
            except CircuitOpenError as exc:
                elapsed = time.perf_counter() - start
                return ProbeResult(
                    url=url,
                    dataset=dataset,
                    status_code=0,
                    reachable=False,
                    error=str(exc),
                    duration_seconds=elapsed,
                    circuit_open=True,
                )
            except RuntimeError as exc:
                elapsed = time.perf_counter() - start
                return ProbeResult(
                    url=url,
                    dataset=dataset,
                    status_code=0,
                    reachable=False,
                    error=str(exc),
                    duration_seconds=elapsed,
                )

        return self._pool.submit(_run)

    def as_completed(
        self,
        futures: list[Future],
        timeout: float | None = None,
    ):
        """Itera sui risultati nell'ordine di completamento.

        Args:
            futures: Lista di Future da ``submit()``.
            timeout: Timeout globale in secondi (None = nessun limite).

        Yields:
            ``ProbeResult`` man mano che le probe completano.

        """
        for future in as_completed(futures, timeout=timeout):
            yield future.result()

    def wait(self, futures: list[Future], timeout: float | None = None) -> list[ProbeResult]:
        """Attende tutte le probe e restituisce i risultati in ordine di submit.

        Args:
            futures: Lista di Future da ``submit()``.
            timeout: Timeout globale in secondi.

        Returns:
            Lista di ``ProbeResult`` nello stesso ordine dei futures.

        """
        done = set()
        for f in as_completed(futures, timeout=timeout):
            done.add(f)
        return [f.result() for f in futures]

    def close(self) -> None:
        """Chiude il thread pool attendendo il completamento.

        Se il pool possiede il client (creato internamente),
        ne chiude anche la sessione HTTP.
        """
        self._pool.shutdown(wait=True)
        if self._owns_client and self._client is not None:
            self._client.close()

    def __enter__(self) -> ProbePool:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
