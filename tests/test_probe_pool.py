"""Tests per ProbePool in toolkit/core/probe.py."""

from __future__ import annotations

import time

import pytest

from toolkit.core.probe import ProbePool, ProbeResult


class TestProbePool:
    @pytest.mark.contract
    def test_submit_returns_probe_result(self, monkeypatch) -> None:
        """submit() restituisce un Future che produce ProbeResult."""
        monkeypatch.setattr(
            "toolkit.scout.http.probe_url_headers",
            lambda url, timeout=5, client=None: {
                "status_code": 200,
                "content_type": "text/csv",
            },
        )

        pool = ProbePool(workers=2)
        future = pool.submit("https://example.test/data.csv", dataset="test_ds")
        result = future.result(timeout=5)

        assert isinstance(result, ProbeResult)
        assert result.url == "https://example.test/data.csv"
        assert result.dataset == "test_ds"
        assert result.status_code == 200
        assert result.reachable is True
        assert result.content_type == "text/csv"
        pool.close()

    @pytest.mark.contract
    def test_default_timeout_applied(self, monkeypatch) -> None:
        """Il default_timeout del pool viene passato a probe_url_headers."""
        calls = []

        def _probe(url, timeout=5, client=None):
            calls.append(timeout)
            return {"status_code": 200}

        monkeypatch.setattr("toolkit.scout.http.probe_url_headers", _probe)

        pool = ProbePool(workers=2, default_timeout=10)
        pool.submit("https://example.test/", dataset="ds").result(timeout=5)
        assert calls == [10], f"Expected timeout=10, got {calls}"
        pool.close()

    @pytest.mark.contract
    def test_wait_returns_in_order(self, monkeypatch) -> None:
        """wait() restituisce risultati in ordine di submit."""
        monkeypatch.setattr(
            "toolkit.scout.http.probe_url_headers",
            lambda url, timeout=5, client=None: {"status_code": 200},
        )

        pool = ProbePool(workers=3)
        futures = [pool.submit(f"https://src{i}.test/", dataset=f"ds{i}") for i in range(5)]
        results = pool.wait(futures, timeout=10)

        assert len(results) == 5
        for i, r in enumerate(results):
            assert r.dataset == f"ds{i}", f"Expected ds{i}, got {r.dataset}"
        pool.close()

    @pytest.mark.policy
    def test_pool_runs_concurrently(self, monkeypatch) -> None:
        """Pool esegue probe in parallelo (tempo < somma sequenziale)."""
        DELAY = 0.4

        def _slow_probe(url, timeout=5, client=None):
            time.sleep(DELAY)
            return {"status_code": 200}

        monkeypatch.setattr("toolkit.scout.http.probe_url_headers", _slow_probe)

        pool = ProbePool(workers=3)
        futures = [pool.submit(f"https://src{i}.test/") for i in range(3)]

        start = time.perf_counter()
        pool.wait(futures, timeout=10)
        elapsed = time.perf_counter() - start

        # Sequenziale: 3 * 0.4 = 1.2s. Con 3 workers: ~0.4s
        assert elapsed < 0.9, f"Pool troppo lento ({elapsed:.2f}s) — atteso < 0.9s per 3 probe"
        pool.close()

    @pytest.mark.smoke
    @pytest.mark.policy
    def test_circuit_breaker_integration(self, monkeypatch) -> None:
        """Con circuit_threshold>0, errori consecutivi aprono il circuito.

        Mokka requests.head per simulare errori HTTP 502.
        probe_url_headers e HttpClient restano reali — e il circuito
        si apre dopo circuit_threshold errori consecutivi.
        """
        call_count = 0

        def _fake_fail(url, **kw):
            nonlocal call_count
            call_count += 1
            from requests import Response

            resp = Response()
            resp.status_code = 502
            resp._content = b""
            resp.url = url
            return resp

        monkeypatch.setattr("requests.head", _fake_fail)
        monkeypatch.setattr("requests.get", _fake_fail)
        monkeypatch.setattr("requests.Session.head", lambda self, url, **kw: _fake_fail(url, **kw))
        monkeypatch.setattr("requests.Session.get", lambda self, url, **kw: _fake_fail(url, **kw))

        pool = ProbePool(workers=2, circuit_threshold=3, default_timeout=5)

        futures = []
        for i in range(5):
            f = pool.submit("https://host-502.test/api", dataset="ds")
            futures.append(f)

        results = pool.wait(futures, timeout=30)

        circuit_open_count = sum(1 for r in results if r.circuit_open)
        assert circuit_open_count >= 1, "Nessuna probe ha attivato il circuit breaker"
        pool.close()

    @pytest.mark.contract
    def test_as_completed_yields_all_results(self, monkeypatch) -> None:
        """as_completed() produce tutti i risultati prima o poi."""
        monkeypatch.setattr(
            "toolkit.scout.http.probe_url_headers",
            lambda url, timeout=5, client=None: {"status_code": 200},
        )

        pool = ProbePool(workers=3)
        futures = [pool.submit(f"https://src{i}.test/") for i in range(4)]
        seen = set()
        for r in pool.as_completed(futures, timeout=10):
            seen.add(r.url)
        assert len(seen) == 4
        pool.close()

    @pytest.mark.contract
    def test_context_manager(self, monkeypatch) -> None:
        """ProbePool funziona come context manager."""
        monkeypatch.setattr(
            "toolkit.scout.http.probe_url_headers",
            lambda url, timeout=5, client=None: {"status_code": 200},
        )

        with ProbePool(workers=2) as pool:
            f = pool.submit("https://test.test/")
            r = f.result(timeout=5)
            assert r.reachable

    @pytest.mark.policy
    def test_default_timeout_resolver(self, monkeypatch) -> None:
        """Senza get_timeout, usa default_timeout."""
        calls = []

        def _probe(url, timeout=5, client=None):
            calls.append(timeout)
            return {"status_code": 200}

        monkeypatch.setattr("toolkit.scout.http.probe_url_headers", _probe)

        pool = ProbePool(workers=2, default_timeout=10)
        pool.submit("https://ex.test/").result(timeout=5)
        assert calls == [10], f"Expected default timeout=10, got {calls}"
        pool.close()
