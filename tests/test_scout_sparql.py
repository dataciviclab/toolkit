"""Tests per toolkit.scout.sparql — fetch_sparql_count e altri."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from toolkit.scout.sparql import fetch_sparql_count

pytestmark = pytest.mark.contract


def _fake_bindings(count: int) -> list[dict]:
    """Crea bindings finti per execute_sparql."""
    return [{"c": {"type": "xsd:integer", "value": str(count)}}]


class TestFetchSparqlCount:
    """Contract: fetch_sparql_count torna conteggio o None."""

    def test_endpoint_responds_with_count(self):
        """Endpoint raggiungibile → triple count."""
        with patch(
            "toolkit.scout.sparql.execute_sparql",
            return_value=_fake_bindings(42),
        ):
            result = fetch_sparql_count("https://example.test/sparql")
        assert result == 42

    def test_with_graph_uri(self):
        """Graph URI passato → query GRAPH inclusa."""
        captured_query = None

        def _mock_execute(endpoint, query, timeout=15):
            nonlocal captured_query
            captured_query = query
            return _fake_bindings(100)

        with patch(
            "toolkit.scout.sparql.execute_sparql",
            side_effect=_mock_execute,
        ):
            result = fetch_sparql_count(
                "https://example.test/sparql",
                graph_uri="http://example.org/graph/1",
            )

        assert result == 100
        assert captured_query is not None
        assert "GRAPH <http://example.org/graph/1>" in captured_query

    def test_no_bindings_returns_zero(self):
        """Nessun binding → 0 (endpoint funziona ma nessuna tripla)."""
        with patch(
            "toolkit.scout.sparql.execute_sparql",
            return_value=[],
        ):
            result = fetch_sparql_count("https://example.test/sparql")
        assert result == 0

    def test_endpoint_unreachable_returns_none(self):
        """Endpoint irraggiungibile → None."""
        with patch(
            "toolkit.scout.sparql.execute_sparql",
            side_effect=RuntimeError("connection refused"),
        ):
            result = fetch_sparql_count("https://example.test/sparql")
        assert result is None

    def test_timeout_passed_through(self):
        """timeout propagato a execute_sparql."""
        captured_timeout = None

        def _mock_execute(endpoint, query, timeout=15):
            nonlocal captured_timeout
            captured_timeout = timeout
            return _fake_bindings(1)

        with patch(
            "toolkit.scout.sparql.execute_sparql",
            side_effect=_mock_execute,
        ):
            fetch_sparql_count(
                "https://example.test/sparql",
                timeout=30,
            )
        assert captured_timeout == 30

    def test_zero_on_empty_bindings_with_value_none(self):
        """Binding con value=None → 0, non crash."""
        with patch(
            "toolkit.scout.sparql.execute_sparql",
            return_value=[{"c": {"type": "xsd:integer", "value": None}}],
        ):
            result = fetch_sparql_count("https://example.test/sparql")
        assert result == 0

    def test_value_error_caught(self):
        """ValueError viene catturato e ritorna None."""
        with patch(
            "toolkit.scout.sparql.execute_sparql",
            side_effect=ValueError("bad query"),
        ):
            result = fetch_sparql_count("https://example.test/sparql")
        assert result is None
