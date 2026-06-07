"""Tests for SparqlSource — mocks HttpClient, not raw requests."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from lab_connectors.http import HttpResult

from toolkit.core.exceptions import DownloadError
from toolkit.plugins.sparql import SparqlSource, _sparql_json_to_csv

pytestmark = pytest.mark.contract


def _http_ok(status=200, text="", headers=None):
    """Build success HttpResult for sparql responses."""
    resp = MagicMock()
    resp.status_code = status
    resp.text = text
    resp.content = text.encode("utf-8")
    resp.headers = headers or {}
    return HttpResult(response=resp, err=None)


def test_sparql_fetch_csv_success():
    """CSV response is returned as bytes."""
    with patch("toolkit.plugins.sparql.HttpClient") as mock_cls:
        mock_cls.return_value.post.return_value = _http_ok(
            status=200,
            text="name,value\nfoo,123\nbar,456\n",
            headers={"Content-Type": "text/csv"},
        )
        source = SparqlSource(timeout=30)
        payload, origin = source.fetch(
            "https://example.test/sparql",
            "SELECT ?name ?value WHERE { }",
            accept_format="csv",
        )

    assert origin == "https://example.test/sparql"
    assert b"name,value" in payload
    assert b"foo,123" in payload


def test_sparql_fetch_json_success():
    """SPARQL JSON response is converted to CSV."""
    json_response = """{
  "head": { "vars": ["name", "value"] },
  "results": {
    "bindings": [
      { "name": { "type": "literal", "value": "Alice" }, "value": { "type": "literal", "value": "42" } },
      { "name": { "type": "literal", "value": "Bob" }, "value": { "type": "literal", "value": "7" } }
    ]
  }
}"""
    with patch("toolkit.plugins.sparql.HttpClient") as mock_cls:
        mock_cls.return_value.post.return_value = _http_ok(
            status=200,
            text=json_response,
            headers={"Content-Type": "application/sparql-results+json"},
        )
        source = SparqlSource()
        payload, origin = source.fetch(
            "https://example.test/sparql",
            "SELECT ?name ?value WHERE { }",
            accept_format="sparql-results+json",
        )

    assert origin == "https://example.test/sparql"
    lines = payload.decode("utf-8").splitlines()
    assert lines[0] == "name,value"
    assert "Alice,42" in payload.decode("utf-8")
    assert "Bob,7" in payload.decode("utf-8")


def test_sparql_fetch_http_error():
    """Non-200 response raises DownloadError."""
    with patch("toolkit.plugins.sparql.HttpClient") as mock_cls:
        mock_cls.return_value.post.return_value = _http_ok(
            status=500,
            text="Internal Server Error",
        )
        source = SparqlSource()
        with pytest.raises(DownloadError, match="HTTP 500"):
            source.fetch("https://example.test/sparql", "SELECT * WHERE { }")


def test_sparql_fetch_network_error():
    """Network error raises DownloadError."""
    with patch("toolkit.plugins.sparql.HttpClient") as mock_cls:
        err_result = HttpResult(response=None, err=Exception("connection refused"))
        mock_cls.return_value.post.return_value = err_result
        mock_cls.return_value.get.return_value = err_result
        source = SparqlSource()
        with pytest.raises(DownloadError, match="connection refused"):
            source.fetch("https://example.test/sparql", "SELECT * WHERE { }")


def test_sparql_fetch_empty_results():
    """Empty SPARQL results raise DownloadError."""
    with patch("toolkit.plugins.sparql.HttpClient") as mock_cls:
        mock_cls.return_value.post.return_value = _http_ok(
            status=200,
            text='{"head":{"vars":["name"]},"results":{"bindings":[]}}',
            headers={"Content-Type": "application/sparql-results+json"},
        )
        source = SparqlSource()
        with pytest.raises(DownloadError, match="no results"):
            source.fetch(
                "https://example.test/sparql",
                "SELECT ?name WHERE { }",
                accept_format="sparql-results+json",
            )


def test_sparql_fetch_invalid_json():
    """Invalid SPARQL JSON raises DownloadError."""
    with patch("toolkit.plugins.sparql.HttpClient") as mock_cls:
        mock_cls.return_value.post.return_value = _http_ok(
            status=200,
            text="not json at all",
            headers={"Content-Type": "application/sparql-results+json"},
        )
        source = SparqlSource()
        with pytest.raises(DownloadError, match="Invalid SPARQL JSON"):
            source.fetch(
                "https://example.test/sparql",
                "SELECT * WHERE { }",
                accept_format="sparql-results+json",
            )


def test_sparql_fetch_unsupported_content_type_raises():
    """Unsupported Content-Type (es. application/xml) raise DownloadError."""
    with patch("toolkit.plugins.sparql.HttpClient") as mock_cls:
        mock_cls.return_value.post.return_value = _http_ok(
            status=200,
            text="<xml>not supported</xml>",
            headers={"Content-Type": "application/xml"},
        )
        source = SparqlSource()
        with pytest.raises(DownloadError, match="Unsupported Content-Type"):
            source.fetch(
                "https://example.test/sparql",
                "SELECT * WHERE { }",
                accept_format="csv",
            )


def test_sparql_fetch_text_plain_csv_fallback():
    """text/plain con corpo CSV non deve fallire (fallback)."""
    csv_body = "col1,col2\na,1\nb,2\n"
    with patch("toolkit.plugins.sparql.HttpClient") as mock_cls:
        mock_cls.return_value.post.return_value = _http_ok(
            status=200,
            text=csv_body,
            headers={"Content-Type": "text/plain"},
        )
        source = SparqlSource()
        result, endpoint = source.fetch(
            "https://example.test/sparql",
            "SELECT * WHERE { }",
            accept_format="csv",
        )
        assert result == csv_body.encode("utf-8")
        assert endpoint == "https://example.test/sparql"


def test_sparql_fetch_text_plain_json_fallback():
    """text/plain con corpo SPARQL JSON deve convertirsi in CSV."""
    json_body = '{"head": {"vars": ["s","p"]}, "results": {"bindings": [{"s": {"value": "x"}, "p": {"value": "y"}}]}}'
    with patch("toolkit.plugins.sparql.HttpClient") as mock_cls:
        mock_cls.return_value.post.return_value = _http_ok(
            status=200,
            text=json_body,
            headers={"Content-Type": "text/plain"},
        )
        source = SparqlSource()
        result, endpoint = source.fetch(
            "https://example.test/sparql",
            "SELECT * WHERE { }",
            accept_format="csv",
        )
        assert result  # non vuoto
        assert b"s,p" in result
        assert b"x,y" in result


def test_sparql_fetch_unsupported_accept_format_raises():
    """Unsupported accept_format raises DownloadError early (no HTTP)."""
    source = SparqlSource()
    with pytest.raises(DownloadError, match="Unsupported accept_format"):
        source.fetch(
            "https://example.test/sparql",
            "SELECT * WHERE { }",
            accept_format="xml",
        )


def test_sparql_fetch_missing_endpoint_raises():
    """Missing endpoint raises DownloadError early."""
    source = SparqlSource()
    with pytest.raises(DownloadError, match="requires endpoint URL"):
        source.fetch("", "SELECT * WHERE { }")


def test_sparql_fetch_missing_query_raises():
    """Missing query raises DownloadError early."""
    source = SparqlSource()
    with pytest.raises(DownloadError, match="requires a query"):
        source.fetch("https://example.test/sparql", "")


def test_sparql_json_to_csv_binding_types():
    """URI and literal bindings are both converted to their string value."""
    json_response = """{
  "head": { "vars": ["person", "age"] },
  "results": {
    "bindings": [
      {
        "person": { "type": "uri", "value": "http://example.org/Alice" },
        "age": { "type": "literal", "value": "30", "datatype": "http://www.w3.org/2001/XMLSchema#integer" }
      }
    ]
  }
}"""
    csv_bytes = _sparql_json_to_csv(json_response)
    csv_text = csv_bytes.decode("utf-8")
    assert "person,age" in csv_text
    assert "http://example.org/Alice,30" in csv_text


def test_sparql_probe_returns_schema_and_stats():
    """Probe returns schema, stats, and warnings."""
    bindings = [
        {
            "name": {"type": "literal", "value": "Alice"},
            "value": {"type": "literal", "value": "42"},
        },
        {"name": {"type": "literal", "value": "Bob"}, "value": {"type": "literal", "value": None}},
        {
            "name": {"type": "literal", "value": "Charlie"},
            "value": {"type": "literal", "value": "7"},
        },
    ]
    source = SparqlSource(timeout=30)
    with patch.object(source, "_fetch_bindings", return_value=bindings):
        result = source.probe(
            "https://example.test/sparql",
            "SELECT ?name ?value WHERE { }",
            limit=10,
        )

    assert result["endpoint"] == "https://example.test/sparql"
    assert result["variables"] == ["name", "value"]
    assert result["row_count"] == 3
    assert result["null_counts"]["name"] == 0
    assert result["null_counts"]["value"] == 1
    assert result["distinct_counts"]["name"] == 3
    assert result["distinct_counts"]["value"] == 2
    assert len(result["sample_rows"]) == 3
    assert any("variable 'value'" in w and "null" in w for w in result["warnings"])
    assert result["query_time_ms"] >= 0


def test_sparql_probe_adds_limit_if_missing():
    """Probe appends LIMIT if not in query."""
    captured_query = None

    def _mock_fetch(endpoint, query):
        nonlocal captured_query
        captured_query = query
        return [{"x": {"type": "literal", "value": "1"}}]

    source = SparqlSource()
    with patch.object(source, "_fetch_bindings", side_effect=_mock_fetch):
        source.probe("https://example.test/sparql", "SELECT ?x WHERE { }", limit=50)

    assert captured_query is not None
    assert "LIMIT 50" in captured_query.upper()


def test_sparql_probe_preserves_existing_limit():
    """Probe does not double-LIMIT."""
    captured_query = None

    def _mock_fetch(endpoint, query):
        nonlocal captured_query
        captured_query = query
        return [{"x": {"type": "literal", "value": "1"}}]

    source = SparqlSource()
    with patch.object(source, "_fetch_bindings", side_effect=_mock_fetch):
        source.probe("https://example.test/sparql", "SELECT ?x WHERE { } LIMIT 10", limit=50)

    assert captured_query is not None
    assert captured_query.upper().count("LIMIT") == 1


def test_sparql_probe_http_error():
    """Error during probe raises DownloadError."""
    source = SparqlSource()
    with patch.object(source, "_fetch_bindings", side_effect=DownloadError("HTTP 500 from SPARQL")):
        with pytest.raises(DownloadError, match="HTTP 500"):
            source.probe("https://example.test/sparql", "SELECT * WHERE { }")


def test_sparql_probe_missing_endpoint_raises():
    """Missing endpoint raises DownloadError early."""
    source = SparqlSource()
    with pytest.raises(DownloadError, match="requires endpoint URL"):
        source.probe("", "SELECT * WHERE { }")


def test_sparql_probe_missing_query_raises():
    """Missing query raises DownloadError early."""
    source = SparqlSource()
    with pytest.raises(DownloadError, match="requires a query"):
        source.probe("https://example.test/sparql", "")


# ── Pagination (pages/step) ────────────────────────────────────────────────────


class TestSparqlPagination:
    """Multipage fetch with OFFSET: concatenazione CSV, LIMIT guard, early stop."""

    CSV_P1 = "name,value\nfoo,1\nbar,2\n"
    CSV_P2 = "name,value\nbaz,3\nqux,4\n"
    CSV_P3 = "name,value\nquux,5\n"

    def test_pages_1_default_no_pagination(self):
        """Con pages=1 (default) si fa una sola POST."""
        with patch("toolkit.plugins.sparql.HttpClient") as mock_cls:
            mock_cls.return_value.post.return_value = _http_ok(
                text=self.CSV_P1,
                headers={"Content-Type": "text/csv"},
            )
            source = SparqlSource()
            payload, _ = source.fetch(
                "https://example.test/sparql",
                "SELECT * WHERE { ?s ?p ?o } LIMIT 10",
            )
            assert mock_cls.return_value.post.call_count == 1
            assert payload == self.CSV_P1.encode()

    def test_pages_3_concatenates_without_header_duplication(self):
        """3 pagine devono concatenare i dati saltando l'header delle pagine successive."""
        call = [0]

        def side_effect(*a, **kw):
            call[0] += 1
            if call[0] == 1:
                return _http_ok(text=self.CSV_P1, headers={"Content-Type": "text/csv"})
            if call[0] == 2:
                return _http_ok(text=self.CSV_P2, headers={"Content-Type": "text/csv"})
            return _http_ok(text=self.CSV_P3, headers={"Content-Type": "text/csv"})

        with patch("toolkit.plugins.sparql.HttpClient") as mock_cls:
            mock_cls.return_value.post.side_effect = side_effect
            source = SparqlSource()
            payload, _ = source.fetch(
                "https://example.test/sparql",
                "SELECT ?s WHERE { ?s ?p ?o } LIMIT 10",
                pages=3,
                step=10,
            )
            assert mock_cls.return_value.post.call_count == 3
            # Header deve apparire una sola volta
            text = payload.decode()
            assert text.count("name,value") == 1
            # Tutti i dati devono essere presenti
            assert (
                "foo" in text
                and "bar" in text
                and "baz" in text
                and "qux" in text
                and "quux" in text
            )

    def test_pages_without_limit_injects_step(self):
        """Se la query non ha LIMIT e pages>1, deve iniettare LIMIT step."""
        with patch("toolkit.plugins.sparql.HttpClient") as mock_cls:
            mock_cls.return_value.post.return_value = _http_ok(
                text=self.CSV_P1,
                headers={"Content-Type": "text/csv"},
            )
            source = SparqlSource()
            source.fetch(
                "https://example.test/sparql",
                "SELECT * WHERE { ?s ?p ?o }",
                pages=2,
                step=100,
            )
            # La seconda chiamata deve avere LIMIT 100 + OFFSET 100
            second_call = mock_cls.return_value.post.call_args_list[1]
            query_body = second_call.args[1].get("query", "")
            assert "LIMIT 100" in query_body
            assert "OFFSET 100" in query_body

    def test_pages_stops_when_page_empty(self):
        """Se una pagina restituisce 0 righe, ci si ferma prima di pages totali."""
        call = [0]

        def side_effect(*a, **kw):
            call[0] += 1
            if call[0] == 1:
                return _http_ok(text=self.CSV_P1, headers={"Content-Type": "text/csv"})
            # Pagine successive vuote (solo header)
            return _http_ok(text="name,value\n", headers={"Content-Type": "text/csv"})

        with patch("toolkit.plugins.sparql.HttpClient") as mock_cls:
            mock_cls.return_value.post.side_effect = side_effect
            source = SparqlSource()
            payload, _ = source.fetch(
                "https://example.test/sparql",
                "SELECT ?s WHERE { ?s ?p ?o } LIMIT 10",
                pages=5,
                step=10,
            )
            # Pagina 0 ok, pagina 1 vuota → stop (2 chiamate)
            assert mock_cls.return_value.post.call_count == 2

    def test_pages_early_stop_on_http_error(self):
        """Se una pagina successiva da HTTP error, ci si ferma senza crash."""
        from toolkit.core.exceptions import DownloadError

        call = [0]

        def side_effect(*a, **kw):
            call[0] += 1
            if call[0] == 1:
                return _http_ok(text=self.CSV_P1, headers={"Content-Type": "text/csv"})
            raise DownloadError("endpoint returned HTTP 500 for page 2")

        with patch("toolkit.plugins.sparql.HttpClient") as mock_cls:
            mock_cls.return_value.post.side_effect = side_effect
            source = SparqlSource()
            payload, _ = source.fetch(
                "https://example.test/sparql",
                "SELECT ?s WHERE { ?s ?p ?o } LIMIT 10",
                pages=3,
                step=10,
            )
            # Prima pagina ok, seconda fallisce → stop, ritorna solo pagina 1
            assert payload.decode().count("foo") == 1
            assert payload.decode().count("baz") == 0
