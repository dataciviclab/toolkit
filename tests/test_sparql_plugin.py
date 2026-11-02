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


def test_sparql_fetch_xml_success():
    """SPARQL XML response is parsed and converted to CSV."""
    xml_response = """<?xml version="1.0"?>
<sparql xmlns="http://www.w3.org/2005/sparql-results#">
 <head><variable name="name"/><variable name="value"/></head>
 <results>
  <result>
   <binding name="name"><literal>Alice</literal></binding>
   <binding name="value"><literal>42</literal></binding>
  </result>
  <result>
   <binding name="name"><literal>Bob</literal></binding>
   <binding name="value"><literal>7</literal></binding>
  </result>
 </results>
</sparql>"""
    with patch("toolkit.plugins.sparql.HttpClient") as mock_cls:
        mock_cls.return_value.post.return_value = _http_ok(
            status=200,
            text=xml_response,
            headers={"Content-Type": "application/sparql-results+xml"},
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


def test_sparql_fetch_xml_get_fallback():
    """XML response via GET fallback is parsed correctly."""
    xml_response = """<?xml version="1.0"?>
<sparql xmlns="http://www.w3.org/2005/sparql-results#">
 <head><variable name="x"/></head>
 <results>
  <result><binding name="x"><literal>1</literal></binding></result>
 </results>
</sparql>"""
    with patch("toolkit.plugins.sparql.HttpClient") as mock_cls:
        mock_cls.return_value.post.return_value = _http_ok(status=403)
        mock_cls.return_value.get.return_value = _http_ok(
            status=200,
            text=xml_response,
            headers={"Content-Type": "application/sparql-results+xml"},
        )
        source = SparqlSource()
        payload, _ = source.fetch(
            "https://example.test/sparql",
            "SELECT ?x WHERE { }",
        )

    assert "x" in payload.decode("utf-8")
    assert "1" in payload.decode("utf-8")
    """Non-200 response raises DownloadError."""
    with patch("toolkit.plugins.sparql.HttpClient") as mock_cls:
        mock_cls.return_value.post.return_value = _http_ok(
            status=500,
            text="Internal Server Error",
        )
        source = SparqlSource()
        source._page_retry_delay = 0
        with pytest.raises(DownloadError, match="HTTP 500"):
            source.fetch("https://example.test/sparql", "SELECT * WHERE { }")


def test_sparql_fetch_post_403_falls_back_to_get():
    """POST 403 (WAF/Virtuoso) deve cadere sul fallback GET.

    Regressione: il plugin considerava ok il 403 (is_ok = response presente,
    err None) e salvava il body HTML come CSV — il fallback GET non scattava.
    Caso reale: dati.senato.it accetta solo GET (POST → 403).
    """
    with patch("toolkit.plugins.sparql.HttpClient") as mock_cls:
        mock_cls.return_value.post.return_value = _http_ok(
            status=403,
            text="<!DOCTYPE html><html>Forbidden</html>",
        )
        mock_cls.return_value.get.return_value = _http_ok(
            status=200,
            text="name,value\nfoo,123\n",
            headers={"Content-Type": "text/csv"},
        )
        source = SparqlSource()
        payload, origin = source.fetch(
            "https://dati.senato.it/sparql",
            "SELECT ?name ?value WHERE { }",
            accept_format="csv",
        )
        assert b"foo" in payload
        assert mock_cls.return_value.get.called  # il fallback GET è scattato


def test_sparql_fetch_get_fallback_5xx_raises():
    """POST 403 → GET fallback 500 → DownloadError con status del GET.

    Il fallback GET non deve mascherare un errore server del GET stesso.
    """
    with patch("toolkit.plugins.sparql.HttpClient") as mock_cls:
        mock_cls.return_value.post.return_value = _http_ok(
            status=403,
            text="<!DOCTYPE html><html>Forbidden</html>",
        )
        mock_cls.return_value.get.return_value = _http_ok(
            status=500,
            text="Internal Server Error",
        )
        source = SparqlSource()
        source._page_retry_delay = 0
        with pytest.raises(DownloadError, match="GET fallback returned HTTP 500"):
            source.fetch("https://example.test/sparql", "SELECT * WHERE { }")
    """POST con errore di rete/timeout (response None) deve cadere sul GET.

    Estensione del fix 403: il docstring prometteva il fallback anche su
    timeout — il POST con err (connection refused) produce post_status=None
    e prima del fix andava dritto al raise senza provare il GET.
    """
    from lab_connectors.http import HttpResult

    with patch("toolkit.plugins.sparql.HttpClient") as mock_cls:
        mock_cls.return_value.post.return_value = HttpResult(
            response=None, err=TimeoutError("connection refused")
        )
        mock_cls.return_value.get.return_value = _http_ok(
            status=200,
            text="name,value\nfoo,123\n",
            headers={"Content-Type": "text/csv"},
        )
        source = SparqlSource()
        payload, origin = source.fetch(
            "https://dati.senato.it/sparql",
            "SELECT ?name ?value WHERE { }",
            accept_format="csv",
        )
        assert b"foo" in payload
        assert mock_cls.return_value.get.called  # il fallback GET è scattato


def test_sparql_fetch_network_error():
    """Network error raises DownloadError."""
    with patch("toolkit.plugins.sparql.HttpClient") as mock_cls:
        err_result = HttpResult(response=None, err=Exception("connection refused"))
        mock_cls.return_value.post.return_value = err_result
        mock_cls.return_value.get.return_value = err_result
        source = SparqlSource()
        source._page_retry_delay = 0
        source._curl_fallback = False  # test ermetico: niente curl reale
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
        source._page_retry_delay = 0
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
        source._page_retry_delay = 0
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
        source._page_retry_delay = 0
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
                step=2,
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
            mock_cls.return_value.post.side_effect = [
                _http_ok(text=self.CSV_P1, headers={"Content-Type": "text/csv"}),
                _http_ok(text=self.CSV_P2, headers={"Content-Type": "text/csv"}),
            ]
            source = SparqlSource()
            source.fetch(
                "https://example.test/sparql",
                "SELECT * WHERE { ?s ?p ?o }",
                pages=2,
                step=2,
            )
            # La seconda chiamata deve avere LIMIT 2 + OFFSET 2
            second_call = mock_cls.return_value.post.call_args_list[1]
            query_body = second_call.args[1].get("query", "")
            assert "LIMIT 2" in query_body
            assert "OFFSET 2" in query_body

    def test_pages_stops_when_page_empty(self):
        """Una pagina vuota viene ritentata (throttle WAF) e, se resta vuota,
        la paginazione si ferma prima di pages totali."""
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
            with patch("toolkit.plugins.sparql.time.sleep"):
                payload, _ = source.fetch(
                    "https://example.test/sparql",
                    "SELECT ?s WHERE { ?s ?p ?o } LIMIT 10",
                    pages=5,
                    step=2,
                )
            # Pagina 0 ok (2 righe = piena); pagina 1 vuota ritentata
            # (_page_retries=3, qui senza dormire) → stop. 1 + 4 = 5 chiamate.
            assert mock_cls.return_value.post.call_count == 5

    def test_pages_stops_when_page_short(self):
        """Una pagina con meno righe di step = ultima pagina: stop senza OFFSET oltre fine.

        regression: issue #449 — su endpoint come dati.camera.it l'OFFSET oltre la
        fine risponde 503 invece di 200 con 0 righe; la fine va rilevata dalla
        pagina corta, non da un errore HTTP.
        """
        call = [0]

        def side_effect(*a, **kw):
            call[0] += 1
            if call[0] == 1:
                return _http_ok(text=self.CSV_P1, headers={"Content-Type": "text/csv"})
            # Pagina 2: solo 1 riga (quux) — corta, ultima
            return _http_ok(text=self.CSV_P3, headers={"Content-Type": "text/csv"})

        with patch("toolkit.plugins.sparql.HttpClient") as mock_cls:
            mock_cls.return_value.post.side_effect = side_effect
            source = SparqlSource()
            payload, _ = source.fetch(
                "https://example.test/sparql",
                "SELECT ?s WHERE { ?s ?p ?o } LIMIT 10",
                pages=5,
                step=10,
            )
            # Pagina 0 (2 righe < 10) corta già alla prima → fermati subito
            assert mock_cls.return_value.post.call_count == 1
            assert b"foo" in payload and b"bar" in payload

    def test_pages_short_page_concatenates_then_stops(self):
        """Pagina piena (== step) concatena e continua; pagina corta concatena e ferma."""
        call = [0]

        def side_effect(*a, **kw):
            call[0] += 1
            if call[0] == 1:
                return _http_ok(text=self.CSV_P1, headers={"Content-Type": "text/csv"})
            # Pagina 2: corta (1 riga) — deve essere concatenata e poi stop
            return _http_ok(text=self.CSV_P3, headers={"Content-Type": "text/csv"})

        with patch("toolkit.plugins.sparql.HttpClient") as mock_cls:
            mock_cls.return_value.post.side_effect = side_effect
            source = SparqlSource()
            payload, _ = source.fetch(
                "https://example.test/sparql",
                "SELECT ?s WHERE { ?s ?p ?o } LIMIT 10",
                pages=3,
                step=2,
            )
            # Pagina 1 piena (2 righe = step) → pagina 2 corta (1 riga) → stop
            assert mock_cls.return_value.post.call_count == 2
            text = payload.decode()
            assert "foo" in text and "bar" in text and "quux" in text
            assert "baz" not in text  # mai fetchata

    def test_pages_http_error_after_retries_raises(self):
        """Un errore HTTP persistente su una pagina deve propagare (non troncare).

        regression: issue #449 — il vecchio comportamento faceva break silenzioso
        su DownloadError, producendo CSV troncati su endpoint instabili (es. Camera
        500/503 intermittenti). Dopo i retry, l'errore deve far fallire il run.
        """
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
            source._page_retry_delay = 0  # test veloce
            with pytest.raises(DownloadError, match="HTTP 500"):
                source.fetch(
                    "https://example.test/sparql",
                    "SELECT ?s WHERE { ?s ?p ?o } LIMIT 10",
                    pages=3,
                    step=2,
                )
            # 1 pagina ok + 3 retry falliti sulla pagina 2
            assert mock_cls.return_value.post.call_count == 4

    def test_pages_transient_error_retries_then_succeeds(self):
        """Un errore transitorio su una pagina deve essere ritentato e recuperato."""
        call = [0]

        def side_effect(*a, **kw):
            call[0] += 1
            if call[0] == 1:
                return _http_ok(text=self.CSV_P1, headers={"Content-Type": "text/csv"})
            if call[0] == 2:
                raise DownloadError("endpoint returned HTTP 503 for page 2")
            # Al retry successivo la pagina risponde
            return _http_ok(text=self.CSV_P2, headers={"Content-Type": "text/csv"})

        with patch("toolkit.plugins.sparql.HttpClient") as mock_cls:
            mock_cls.return_value.post.side_effect = side_effect
            source = SparqlSource()
            source._page_retry_delay = 0  # test veloce
            payload, _ = source.fetch(
                "https://example.test/sparql",
                "SELECT ?s WHERE { ?s ?p ?o } LIMIT 10",
                pages=2,
                step=2,
            )
            text = payload.decode()
            assert "foo" in text and "baz" in text  # pagina 2 recuperata
            # 1 (pagina 1) + 1 fallita + 1 retry riuscito
            assert mock_cls.return_value.post.call_count == 3

    # --- keyset pagination ---

    CSV_K1 = "s,value\nfoo,1\nbar,2\n"  # 2 righe (piena con step=2)
    CSV_K2 = "s,value\nbaz,3\nqux,4\n"  # 2 righe (piena con step=2)
    CSV_K3 = "s,value\nquux,5\n"  # 1 riga (corta → ultima)

    def test_keyset_injects_filter_and_concatenates(self):
        """Keyset: le pagine successive iniettano FILTER(?s > <last>) e concatena."""
        queries: list[str] = []

        def side_effect(*a, **kw):
            q = a[1].get("query", "") if len(a) > 1 and isinstance(a[1], dict) else ""
            queries.append(q)
            if len(queries) == 1:
                return _http_ok(text=self.CSV_K1, headers={"Content-Type": "text/csv"})
            if len(queries) == 2:
                return _http_ok(text=self.CSV_K2, headers={"Content-Type": "text/csv"})
            # Dopo la pagina 2 (piena) il loop continua; la 3a pagina è vuota → stop
            return _http_ok(text="s,value\n", headers={"Content-Type": "text/csv"})

        with patch("toolkit.plugins.sparql.HttpClient") as mock_cls:
            mock_cls.return_value.post.side_effect = side_effect
            source = SparqlSource()
            with patch("toolkit.plugins.sparql.time.sleep"):
                payload, _ = source.fetch(
                    "https://example.test/sparql",
                    "SELECT ?s WHERE { ?s ?p ?o } ORDER BY ?s LIMIT 10",
                    pages=1,
                    step=2,
                    pagination={"mode": "keyset", "key": "?s"},
                )
            # Pagina 2: FILTER con l'ultimo valore della colonna 's' (bar)
            assert 'FILTER(?s > "bar")' in queries[1], f"pagina 2: {queries[1]}"
            text = payload.decode()
            assert "foo" in text and "baz" in text and "qux" in text
            # 6 chiamate: K1, K2, pagina vuota ritentata 3 volte → stop
            assert len(queries) == 6

    def test_keyset_recovers_from_throttle_empty_page(self):
        """Regressione anti-troncamento: una pagina vuota (throttle WAF) nel
        keyset viene ritentata e, se il retry restituisce dati, si prosegue."""
        queries: list[str] = []

        def side_effect(*a, **kw):
            q = a[1].get("query", "") if len(a) > 1 and isinstance(a[1], dict) else ""
            queries.append(q)
            if len(queries) == 1:
                return _http_ok(text=self.CSV_K1, headers={"Content-Type": "text/csv"})
            if len(queries) == 2:
                # pagina vuota (throttle) al primo tentativo
                return _http_ok(text="s,value\n", headers={"Content-Type": "text/csv"})
            if len(queries) == 3:
                # retry → dati
                return _http_ok(text=self.CSV_K2, headers={"Content-Type": "text/csv"})
            # ultima pagina corta → fine
            return _http_ok(text=self.CSV_K3, headers={"Content-Type": "text/csv"})

        with patch("toolkit.plugins.sparql.HttpClient") as mock_cls:
            mock_cls.return_value.post.side_effect = side_effect
            source = SparqlSource()
            with patch("toolkit.plugins.sparql.time.sleep"):
                payload, _ = source.fetch(
                    "https://example.test/sparql",
                    "SELECT ?s WHERE { ?s ?p ?o } ORDER BY ?s LIMIT 10",
                    pages=1,
                    step=2,
                    pagination={"mode": "keyset", "key": "?s"},
                )
            text = payload.decode()
            # La pagina 'throttlata' è stata recuperata dal retry
            assert "baz" in text and "qux" in text
            assert "foo" in text

    def test_keyset_stops_when_short_page(self):
        """Keyset: pagina corta (< step) = ultima, stop senza ulteriori fetch."""
        call = [0]

        def side_effect(*a, **kw):
            call[0] += 1
            if call[0] == 1:
                return _http_ok(text=self.CSV_K1, headers={"Content-Type": "text/csv"})
            return _http_ok(text=self.CSV_K3, headers={"Content-Type": "text/csv"})

        with patch("toolkit.plugins.sparql.HttpClient") as mock_cls:
            mock_cls.return_value.post.side_effect = side_effect
            source = SparqlSource()
            payload, _ = source.fetch(
                "https://example.test/sparql",
                "SELECT ?s WHERE { ?s ?p ?o } ORDER BY ?s LIMIT 10",
                pagination={"mode": "keyset", "key": "?s"},
                step=2,
            )
            assert mock_cls.return_value.post.call_count == 2
            assert b"quux" in payload  # pagina corta concatenata

    def test_first_page_empty_retries(self):
        """Regressione: il retry anti-throttle si applica ANCHE alla prima pagina
        in modalità paginazione (keyset) — un 200 vuoto all'ingresso viene
        ritentato, non trattato come 'fine dati'."""
        calls = [0]

        def side_effect(*a, **kw):
            calls[0] += 1
            if calls[0] == 1:
                # prima pagina vuota (throttle WAF)
                return _http_ok(text="s,value\n", headers={"Content-Type": "text/csv"})
            if calls[0] == 2:
                # retry → dati (piena)
                return _http_ok(text=self.CSV_K1, headers={"Content-Type": "text/csv"})
            # pagina corta → fine
            return _http_ok(text=self.CSV_K3, headers={"Content-Type": "text/csv"})

        with patch("toolkit.plugins.sparql.HttpClient") as mock_cls:
            mock_cls.return_value.post.side_effect = side_effect
            source = SparqlSource()
            with patch("toolkit.plugins.sparql.time.sleep"):
                payload, _ = source.fetch(
                    "https://example.test/sparql",
                    "SELECT ?s WHERE { ?s ?p ?o } ORDER BY ?s LIMIT 10",
                    pagination={"mode": "keyset", "key": "?s"},
                    step=2,
                )
            # la prima pagina throttlata è stata recuperata dal retry
            assert b"foo" in payload and b"bar" in payload

    def test_keyset_max_pages_enforced(self):
        """Regressione: keyset rispetta il tetto ``pages`` — un endpoint che
        restituisce sempre pagine piene non causa loop infinito."""
        with patch("toolkit.plugins.sparql.HttpClient") as mock_cls:
            mock_cls.return_value.post.return_value = _http_ok(
                text=self.CSV_K1, headers={"Content-Type": "text/csv"}
            )
            source = SparqlSource()
            with patch("toolkit.plugins.sparql.time.sleep"):
                with pytest.raises(DownloadError, match="exceeded max pages"):
                    source.fetch(
                        "https://example.test/sparql",
                        "SELECT ?s WHERE { ?s ?p ?o } ORDER BY ?s LIMIT 10",
                        pagination={"mode": "keyset", "key": "?s"},
                        step=2,
                        pages=2,
                    )

    def test_keyset_missing_key_column_raises(self):
        """Keyset: colonna chiave assente nell'output → DownloadError (non silenzio)."""
        csv_p1_no_key = "altra_colonna,value\nfoo,1\nbar,2\n"
        with patch("toolkit.plugins.sparql.HttpClient") as mock_cls:
            mock_cls.return_value.post.return_value = _http_ok(
                text=csv_p1_no_key,
                headers={"Content-Type": "text/csv"},
            )
            source = SparqlSource()
            with pytest.raises(DownloadError, match="non trovata"):
                source.fetch(
                    "https://example.test/sparql",
                    "SELECT ?s WHERE { ?s ?p ?o } ORDER BY ?s LIMIT 10",
                    pagination={"mode": "keyset", "key": "?s"},
                    step=2,
                )

    def test_keyset_requires_key(self):
        """Keyset senza 'key' → DownloadError esplicito."""
        source = SparqlSource()
        with pytest.raises(DownloadError, match="requires 'key'"):
            source.fetch(
                "https://example.test/sparql",
                "SELECT ?s WHERE { ?s ?p ?o }",
                pagination={"mode": "keyset"},
            )


def test_keyset_literal_types():
    """_keyset_literal genera il letterale SPARQL corretto per tipo."""
    from toolkit.plugins.sparql import _keyset_literal

    uri = "http://dati.camera.it/ocd/deputato.rdf/d1_19"
    assert _keyset_literal(uri) == f"<{uri}>"
    assert _keyset_literal("12345") == "12345"
    assert _keyset_literal("bar") == '"bar"'
    assert _keyset_literal('say "hi"') == '"say \\"hi\\""'


def test_keyset_literal_modes():
    """key_literal forza il tipo: uri, string, number."""
    from toolkit.plugins.sparql import _keyset_literal

    uri = "http://dati.camera.it/ocd/aic.rdf/a19_123"
    assert _keyset_literal(uri, "uri") == f"<{uri}>"
    # STR(?var) → il valore è una stringa, non un IRI
    assert _keyset_literal(uri, "string") == f'"{uri}"'
    assert _keyset_literal("42", "number") == "42"
    with pytest.raises(DownloadError, match="non è numerico"):
        _keyset_literal("bar", "number")


def test_inject_filter_into_where_not_after_brace():
    """Il FILTER keyset deve stare DENTRO la WHERE, non dopo la graffa.

    Regressione: appenderlo prima di ORDER BY produceva
    `... } FILTER(...) ORDER BY ...`, sintassi che Virtuoso rifiuta —
    causa per cui camera-sindisp usava uno script manuale.
    """
    from toolkit.plugins.sparql import _inject_filter_into_where

    q = "SELECT ?s WHERE { ?s ?p ?o } ORDER BY ?s LIMIT 10"
    out = _inject_filter_into_where(q, "FILTER(?s > 1)")
    assert "} FILTER" not in out
    assert out.index("FILTER") < out.rindex("}")
    assert out.rindex("}") < out.index("ORDER BY")


def test_inject_filter_ignores_braces_in_strings_and_iris():
    """Graffe dentro stringhe/IRI non alterano l'individuazione della WHERE."""
    from toolkit.plugins.sparql import _inject_filter_into_where

    q = (
        "PREFIX ocd: <http://dati.camera.it/ocd/>\n"
        'SELECT ?s WHERE { ?s ocd:titolo "un {titolo} strano" . }\n'
        "ORDER BY ?s"
    )
    out = _inject_filter_into_where(q, "FILTER(?s > 1)")
    assert out.count("FILTER(?s > 1)") == 1
    assert out.index("FILTER") > out.index("un {titolo} strano")
    assert out.index("FILTER") < out.rindex("}")


def test_inject_filter_supports_query_without_where_keyword():
    """La keyword WHERE è opzionale in SPARQL: la graffa iniziale è la WHERE."""
    from toolkit.plugins.sparql import _inject_filter_into_where

    out = _inject_filter_into_where("SELECT ?s { ?s ?p ?o }", "FILTER(?s > 1)")
    assert "} FILTER" not in out
    assert out.index("FILTER") < out.rindex("}")


class TestKeysetFilterPlacement:
    """Keyset: il FILTER è valido per Virtuoso (dentro la WHERE / placeholder)."""

    CSV = "s,value\nfoo,1\nbar,2\n"

    def _run(self, query, pagination, csv=CSV, step=2):
        queries: list[str] = []

        def side_effect(*a, **kw):
            q = a[1].get("query", "") if len(a) > 1 and isinstance(a[1], dict) else ""
            queries.append(q)
            if len(queries) == 1:
                return _http_ok(text=csv, headers={"Content-Type": "text/csv"})
            return _http_ok(text="s,value\n", headers={"Content-Type": "text/csv"})

        with patch("toolkit.plugins.sparql.HttpClient") as mock_cls:
            mock_cls.return_value.post.side_effect = side_effect
            source = SparqlSource()
            with patch("toolkit.plugins.sparql.time.sleep"):
                source.fetch(
                    "https://example.test/sparql",
                    query,
                    pagination=pagination,
                    step=step,
                )
        return queries

    def test_filter_inside_where(self):
        queries = self._run(
            "SELECT ?s WHERE { ?s ?p ?o } ORDER BY ?s LIMIT 10",
            {"mode": "keyset", "key": "?s"},
        )
        assert 'FILTER(?s > "bar")' in queries[1]
        assert "} FILTER" not in queries[1]
        assert queries[1].index("FILTER") < queries[1].rindex("}")

    def test_placeholder_isolates_filter_in_subquery(self):
        """Con {keyset} il filtro va nella sottoquery: evita il bug
        ottimizzatore Virtuoso su FILTER + OPTIONAL nella stessa WHERE."""
        query = (
            "SELECT ?s ?label WHERE {\n"
            "  { SELECT ?s WHERE { ?s ?p ?o {keyset} } ORDER BY ?s LIMIT 10 }\n"
            "  OPTIONAL { ?s <http://www.w3.org/2000/01/rdf-schema#label> ?label }\n"
            "}"
        )
        queries = self._run(query, {"mode": "keyset", "key": "?s"})
        # pagina 1: placeholder rimosso
        assert "{keyset}" not in queries[0]
        # pagina 2: FILTER nella sottoquery, prima della sua graffa di chiusura
        assert 'FILTER(?s > "bar")' in queries[1]
        inner_close = queries[1].index("} ORDER BY")
        assert queries[1].index("FILTER") < inner_close

    def test_key_expr_and_string_literal(self):
        """STR(?atto) → confronto con stringa quotata, non con IRI."""
        csv = "atto,titolo\nhttp://dati.camera.it/ocd/aic.rdf/a19_1,uno\nhttp://dati.camera.it/ocd/aic.rdf/a19_2,due\n"
        queries = self._run(
            "SELECT ?atto WHERE { ?atto ?p ?o {keyset} } ORDER BY STR(?atto) LIMIT 10",
            {
                "mode": "keyset",
                "key": "?atto",
                "key_expr": "STR(?atto)",
                "key_column": "atto",
                "key_literal": "string",
            },
            csv=csv,
        )
        assert 'FILTER(STR(?atto) > "http://dati.camera.it/ocd/aic.rdf/a19_2")' in queries[1]
        assert "FILTER(STR(?atto) > <" not in queries[1]


class TestCurlFallback:
    """Ultima spiaggia per WAF con TLS fingerprinting (es. dati.senato.it).

    Le librerie Python vengono bloccate prima della risposta HTTP; curl passa.
    Il fallback scatta solo dopo POST e GET falliti.
    """

    JSON_BODY = '{"head":{"vars":["x"]},"results":{"bindings":[{"x":{"value":"1"}}]}}'

    def test_post_and_get_403_then_curl_succeeds(self):
        with (
            patch("toolkit.plugins.sparql.HttpClient") as mock_cls,
            patch(
                "toolkit.plugins.sparql._curl_sparql",
                return_value=(200, self.JSON_BODY.encode(), "application/sparql-results+json"),
            ) as mock_curl,
        ):
            mock_cls.return_value.post.return_value = _http_ok(status=403, text="Forbidden")
            mock_cls.return_value.get.return_value = _http_ok(status=403, text="Forbidden")
            source = SparqlSource()
            payload, _ = source.fetch(
                "https://dati.senato.it/sparql",
                "SELECT ?x WHERE { }",
                accept_format="sparql-results+json",
            )
        assert b"x" in payload and b"1" in payload
        assert mock_curl.called

    def test_html_200_then_curl_succeeds(self):
        """WAF che risponde 200 con HTML: non va scambiato per un risultato."""
        with (
            patch("toolkit.plugins.sparql.HttpClient") as mock_cls,
            patch(
                "toolkit.plugins.sparql._curl_sparql",
                return_value=(200, b"name,value\nfoo,1\n", "text/csv"),
            ) as mock_curl,
        ):
            html = _http_ok(
                status=200,
                text="<!DOCTYPE html><html><body>blocked</body></html>",
                headers={"Content-Type": "text/html"},
            )
            mock_cls.return_value.post.return_value = html
            mock_cls.return_value.get.return_value = html
            source = SparqlSource()
            payload, _ = source.fetch("https://dati.senato.it/sparql", "SELECT ?s WHERE { }")
        assert b"foo" in payload
        assert mock_curl.called

    def test_curl_unavailable_propagates_original_error(self):
        with (
            patch("toolkit.plugins.sparql.HttpClient") as mock_cls,
            patch("toolkit.plugins.sparql._curl_sparql", return_value=None),
        ):
            mock_cls.return_value.post.return_value = _http_ok(status=403, text="Forbidden")
            mock_cls.return_value.get.return_value = _http_ok(status=403, text="Forbidden")
            source = SparqlSource()
            source._page_retry_delay = 0
            with pytest.raises(DownloadError, match="SPARQL request failed"):
                source.fetch("https://dati.senato.it/sparql", "SELECT ?s WHERE { }")

    def test_curl_disabled_skips_fallback(self):
        with (
            patch("toolkit.plugins.sparql.HttpClient") as mock_cls,
            patch("toolkit.plugins.sparql._curl_sparql") as mock_curl,
        ):
            mock_cls.return_value.post.return_value = _http_ok(status=403, text="Forbidden")
            mock_cls.return_value.get.return_value = _http_ok(status=403, text="Forbidden")
            source = SparqlSource()
            source._curl_fallback = False
            source._page_retry_delay = 0
            with pytest.raises(DownloadError, match="SPARQL request failed"):
                source.fetch("https://dati.senato.it/sparql", "SELECT ?s WHERE { }")
        assert not mock_curl.called
