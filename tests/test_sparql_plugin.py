from __future__ import annotations

import pytest

from toolkit.core.exceptions import DownloadError
from toolkit.plugins.sparql import SparqlSource, _sparql_json_to_csv


class _FakeSparqlResponse:
    def __init__(
        self,
        status_code: int = 200,
        text: str = "",
        content: bytes | None = None,
        headers: dict[str, str | bytes] | None = None,
    ):
        self.status_code = status_code
        self.text = text
        self.content = content if content is not None else text.encode("utf-8")
        self.headers = headers or {}


def test_sparql_fetch_csv_success(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[object, object]] = []

    def fake_post(url: str, data: dict, headers: dict, timeout: int):
        calls.append({"url": url, "data": data, "headers": headers, "timeout": timeout})
        return _FakeSparqlResponse(
            status_code=200,
            text="name,value\nfoo,123\nbar,456\n",
            headers={"Content-Type": "text/csv"},
        )

    monkeypatch.setattr("toolkit.plugins.sparql.requests.post", fake_post)

    source = SparqlSource(timeout=30)
    payload, origin = source.fetch(
        "https://example.test/sparql",
        "SELECT ?name ?value WHERE { }",
        accept_format="csv",
    )

    assert origin == "https://example.test/sparql"
    assert b"name,value" in payload
    assert b"foo,123" in payload
    assert len(calls) == 1
    assert calls[0]["url"] == "https://example.test/sparql"
    assert calls[0]["timeout"] == 30


def test_sparql_fetch_json_success(monkeypatch: pytest.MonkeyPatch) -> None:
    json_response = """{
  "head": { "vars": ["name", "value"] },
  "results": {
    "bindings": [
      { "name": { "type": "literal", "value": "Alice" }, "value": { "type": "literal", "value": "42" } },
      { "name": { "type": "literal", "value": "Bob" }, "value": { "type": "literal", "value": "7" } }
    ]
  }
}"""
    calls: list[dict[object, object]] = []

    def fake_post(url: str, data: dict, headers: dict, timeout: int):
        calls.append({"url": url, "data": data, "headers": headers, "timeout": timeout})
        return _FakeSparqlResponse(
            status_code=200,
            text=json_response,
            headers={"Content-Type": "application/sparql-results+json"},
        )

    monkeypatch.setattr("toolkit.plugins.sparql.requests.post", fake_post)

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


def test_sparql_fetch_http_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_post(url: str, data: dict, headers: dict, timeout: int):
        return _FakeSparqlResponse(status_code=500, text="Internal Server Error")

    monkeypatch.setattr("toolkit.plugins.sparql.requests.post", fake_post)

    source = SparqlSource()
    with pytest.raises(DownloadError, match="HTTP 500"):
        source.fetch("https://example.test/sparql", "SELECT * WHERE { }")


def test_sparql_fetch_network_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_post(url: str, data: dict, headers: dict, timeout: int):
        raise Exception("connection refused")

    monkeypatch.setattr("toolkit.plugins.sparql.requests.post", fake_post)

    source = SparqlSource()
    with pytest.raises(DownloadError, match="connection refused"):
        source.fetch("https://example.test/sparql", "SELECT * WHERE { }")


def test_sparql_fetch_empty_results(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_post(url: str, data: dict, headers: dict, timeout: int):
        return _FakeSparqlResponse(
            status_code=200,
            text='{"head":{"vars":["name"]},"results":{"bindings":[]}}',
            headers={"Content-Type": "application/sparql-results+json"},
        )

    monkeypatch.setattr("toolkit.plugins.sparql.requests.post", fake_post)

    source = SparqlSource()
    with pytest.raises(DownloadError, match="no results"):
        source.fetch(
            "https://example.test/sparql",
            "SELECT ?name WHERE { }",
            accept_format="sparql-results+json",
        )


def test_sparql_fetch_invalid_json(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_post(url: str, data: dict, headers: dict, timeout: int):
        return _FakeSparqlResponse(
            status_code=200,
            text="not json at all",
            headers={"Content-Type": "application/sparql-results+json"},
        )

    monkeypatch.setattr("toolkit.plugins.sparql.requests.post", fake_post)

    source = SparqlSource()
    with pytest.raises(DownloadError, match="Invalid SPARQL JSON"):
        source.fetch(
            "https://example.test/sparql",
            "SELECT * WHERE { }",
            accept_format="sparql-results+json",
        )


def test_sparql_fetch_unsupported_content_type_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_post(url: str, data: dict, headers: dict, timeout: int):
        return _FakeSparqlResponse(
            status_code=200,
            text="<xml>not supported</xml>",
            headers={"Content-Type": "application/xml"},
        )

    monkeypatch.setattr("toolkit.plugins.sparql.requests.post", fake_post)

    source = SparqlSource()
    with pytest.raises(DownloadError, match="Unsupported Content-Type"):
        source.fetch(
            "https://example.test/sparql",
            "SELECT * WHERE { }",
            accept_format="csv",
        )


def test_sparql_fetch_missing_endpoint_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    source = SparqlSource()
    with pytest.raises(DownloadError, match="requires endpoint URL"):
        source.fetch("", "SELECT * WHERE { }")


def test_sparql_fetch_missing_query_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    source = SparqlSource()
    with pytest.raises(DownloadError, match="requires a query"):
        source.fetch("https://example.test/sparql", "")


def test_sparql_json_to_csv_binding_types(monkeypatch: pytest.MonkeyPatch) -> None:
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
