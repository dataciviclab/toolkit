from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

pytest.importorskip("mcp.server.fastmcp", reason="richiede mcp>=1.2 (lab-connectors[mcp])")
from toolkit.mcp import server as mcp_server
from toolkit.mcp.errors import ToolkitClientError

pytestmark = pytest.mark.contract


def test_mcp_server_registers_expected_tools() -> None:
    tools = asyncio.run(mcp_server.mcp.list_tools())
    tool_names = {tool.name for tool in tools}
    assert tool_names == {
        "toolkit_dataset",
        "toolkit_query",
        "toolkit_pipeline",
        "toolkit_source",
    }


# ---------------------------------------------------------------------------
# toolkit_pipeline
# ---------------------------------------------------------------------------


def test_toolkit_pipeline_contract() -> None:
    result = mcp_server.toolkit_pipeline(action="contract", layer="clean")
    assert result["layer"] == "clean"
    assert "sql_source" in result


def test_toolkit_pipeline_contract_all() -> None:
    result = mcp_server.toolkit_pipeline(action="contract")
    assert "clean" in result
    assert "mart" in result


def test_toolkit_source_probe_error_has_error_code(monkeypatch: pytest.MonkeyPatch) -> None:
    from lab_connectors.mcp import ErrorCode as LabErrorCode

    def failing_impl(url: str, timeout: int) -> dict:
        raise ToolkitClientError("test probe error")

    monkeypatch.setattr(mcp_server, "probe_url_routed_impl", failing_impl)
    payload = mcp_server.toolkit_source(action="probe", url="https://example.gov.it", timeout=15)
    assert "error" in payload
    assert "message" in payload
    assert payload["error"] == LabErrorCode.UNEXPECTED.value


# ---------------------------------------------------------------------------
# CSV preview (schema_ops unit tests — unchanged)
# ---------------------------------------------------------------------------


def test_csv_preview_returns_profiler_aligned_fields(tmp_path: Path) -> None:
    from toolkit.mcp.schema_ops import csv_preview

    csv_path = tmp_path / "italian.csv"
    csv_path.write_text("Regione;Valore\nLombardia;1.234,56\nLazio;7.890,12\n", encoding="utf-8")

    result = csv_preview(str(csv_path), limit=10)

    assert "delim_suggested" in result
    assert "encoding_suggested" in result
    assert "decimal_suggested" in result
    assert "skip_suggested" in result
    assert "robust_read_suggested" in result
    assert result["delim_suggested"] == ";"
    assert result["decimal_suggested"] == ","
    assert result["encoding_suggested"] is not None

    assert "mapping_suggestions" in result
    mapping = result["mapping_suggestions"]
    assert "Regione" in mapping or "Valore" in mapping

    assert result["path"] == str(csv_path)
    assert result["column_count"] == 2
    assert len(result["preview"]) == 2
    assert result["row_count_estimate"] == 2


def test_csv_preview_ragged_csv_succeeds_with_robust_read(tmp_path: Path) -> None:
    from toolkit.mcp.schema_ops import csv_preview

    csv_path = tmp_path / "ragged.csv"
    csv_path.write_text("a;b\n1;2;3\n4;5;6\n", encoding="utf-8")

    result = csv_preview(str(csv_path), limit=10)

    assert "preview" in result
    assert "mapping_suggestions" in result
    assert result["robust_read_suggested"] is True
    assert len(result["preview"]) == 2


# ---------------------------------------------------------------------------
# SPARQL flattening (scout_ops unit tests — unchanged)
# ---------------------------------------------------------------------------


def _make_fake_bindings(*rows: dict[str, str]) -> list[dict[str, dict]]:
    return [{k: {"type": "literal", "value": v} for k, v in row.items()} for row in rows]


def test_mcp_sparql_query_flattens_bindings(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fake_sparql(_endpoint: str, _query: str, timeout: int = 60) -> list[dict[str, dict]]:
        return _make_fake_bindings(
            {"s": "http://a/1", "p": "pred1", "o": "hello"},
            {"s": "http://a/2", "p": "pred2", "o": "world"},
        )

    monkeypatch.setattr("lab_connectors.http.sparql.execute_sparql", _fake_sparql)

    from toolkit.mcp.scout_ops import mcp_sparql_query

    result = mcp_sparql_query("https://e.org/sparql", "SELECT * WHERE {?s ?p ?o} LIMIT 2")

    assert result["columns"] == ["s", "p", "o"]
    assert result["total_rows"] == 2
    assert result["results"] == [
        {"s": "http://a/1", "p": "pred1", "o": "hello"},
        {"s": "http://a/2", "p": "pred2", "o": "world"},
    ]
    assert result["truncated"] is False
    assert "error" not in result


def test_mcp_sparql_query_respects_max_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    many = _make_fake_bindings(*[{"x": str(i)} for i in range(50)])

    def _fake_sparql(_endpoint: str, _query: str, timeout: int = 60) -> list[dict[str, dict]]:
        return many

    monkeypatch.setattr("lab_connectors.http.sparql.execute_sparql", _fake_sparql)

    from toolkit.mcp.scout_ops import mcp_sparql_query

    result = mcp_sparql_query("https://e.org/sparql", "SELECT ?x WHERE {?s ?p ?x}", max_rows=3)

    assert len(result["results"]) == 3
    assert result["total_rows"] == 3
    assert result["truncated"] is True
    assert result["columns"] == ["x"]
    assert result["results"][0]["x"] == "0"
    assert result["results"][2]["x"] == "2"


def test_mcp_sparql_query_handles_empty_bindings(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fake_sparql(_endpoint: str, _query: str, timeout: int = 60) -> list[dict[str, dict]]:
        return []

    monkeypatch.setattr("lab_connectors.http.sparql.execute_sparql", _fake_sparql)

    from toolkit.mcp.scout_ops import mcp_sparql_query

    result = mcp_sparql_query("https://e.org/sparql", "SELECT * WHERE {?s ?p ?o}")

    assert result["columns"] == []
    assert result["total_rows"] == 0
    assert result["results"] == []
    assert "error" not in result


def test_mcp_sparql_query_handles_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fake_sparql(_endpoint: str, _query: str, timeout: int = 60) -> list[dict[str, dict]]:
        raise RuntimeError("SPARQL endpoint unreachable")

    monkeypatch.setattr("lab_connectors.http.sparql.execute_sparql", _fake_sparql)

    from toolkit.mcp.scout_ops import mcp_sparql_query

    result = mcp_sparql_query("https://e.org/sparql", "SELECT * WHERE {?s ?p ?o}")

    assert "error" in result
    assert "SPARQL query failed" in result["error"]
    assert result["columns"] == []
    assert result["total_rows"] == 0
    assert result["results"] == []
