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
        "toolkit_list_runs",
        "toolkit_schema_diff",
        "toolkit_layer",
        "toolkit_status",
        "toolkit_contract",
        "toolkit_probe_url",
        "toolkit_ckan_package_show",
        "toolkit_html_extract_links",
        "toolkit_sparql_query",
        "toolkit_preview_url",
        "toolkit_preflight",
        "toolkit_find",
        "toolkit_dataset_overview",
        "toolkit_registry_list",
        "toolkit_registry_show",
        "toolkit_graph",
    }


def test_toolkit_contract_structure() -> None:
    """toolkit_contract returns stable structure with all required keys."""
    result = mcp_server.toolkit_contract(layer="all")
    assert "version" in result
    assert "pipeline" in result
    assert "clean" in result
    assert "mart" in result
    assert "constants" in result
    assert "tldr" in result

    # Clean contract has macros with warning rules
    clean = result["clean"]
    assert clean["sql_source"]["view"] == "raw_input"
    assert len(clean["macros"]) >= 8
    italian_macro = [m for m in clean["macros"] if m["name"] == "normalize_italian_number"]
    assert len(italian_macro) == 1
    assert "warning" in italian_macro[0]

    # Layer-specific queries
    # Layer-specific queries
    raw_only = mcp_server.toolkit_contract(layer="raw")
    assert raw_only["layer"] == "raw"
    assert "source_types" in raw_only
    assert any(s["type"] == "http_file" for s in raw_only["source_types"])

    clean_only = mcp_server.toolkit_contract(layer="clean")
    assert clean_only["layer"] == "clean"
    assert "sql_source" in clean_only
    assert clean_only["sql_source"]["view"] == "raw_input"

    mart_only = mcp_server.toolkit_contract(layer="mart")
    assert mart_only["layer"] == "mart"
    assert "sql_source" in mart_only
    assert mart_only["sql_source"]["view"] == "clean_input"


def test_tool_returns_payload_on_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Through a real tool implementation, guard passes payload through unchanged."""
    monkeypatch.setattr(mcp_server, "probe_url_impl", lambda url, timeout: {"ok": True})
    result = mcp_server.toolkit_probe_url("https://example.gov.it", timeout=15)
    assert result == {"ok": True}


# ---------------------------------------------------------------------------
# Scout tool contract tests
# ---------------------------------------------------------------------------


def test_toolkit_probe_url_forwards_params(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict = {}

    def fake_impl(url: str, timeout: int) -> dict:
        calls.update(url=url, timeout=timeout)
        return {"status_code": 200}

    monkeypatch.setattr(mcp_server, "probe_url_impl", fake_impl)
    result = mcp_server.toolkit_probe_url("https://example.gov.it", timeout=30)
    assert result == {"status_code": 200}
    assert calls == {"url": "https://example.gov.it", "timeout": 30}


def test_toolkit_probe_url_with_routed(monkeypatch: pytest.MonkeyPatch) -> None:
    """toolkit_probe_url con routed=True usa l'implementazione routed."""
    calls: dict = {}

    def fake_impl(url: str, timeout: int) -> dict:
        calls.update(url=url, timeout=timeout)
        return {"source_type": "ckan"}

    monkeypatch.setattr(mcp_server, "probe_url_routed_impl", fake_impl)
    result = mcp_server.toolkit_probe_url("https://dati.gov.it", timeout=15, routed=True)
    assert result == {"source_type": "ckan"}
    assert calls == {"url": "https://dati.gov.it", "timeout": 15}


def test_toolkit_ckan_package_show_forwards_params(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict = {}

    def fake_impl(endpoint: str, package_id: str, timeout: int) -> dict:
        calls.update(endpoint=endpoint, package_id=package_id, timeout=timeout)
        return {"title": "Test dataset", "resources": []}

    monkeypatch.setattr(mcp_server, "ckan_package_show_impl", fake_impl)
    result = mcp_server.toolkit_ckan_package_show("https://dati.gov.it", "test-dataset", timeout=30)
    assert result == {"title": "Test dataset", "resources": []}
    assert calls == {"endpoint": "https://dati.gov.it", "package_id": "test-dataset", "timeout": 30}


def test_toolkit_html_extract_links_forwards_params(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict = {}

    def fake_impl(url: str, timeout: int) -> dict:
        calls.update(url=url, timeout=timeout)
        return {"total": 2, "data_links": [{"url": "https://ex.it/data.csv"}], "groups": []}

    monkeypatch.setattr(mcp_server, "html_extract_links_impl", fake_impl)
    result = mcp_server.toolkit_html_extract_links("https://example.gov.it/pagina", timeout=20)
    assert result["total"] == 2
    assert len(result["data_links"]) == 1
    assert calls == {"url": "https://example.gov.it/pagina", "timeout": 20}


def test_toolkit_sparql_query_forwards_params(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict = {}

    def fake_impl(endpoint: str, query: str, timeout: int, max_rows: int) -> dict:
        calls.update(endpoint=endpoint, query=query, timeout=timeout, max_rows=max_rows)
        return {"columns": ["s", "p", "o"], "total_rows": 10}

    monkeypatch.setattr(mcp_server, "sparql_query_impl", fake_impl)
    result = mcp_server.toolkit_sparql_query(
        "https://example.org/sparql", "SELECT * WHERE {?s ?p ?o}", timeout=60, max_rows=500
    )
    assert result == {"columns": ["s", "p", "o"], "total_rows": 10}
    assert calls == {
        "endpoint": "https://example.org/sparql",
        "query": "SELECT * WHERE {?s ?p ?o}",
        "timeout": 60,
        "max_rows": 500,
    }


def test_toolkit_probe_url_error_has_error_code(monkeypatch: pytest.MonkeyPatch) -> None:
    from lab_connectors.mcp import ErrorCode as LabErrorCode

    def failing_impl(url: str, timeout: int) -> dict:
        raise ToolkitClientError("test probe error")

    monkeypatch.setattr(mcp_server, "probe_url_impl", failing_impl)

    payload = mcp_server.toolkit_probe_url("https://example.gov.it", timeout=15)
    assert "error" in payload
    assert "message" in payload
    assert payload["error"] == LabErrorCode.UNEXPECTED.value


def test_toolkit_probe_url_returns_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    """guard_timed passes payload through unchanged for scout tools."""

    def fake_impl(url: str, timeout: int) -> dict:
        return {"status_code": 200, "content_type": "text/csv"}

    monkeypatch.setattr(mcp_server, "probe_url_impl", fake_impl)
    result = mcp_server.toolkit_probe_url("https://example.gov.it/data.csv", timeout=15)
    assert result == {"status_code": 200, "content_type": "text/csv"}


# inspect_schema e inspect_profile rimossi come tool MCP —
# coperti da toolkit_layer(mode="schema") e toolkit_status


def test_csv_preview_returns_profiler_aligned_fields(tmp_path: Path) -> None:
    """csv_preview output must include sniff params and be compatible with profiler.

    Regression test: ensures csv_preview reuses sniff_source_file and
    profile_with_read_cfg so mapping_suggestions, delim_suggested,
    encoding_suggested, decimal_suggested, skip_suggested, and
    robust_read_suggested are all present and consistent with profile_raw.
    """
    from toolkit.mcp.schema_ops import csv_preview

    # Italian decimal CSV: semicolon delim, comma decimal
    csv_path = tmp_path / "italian.csv"
    csv_path.write_text("Regione;Valore\nLombardia;1.234,56\nLazio;7.890,12\n", encoding="utf-8")

    result = csv_preview(str(csv_path), limit=10)

    # Must have profiler alignment fields
    assert "delim_suggested" in result
    assert "encoding_suggested" in result
    assert "decimal_suggested" in result
    assert "skip_suggested" in result
    assert "robust_read_suggested" in result
    assert result["delim_suggested"] == ";"
    assert result["decimal_suggested"] == ","
    assert result["encoding_suggested"] is not None

    # mapping_suggestions must be present and valid
    assert "mapping_suggestions" in result
    mapping = result["mapping_suggestions"]
    assert "Regione" in mapping or "Valore" in mapping

    # Basic schema fields still present
    assert result["path"] == str(csv_path)
    assert result["column_count"] == 2
    assert len(result["preview"]) == 2
    assert result["row_count_estimate"] == 2


def test_csv_preview_ragged_csv_succeeds_with_robust_read(tmp_path: Path) -> None:
    """csv_preview must succeed on ragged/IRPEF-like CSV (header < data cols).

    When profile_with_read_cfg retries with robust fallback (null_padding),
    csv_preview preview/count phase must also use the robust fallback,
    not the original cfg that would fail on ragged rows.
    Regression test for the fix: preview phase must use robust_preset
    when robust_read_suggested=True.
    """
    from toolkit.mcp.schema_ops import csv_preview

    # Ragged CSV: header has 2 cols, data rows have 3 cols
    csv_path = tmp_path / "ragged.csv"
    csv_path.write_text("a;b\n1;2;3\n4;5;6\n", encoding="utf-8")

    result = csv_preview(str(csv_path), limit=10)

    # Must succeed without raising ToolkitClientError
    assert "preview" in result
    assert "mapping_suggestions" in result
    # robust_read_suggested must be True since ragged rows need null_padding
    assert result["robust_read_suggested"] is True
    # Preview still returns data
    assert len(result["preview"]) == 2


def test_toolkit_preflight_returns_report(monkeypatch: pytest.MonkeyPatch) -> None:
    """toolkit_preflight passa config e years a run_preflight."""
    calls: dict[str, object] = {}

    def fake_preflight(config, *, years_arg=None):
        calls["config"] = str(config)
        calls["years_arg"] = years_arg
        return {"config": str(config), "sources": [], "years": [2024], "status": "passed"}

    monkeypatch.setattr(
        "toolkit.domain.preflight.run_preflight",
        fake_preflight,
    )

    result = mcp_server.toolkit_preflight("dataset.yml", years="2024")

    assert result["status"] == "passed"
    assert calls["config"] == "dataset.yml"
    assert calls["years_arg"] == "2024"


# ---------------------------------------------------------------------------
# mcp_sparql_query — flattening SPARQL bindings → righe MCP
# ---------------------------------------------------------------------------


def _make_fake_bindings(*rows: dict[str, str]) -> list[dict[str, dict]]:
    """Costruisce bindings SPARQL finti (formato {var: {type, value}})."""
    return [{k: {"type": "literal", "value": v} for k, v in row.items()} for row in rows]


def test_mcp_sparql_query_flattens_bindings(monkeypatch: pytest.MonkeyPatch) -> None:
    """Binding SPARQL JSON ({var: {type, value}}) → righe piatte {var: value}."""

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
    """Il parametro max_rows tronca i risultati e imposta truncated=True."""
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
    """Bindings vuoti → colonne vuote, zero righe, nessun errore."""

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
    """RuntimeError da execute_sparql → dict con error, risultati vuoti."""

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
