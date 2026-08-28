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
        "toolkit_contract",
    }


# ---------------------------------------------------------------------------
# toolkit_dataset
# ---------------------------------------------------------------------------


def test_toolkit_dataset_find(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_find(**kwargs):
        return {"datasets": [], "total_count": 0}

    monkeypatch.setattr(mcp_server, "find_impl", fake_find)
    result = mcp_server.toolkit_dataset(action="find", query="terna")
    assert "datasets" in result


def test_toolkit_dataset_overview(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_overview(**kwargs):
        return {"slug": kwargs.get("slug"), "columns": []}

    monkeypatch.setattr(mcp_server, "dataset_overview_impl", fake_overview)
    result = mcp_server.toolkit_dataset(action="overview", slug="terna_electricity_by_source")
    assert result["slug"] == "terna_electricity_by_source"


def test_toolkit_dataset_overview_missing_slug() -> None:
    with pytest.raises(ToolkitClientError, match="overview richiede slug"):
        mcp_server.toolkit_dataset(action="overview")


def test_toolkit_dataset_status(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_status(config, **kwargs):
        return {"config": str(config)}

    monkeypatch.setattr(mcp_server, "dataset_status_impl", fake_status)
    result = mcp_server.toolkit_dataset(action="status", config_path="dataset.yml")
    assert result["config"] == "dataset.yml"


def test_toolkit_dataset_status_missing_config() -> None:
    with pytest.raises(ToolkitClientError, match="status richiede config_path"):
        mcp_server.toolkit_dataset(action="status")


def test_toolkit_dataset_preflight(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_preflight(config, *, years_arg=None):
        return {"config": str(config), "status": "passed"}

    monkeypatch.setattr("toolkit.domain.preflight.run_preflight", fake_preflight)
    result = mcp_server.toolkit_dataset(action="preflight", config_path="dataset.yml")
    assert result["status"] == "passed"


def test_toolkit_dataset_schema_diff(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_diff(config):
        return {"config": str(config), "diff": []}

    monkeypatch.setattr(mcp_server, "schema_diff_impl", fake_diff)
    result = mcp_server.toolkit_dataset(action="schema-diff", config_path="dataset.yml")
    assert result["diff"] == []


def test_toolkit_dataset_invalid_action() -> None:
    with pytest.raises(ToolkitClientError, match="non valida"):
        mcp_server.toolkit_dataset(action="bogus")


# ---------------------------------------------------------------------------
# toolkit_query
# ---------------------------------------------------------------------------


def test_toolkit_query_run(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_layer(**kwargs):
        return {"columns": ["anno"], "rows": [{"anno": 2024}]}

    monkeypatch.setattr(mcp_server, "layer_query_impl", fake_layer)
    result = mcp_server.toolkit_query(
        action="run", datasets=["terna"], sql="SELECT * FROM terna LIMIT 1"
    )
    assert "columns" in result


def test_toolkit_query_run_missing_sql() -> None:
    with pytest.raises(ToolkitClientError, match="run richiede sql"):
        mcp_server.toolkit_query(action="run", datasets=["terna"])


def test_toolkit_query_preview(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_preview(url, **kwargs):
        return {"url": url, "columns": []}

    monkeypatch.setattr(mcp_server, "preview_url_impl", fake_preview)
    result = mcp_server.toolkit_query(action="preview", url="https://example.it/data.csv")
    assert result["url"] == "https://example.it/data.csv"


def test_toolkit_query_invalid_action() -> None:
    with pytest.raises(ToolkitClientError, match="non valida"):
        mcp_server.toolkit_query(action="bogus")


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


def test_toolkit_pipeline_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_runs(config, *args, **kwargs):
        return {"runs": []}

    monkeypatch.setattr(mcp_server, "list_runs_impl", fake_runs)
    result = mcp_server.toolkit_pipeline(action="runs", config_path="dataset.yml")
    assert "runs" in result


def test_toolkit_pipeline_registry_list(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_list():
        return {"repos": []}

    monkeypatch.setattr(mcp_server, "registry_list_impl", fake_list)
    result = mcp_server.toolkit_pipeline(action="registry_list")
    assert "repos" in result


def test_toolkit_pipeline_registry_show(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_show(repo, artifact, slug=None):
        return {"repo": repo, "artifact": artifact}

    monkeypatch.setattr(mcp_server, "registry_show_impl", fake_show)
    result = mcp_server.toolkit_pipeline(
        action="registry_show", repo="eurostat", artifact="datasets"
    )
    assert result["repo"] == "eurostat"


def test_toolkit_pipeline_graph(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_graph(**kwargs):
        return {"nodes": [], "edges": []}

    monkeypatch.setattr(mcp_server, "graph_impl", fake_graph)
    result = mcp_server.toolkit_pipeline(action="graph", by_domain="appalti")
    assert "nodes" in result


def test_toolkit_pipeline_invalid_action() -> None:
    with pytest.raises(ToolkitClientError, match="non valida"):
        mcp_server.toolkit_pipeline(action="bogus")


# ---------------------------------------------------------------------------
# toolkit_source
# ---------------------------------------------------------------------------


def test_toolkit_source_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_probe(url, timeout):
        return {"status_code": 200}

    monkeypatch.setattr(mcp_server, "probe_url_impl", fake_probe)
    result = mcp_server.toolkit_source(action="probe", url="https://example.gov.it")
    assert result["status_code"] == 200


def test_toolkit_source_ckan(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_ckan(endpoint, package_id, timeout):
        return {"title": "Test"}

    monkeypatch.setattr(mcp_server, "ckan_package_show_impl", fake_ckan)
    result = mcp_server.toolkit_source(
        action="ckan", endpoint="https://dati.gov.it", package_id="test"
    )
    assert result["title"] == "Test"


def test_toolkit_source_links(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_links(url, timeout):
        return {"total": 1}

    monkeypatch.setattr(mcp_server, "html_extract_links_impl", fake_links)
    result = mcp_server.toolkit_source(action="links", url="https://example.gov.it/pagina")
    assert result["total"] == 1


def test_toolkit_source_sparql(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_sparql(endpoint, query, timeout, max_rows):
        return {"columns": ["s"], "total_rows": 1}

    monkeypatch.setattr(mcp_server, "sparql_query_impl", fake_sparql)
    result = mcp_server.toolkit_source(
        action="sparql", endpoint="https://e.org/sparql", query="SELECT * WHERE {?s ?p ?o}"
    )
    assert result["total_rows"] == 1


def test_toolkit_source_invalid_action() -> None:
    with pytest.raises(ToolkitClientError, match="non valida"):
        mcp_server.toolkit_source(action="bogus")


# ---------------------------------------------------------------------------
# toolkit_contract (backward compat)
# ---------------------------------------------------------------------------


def test_toolkit_contract_structure() -> None:
    result = mcp_server.toolkit_contract(layer="all")
    assert "version" in result
    assert "pipeline" in result
    assert "clean" in result
    assert "mart" in result
    assert "constants" in result
    assert "tldr" in result

    clean = result["clean"]
    assert clean["sql_source"]["view"] == "raw_input"
    assert len(clean["macros"]) >= 8

    raw_only = mcp_server.toolkit_contract(layer="raw")
    assert raw_only["layer"] == "raw"
    assert "source_types" in raw_only

    clean_only = mcp_server.toolkit_contract(layer="clean")
    assert clean_only["layer"] == "clean"
    assert clean_only["sql_source"]["view"] == "raw_input"

    mart_only = mcp_server.toolkit_contract(layer="mart")
    assert mart_only["layer"] == "mart"
    assert mart_only["sql_source"]["view"] == "clean_input"


# ---------------------------------------------------------------------------
# Integration: tool returns payload through guard_timed
# ---------------------------------------------------------------------------


def test_tool_returns_payload_on_success(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mcp_server, "probe_url_impl", lambda url, timeout: {"ok": True})
    result = mcp_server.toolkit_source(action="probe", url="https://example.gov.it", timeout=15)
    assert result == {"ok": True}


def test_toolkit_source_probe_error_has_error_code(monkeypatch: pytest.MonkeyPatch) -> None:
    from lab_connectors.mcp import ErrorCode as LabErrorCode

    def failing_impl(url: str, timeout: int) -> dict:
        raise ToolkitClientError("test probe error")

    monkeypatch.setattr(mcp_server, "probe_url_impl", failing_impl)
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
