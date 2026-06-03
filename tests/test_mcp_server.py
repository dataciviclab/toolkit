from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from toolkit.mcp import server as mcp_server
from toolkit.mcp.errors import ErrorCode, ToolkitClientError

pytestmark = pytest.mark.contract


def test_mcp_server_registers_expected_tools() -> None:
    tools = asyncio.run(mcp_server.mcp.list_tools())
    tool_names = {tool.name for tool in tools}
    assert tool_names == {
        "toolkit_inspect_paths",
        "toolkit_inspect_schema",
        "toolkit_inspect_profile",
        "toolkit_run_summary",
        "toolkit_summary",
        "toolkit_review_readiness",
        "toolkit_list_runs",
        "toolkit_schema_diff",
        "toolkit_csv_preview",
        "toolkit_list_candidates",
        "toolkit_dataset_info",
        "toolkit_clean_preview",
        "toolkit_raw_preview",
        "toolkit_probe_url",
        "toolkit_probe_url_routed",
        "toolkit_infer_topic",
        "toolkit_ckan_package_show",
        "toolkit_html_extract_links",
        "toolkit_list_ckan_datasets",
        "toolkit_list_sdmx_dataflows",
        "toolkit_sparql_query",
    }


def test_tool_returns_payload_on_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Through a real tool implementation, guard passes payload through unchanged."""

    def fake_impl(config_path: str, year: int | None) -> dict[str, object]:
        return {"ok": True, "config_path": config_path, "year": year}

    monkeypatch.setattr(mcp_server, "inspect_paths_impl", fake_impl)
    result = mcp_server.toolkit_inspect_paths("dataset.yml", 2024)
    assert result == {"ok": True, "config_path": "dataset.yml", "year": 2024}


def test_tool_error_has_error_code_and_message(monkeypatch: pytest.MonkeyPatch) -> None:
    """ToolkitClientError raised by impl is caught by guard and returned as dict.

    The error dict must have 'error' (code string) and 'message' keys.
    """
    def raising_impl(config_path: str, year: int | None) -> dict[str, object]:
        raise ToolkitClientError("config non trovato", code=ErrorCode.CONFIG_NOT_FOUND)

    monkeypatch.setattr(mcp_server, "inspect_paths_impl", raising_impl)
    result = mcp_server.toolkit_inspect_paths("dataset.yml", 2024)

    assert "error" in result
    assert "message" in result
    assert result["error"] == "config_not_found"
    assert "config non trovato" in result["message"]


def test_unexpected_error_becomes_unexpected_with_message(monkeypatch: pytest.MonkeyPatch) -> None:
    """Any unexpected exception (not ToolkitClientError) is caught by guard and
    returned as 'unexpected_error' with the original message.
    """

    def raising_impl(config_path: str, year: int | None) -> dict[str, object]:
        raise ValueError("unexpected value")

    monkeypatch.setattr(mcp_server, "inspect_paths_impl", raising_impl)
    result = mcp_server.toolkit_inspect_paths("dataset.yml", 2024)

    assert result["error"] == "unexpected_error"
    assert "unexpected value" in result["message"]


def test_toolkit_inspect_paths_passes_none_when_year_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict[str, object] = {}

    def fake_impl(config_path: str, year: int | None) -> dict[str, object]:
        calls["config_path"] = config_path
        calls["year"] = year
        return {"ok": True}

    monkeypatch.setattr(mcp_server, "inspect_paths_impl", fake_impl)

    payload = mcp_server.toolkit_inspect_paths("dataset.yml", 0)

    assert payload == {"ok": True}
    assert calls == {"config_path": "dataset.yml", "year": None}


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


def test_toolkit_probe_url_routed_forwards_params(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict = {}

    def fake_impl(url: str, timeout: int) -> dict:
        calls.update(url=url, timeout=timeout)
        return {"source_type": "ckan"}

    monkeypatch.setattr(mcp_server, "probe_url_routed_impl", fake_impl)
    result = mcp_server.toolkit_probe_url_routed("https://dati.gov.it", timeout=15)
    assert result == {"source_type": "ckan"}
    assert calls == {"url": "https://dati.gov.it", "timeout": 15}


def test_toolkit_infer_topic_forwards_params(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict = {}

    def fake_impl(text: str) -> dict:
        calls["text"] = text
        return {"topics": [{"topic": "lavoro", "score": 3}]}

    monkeypatch.setattr(mcp_server, "infer_topic_impl", fake_impl)
    result = mcp_server.toolkit_infer_topic("disoccupazione giovanile")
    assert result == {"topics": [{"topic": "lavoro", "score": 3}]}
    assert calls == {"text": "disoccupazione giovanile"}


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
        return {"total": 2, "links": ["data.csv"]}

    monkeypatch.setattr(mcp_server, "html_extract_links_impl", fake_impl)
    result = mcp_server.toolkit_html_extract_links("https://example.gov.it/pagina", timeout=20)
    assert result == {"total": 2, "links": ["data.csv"]}
    assert calls == {"url": "https://example.gov.it/pagina", "timeout": 20}


def test_toolkit_sparql_query_forwards_params(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict = {}

    def fake_impl(endpoint: str, query: str, timeout: int, max_rows: int) -> dict:
        calls.update(endpoint=endpoint, query=query, timeout=timeout, max_rows=max_rows)
        return {"columns": ["s", "p", "o"], "total_rows": 10}

    monkeypatch.setattr(mcp_server, "sparql_query_impl", fake_impl)
    result = mcp_server.toolkit_sparql_query("https://example.org/sparql", "SELECT * WHERE {?s ?p ?o}", timeout=60, max_rows=500)
    assert result == {"columns": ["s", "p", "o"], "total_rows": 10}
    assert calls == {"endpoint": "https://example.org/sparql", "query": "SELECT * WHERE {?s ?p ?o}", "timeout": 60, "max_rows": 500}


def test_toolkit_list_ckan_datasets_forwards_params(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict = {}

    def fake_impl(portal_url: str, query: str | None, rows: int, timeout: int) -> dict:
        calls.update(portal_url=portal_url, query=query, rows=rows, timeout=timeout)
        return {"count": 10, "datasets": []}

    monkeypatch.setattr(mcp_server, "list_ckan_datasets_impl", fake_impl)
    result = mcp_server.toolkit_list_ckan_datasets("https://dati.gov.it/opendata", query="pensioni", rows=50, timeout=25)
    assert result == {"count": 10, "datasets": []}
    assert calls == {"portal_url": "https://dati.gov.it/opendata", "query": "pensioni", "rows": 50, "timeout": 25}


def test_toolkit_list_sdmx_dataflows_forwards_params(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict = {}

    def fake_impl(agency: str, timeout: int) -> dict:
        calls.update(agency=agency, timeout=timeout)
        return {"agency": "IT1", "returned": 5, "dataflows": []}

    monkeypatch.setattr(mcp_server, "list_sdmx_dataflows_impl", fake_impl)
    result = mcp_server.toolkit_list_sdmx_dataflows(agency="IT1", timeout=20)
    assert result == {"agency": "IT1", "returned": 5, "dataflows": []}
    assert calls == {"agency": "IT1", "timeout": 20}


def test_toolkit_probe_url_error_has_error_code(monkeypatch: pytest.MonkeyPatch) -> None:
    from lab_connectors.mcp import ErrorCode as LabErrorCode
    from toolkit.mcp.errors import ToolkitClientError

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


def test_toolkit_inspect_schema_passes_layer_and_year(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict[str, object] = {}

    def fake_impl(config_path: str, layer: str, year: int | None) -> dict[str, object]:
        calls["config_path"] = config_path
        calls["layer"] = layer
        calls["year"] = year
        return {"layer": layer, "year": year}

    monkeypatch.setattr(mcp_server, "show_schema_impl", fake_impl)

    payload = mcp_server.toolkit_inspect_schema("dataset.yml", "mart", 2024)

    assert payload == {"layer": "mart", "year": 2024}
    assert calls == {"config_path": "dataset.yml", "layer": "mart", "year": 2024}


def test_toolkit_inspect_profile_passes_config_and_year(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict[str, object] = {}

    def fake_impl(config_path: str, year: int | None) -> dict[str, object]:
        calls["config_path"] = config_path
        calls["year"] = year
        return {"profile_exists": True}

    monkeypatch.setattr(mcp_server, "raw_profile_impl", fake_impl)

    payload = mcp_server.toolkit_inspect_profile("dataset.yml", 2024)

    assert payload == {"profile_exists": True}
    assert calls == {"config_path": "dataset.yml", "year": 2024}


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


def test_toolkit_list_candidates_passes_stage_and_filter(monkeypatch: pytest.MonkeyPatch) -> None:
    """list_candidates must pass stage AND status_filter through to the impl.

    Nota: guard() wrappa i non-dict in ``{"result": ...}``, quindi
    il consumer MCP vedra' ``{"result": [{"slug": ..., ...}]}``.
    """
    calls: dict[str, object] = {}

    def fake_impl(stage: str, status_filter: str | None) -> list[dict[str, object]]:
        calls["stage"] = stage
        calls["status_filter"] = status_filter
        return [{"slug": "test", "stage": stage, "status": status_filter}]

    monkeypatch.setattr(mcp_server, "list_candidates_impl", fake_impl)

    # Test con status_filter
    result = mcp_server.toolkit_list_candidates("candidates", "SUCCESS")
    assert "result" in result
    assert result["result"][0]["stage"] == "candidates"
    assert result["result"][0]["status"] == "SUCCESS"
    assert calls == {"stage": "candidates", "status_filter": "SUCCESS"}

    # Test con status_filter=None
    result2 = mcp_server.toolkit_list_candidates("all", None)
    assert result2["result"][0]["status"] is None


def test_toolkit_dataset_info_passes_config_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """dataset_info must pass config_path to the impl."""
    calls: dict[str, object] = {}

    def fake_impl(config_path: str) -> dict[str, object]:
        calls["config_path"] = config_path
        return {"dataset": "test"}

    monkeypatch.setattr(mcp_server, "dataset_info_impl", fake_impl)
    result = mcp_server.toolkit_dataset_info("some/path/dataset.yml")

    assert result["dataset"] == "test"
    assert calls == {"config_path": "some/path/dataset.yml"}


def test_toolkit_clean_preview_passes_params(monkeypatch: pytest.MonkeyPatch) -> None:
    """clean_preview must pass all params to the impl, converting year=0 → None."""
    calls: dict[str, object] = {}

    def fake_impl(
        config_path: str, layer: str, mart_index: int, year: int | None, limit: int
    ) -> dict[str, object]:
        calls.update(
            config_path=config_path,
            layer=layer,
            mart_index=mart_index,
            year=year,
            limit=limit,
        )
        return {"ok": True}

    monkeypatch.setattr(mcp_server, "clean_preview_impl", fake_impl)
    result = mcp_server.toolkit_clean_preview("d.yml", "mart", 1, 2023, 20)

    assert result == {"ok": True}
    assert calls == {
        "config_path": "d.yml",
        "layer": "mart",
        "mart_index": 1,
        "year": 2023,
        "limit": 20,
    }


def test_toolkit_clean_preview_converts_year_zero_to_none(monkeypatch: pytest.MonkeyPatch) -> None:
    """clean_preview(year=0) must send year=None to the impl."""
    calls: dict[str, object] = {}

    def fake_impl(
        config_path: str, layer: str, mart_index: int, year: int | None, limit: int
    ) -> dict[str, object]:
        calls["year"] = year
        return {"ok": True}

    monkeypatch.setattr(mcp_server, "clean_preview_impl", fake_impl)
    mcp_server.toolkit_clean_preview("d.yml", "clean", 0, 0, 10)

    assert calls["year"] is None


def test_toolkit_raw_preview_passes_params(monkeypatch: pytest.MonkeyPatch) -> None:
    """raw_preview must pass all params to the impl, converting year=0 → None."""
    calls: dict[str, object] = {}

    def fake_impl(
        config_path: str, year: int | None, limit: int
    ) -> dict[str, object]:
        calls.update(config_path=config_path, year=year, limit=limit)
        return {"ok": True}

    monkeypatch.setattr(mcp_server, "raw_preview_impl", fake_impl)
    result = mcp_server.toolkit_raw_preview("d.yml", 2023, 30)

    assert result == {"ok": True}
    assert calls == {"config_path": "d.yml", "year": 2023, "limit": 30}


def test_toolkit_raw_preview_converts_year_zero_to_none(monkeypatch: pytest.MonkeyPatch) -> None:
    """raw_preview(year=0) must send year=None to the impl."""
    calls: dict[str, object] = {}

    def fake_impl(
        config_path: str, year: int | None, limit: int
    ) -> dict[str, object]:
        calls["year"] = year
        return {"ok": True}

    monkeypatch.setattr(mcp_server, "raw_preview_impl", fake_impl)
    mcp_server.toolkit_raw_preview("d.yml", 0, 20)

    assert calls["year"] is None
