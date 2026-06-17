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
        "toolkit_list_runs",
        "toolkit_schema_diff",
        "toolkit_csv_preview",
        "toolkit_list_candidates",
        "toolkit_layer",
        "toolkit_status",
        "toolkit_probe_url",
        "toolkit_probe_url_routed",
        "toolkit_ckan_package_show",
        "toolkit_html_extract_links",
        "toolkit_sparql_query",
        "toolkit_preview_url",
        "toolkit_preflight",
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
    """list_candidates must forward stage and status_filter to the impl."""
    calls: dict[str, object] = {}

    def fake_impl(stage: str, status_filter: str | None) -> list[dict[str, object]]:
        calls["stage"] = stage
        calls["status_filter"] = status_filter
        return [{"slug": "test", "stage": stage, "status": status_filter}]

    monkeypatch.setattr(mcp_server, "list_candidates_impl", fake_impl)

    # Test con status_filter
    result = mcp_server.toolkit_list_candidates("candidates", "SUCCESS")
    assert isinstance(result, dict), f"expected dict, got {type(result)}"
    assert "candidates" in result, f"expected 'candidates' key, got {list(result.keys())}"
    assert result["candidates"][0]["stage"] == "candidates"
    assert result["candidates"][0]["status"] == "SUCCESS"
    assert result["count"] == 1
    assert calls == {"stage": "candidates", "status_filter": "SUCCESS"}

    # Test con status_filter=None
    result2 = mcp_server.toolkit_list_candidates("all", None)
    assert result2["candidates"][0]["status"] is None


def test_toolkit_preflight_returns_report(monkeypatch: pytest.MonkeyPatch) -> None:
    """toolkit_preflight passa config e years a run_preflight."""
    calls: dict[str, object] = {}

    def fake_preflight(config, *, years_arg=None):
        calls["config"] = str(config)
        calls["years_arg"] = years_arg
        return {"config": str(config), "sources": [], "years": [2024], "status": "passed"}

    monkeypatch.setattr(
        "toolkit.cli.preflight_ops.run_preflight",
        fake_preflight,
    )

    result = mcp_server.toolkit_preflight("dataset.yml", years="2024")

    assert result["status"] == "passed"
    assert calls["config"] == "dataset.yml"
    assert calls["years_arg"] == "2024"
