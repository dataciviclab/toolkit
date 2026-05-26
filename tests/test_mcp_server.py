from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from toolkit.mcp import server as mcp_server
from toolkit.mcp.errors import ErrorCode, ToolkitClientError


def test_mcp_server_registers_expected_tools() -> None:
    tools = asyncio.run(mcp_server.mcp.list_tools())
    tool_names = {tool.name for tool in tools}
    assert tool_names == {
        "toolkit_inspect_paths",
        "toolkit_show_schema",
        "toolkit_raw_profile",
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


def test_toolkit_show_schema_passes_layer_and_year(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict[str, object] = {}

    def fake_impl(config_path: str, layer: str, year: int | None) -> dict[str, object]:
        calls["config_path"] = config_path
        calls["layer"] = layer
        calls["year"] = year
        return {"layer": layer, "year": year}

    monkeypatch.setattr(mcp_server, "show_schema_impl", fake_impl)

    payload = mcp_server.toolkit_show_schema("dataset.yml", "mart", 2024)

    assert payload == {"layer": "mart", "year": 2024}
    assert calls == {"config_path": "dataset.yml", "layer": "mart", "year": 2024}


def test_toolkit_raw_profile_passes_config_and_year(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict[str, object] = {}

    def fake_impl(config_path: str, year: int | None) -> dict[str, object]:
        calls["config_path"] = config_path
        calls["year"] = year
        return {"profile_exists": True}

    monkeypatch.setattr(mcp_server, "raw_profile_impl", fake_impl)

    payload = mcp_server.toolkit_raw_profile("dataset.yml", 2024)

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
