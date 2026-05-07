from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from toolkit.mcp import server as mcp_server
from toolkit.mcp.toolkit_client import ToolkitClientError


def test_mcp_server_registers_expected_tools() -> None:
    tools = asyncio.run(mcp_server.mcp.list_tools())
    tool_names = {tool.name for tool in tools}
    assert tool_names == {
        "toolkit_inspect_paths",
        "toolkit_show_schema",
        "toolkit_raw_profile",
        "toolkit_run_summary",
        "toolkit_summary",
        "toolkit_blocker_hints",
        "toolkit_review_readiness",
        "toolkit_list_runs",
        "toolkit_schema_diff",
        "toolkit_csv_preview",
    }


def test_guard_returns_payload_on_success() -> None:
    assert mcp_server._guard(lambda x: {"ok": x}, 123) == {"ok": 123}


def test_guard_wraps_toolkit_client_error() -> None:
    def boom() -> dict[str, str]:
        raise ToolkitClientError("errore client")

    assert mcp_server._guard(boom) == {"error": "errore client"}


def test_guard_does_not_swallow_unexpected_errors() -> None:
    def boom() -> dict[str, str]:
        raise ValueError("unexpected")

    with pytest.raises(ValueError, match="unexpected"):
        mcp_server._guard(boom)


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


def test_toolkit_json_removed() -> None:
    """_toolkit_json rimosso — subprocess sostituito da chiamate dirette."""
    pass

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
