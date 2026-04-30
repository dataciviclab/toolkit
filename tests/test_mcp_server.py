from __future__ import annotations

import asyncio

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
        "toolkit_run_state",
        "toolkit_run_summary",
        "toolkit_summary",
        "toolkit_blocker_hints",
        "toolkit_review_readiness",
        "toolkit_list_runs",
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


def test_toolkit_json_includes_cmd_in_error_on_empty_output(monkeypatch: pytest.MonkeyPatch) -> None:
    """When CLI fails with no stdout/stderr, error should include the command."""
    import subprocess
    from toolkit.mcp.toolkit_client import ToolkitClientError

    def fake_run(*args: object, **kwargs: object) -> object:
        fake_result = subprocess.CompletedProcess(args=[], returncode=127)
        return fake_result

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(ToolkitClientError, match="exit code 127") as exc_info:
        from toolkit.mcp import cli_adapter  # re-import to apply monkeypatch
        cli_adapter._toolkit_json(["inspect", "paths", "--config", "dataset.yml"])

    assert "toolkit.cli.app" in str(exc_info.value)


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
