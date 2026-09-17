"""Test contratto per il comando toolkit validate."""

from __future__ import annotations

import re

import pytest
from typer.testing import CliRunner

from toolkit.cli.app import app

runner = CliRunner()


def _strip_ansi(text: str) -> str:
    """Rimuove sequenze ANSI escape dall'output."""
    return re.sub(r"\x1b\[[0-9;]*[a-zA-Z]", "", text)


@pytest.mark.contract
def test_validate_help_shows_usage():
    result = runner.invoke(app, ["validate", "--help"])
    assert result.exit_code == 0
    output = _strip_ansi(result.output)
    assert "validate" in output.lower()
    assert "--config" in output
    assert "--year" in output
    assert "--layer" in output
    assert "--json" in output


@pytest.mark.contract
def test_invalid_layer_rejected():
    result = runner.invoke(
        app, ["validate", "--layer", "invalid_layer", "-c", "examples/dataset_min.yml"]
    )
    assert result.exit_code != 0
    output = _strip_ansi(result.output)
    assert "Invalid layer" in output or "invalid" in output.lower()


@pytest.mark.pure_unit
def test_resolve_layers_default():
    from toolkit.cli.cmd_validate import _resolve_layers

    class FakeCfg:
        is_mart_only = False

        class mart:
            tables = [1]

    cfg = FakeCfg()
    assert _resolve_layers(None, cfg) == ["raw", "clean", "mart"]


@pytest.mark.pure_unit
def test_resolve_layers_single():
    from toolkit.cli.cmd_validate import _resolve_layers

    result = _resolve_layers("clean", None)
    assert result == ["clean"]


@pytest.mark.pure_unit
def test_resolve_layers_mart_only():
    from toolkit.cli.cmd_validate import _resolve_layers

    class FakeCfg:
        is_mart_only = True

        class mart:
            tables = []

    cfg = FakeCfg()
    assert _resolve_layers(None, cfg) == ["raw"]


@pytest.mark.pure_unit
def test_run_layer_validation_unknown_raises():
    from toolkit.cli.cmd_validate import _run_layer_validation

    with pytest.raises(ValueError, match="Unknown layer"):
        _run_layer_validation("unknown", None, 2024, None)
