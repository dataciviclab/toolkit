"""Test contratto per la CLI ``toolkit inspect config``.

Usa monkeypatch per isolare la CLI dal backend reale: testiamo solo
il layer CLI (flag, errori, formato output), non l'esecuzione backend.
"""

from __future__ import annotations

import json
import re
from typing import Any

import pytest
from typer.testing import CliRunner

from toolkit.cli.app import app

runner = CliRunner()

CONFIG_PATH = "dataset-incubator/candidates/irpef-comunale/dataset.yml"


def _strip_ansi(text: str) -> str:
    """Rimuove sequenze ANSI escape (colori, bold, bordi) dall'output."""
    return re.sub(r"\x1b\[[0-9;]*[a-zA-Z]", "", text)


@pytest.fixture
def mock_layer_query(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Mocka layer_query per evitare dipendenza da parquet reali."""

    def _fake(config_path: str, **kwargs: Any) -> dict[str, Any]:
        mode = kwargs.get("mode", "schema")
        if mode == "schema":
            return {
                "dataset": "test",
                "layer": kwargs.get("layer", "clean"),
                "year": 2024,
                "columns": [{"name": "col1", "type": "INTEGER"}],
            }
        if mode == "preview":
            return {
                "dataset": "test",
                "layer": kwargs.get("layer", "clean"),
                "year": 2024,
                "columns": [{"name": "col1", "type": "INTEGER"}],
                "column_count": 1,
                "row_count": 10,
                "preview": [{"col1": 1}],
            }
        if mode == "profile":
            return {
                "dataset": "test",
                "year": 2024,
                "read_hints": {"encoding": "utf-8", "delimiter": ",", "skip": 0},
                "columns": {"raw": ["col1"], "count": 1},
            }
        if mode == "sql":
            return {
                "dataset": "test",
                "layer": kwargs.get("layer", "clean"),
                "year": 2024,
                "columns": [{"name": "cnt", "type": "BIGINT"}],
                "row_count": 1,
                "sql": kwargs.get("sql"),
                "preview": [{"cnt": 42}],
            }
        return {"dataset": "test"}

    monkeypatch.setattr("toolkit.cli.inspect.config_ops.layer_query", _fake)
    return {}


class TestCliLayer:
    """Test contratto per toolkit inspect config CLI."""

    @pytest.mark.contract
    def test_layer_help(self):
        """--help mostra i flag principali."""
        result = runner.invoke(app, ["inspect", "config", "--help"])
        assert result.exit_code == 0
        output = _strip_ansi(result.stdout)
        assert "--mode" in output
        assert "--layer" in output
        assert "--config" in output

    @pytest.mark.contract
    def test_layer_sql_without_sql_errors(self):
        """mode=sql senza --sql deve dare errore."""
        result = runner.invoke(
            app, ["inspect", "config", "-c", CONFIG_PATH, "-l", "clean", "-m", "sql"]
        )
        assert result.exit_code != 0
        output = (result.stdout + result.stderr).lower()
        assert "sql" in output or "error" in output

    @pytest.mark.contract
    def test_layer_profile_on_clean_errors(self):
        """mode=profile su layer clean deve dare errore."""
        result = runner.invoke(
            app, ["inspect", "config", "-c", CONFIG_PATH, "-l", "clean", "-m", "profile"]
        )
        assert result.exit_code != 0
        output = (result.stdout + result.stderr).lower()
        assert "raw" in output or "error" in output

    @pytest.mark.contract
    @pytest.mark.parametrize("mode", ["schema", "preview", "profile", "sql"])
    def test_layer_mode_json_output(self, mode, mock_layer_query):
        """Ogni mode produce JSON parsabile."""
        args = ["inspect", "config", "-c", CONFIG_PATH, "-m", mode, "--json"]
        if mode == "profile":
            args.extend(["-l", "raw"])
        if mode == "sql":
            args.extend(["--sql", "SELECT 1"])
        result = runner.invoke(app, args)
        assert result.exit_code == 0, f"{mode}: exit {result.exit_code}: {result.stdout[:200]}"
        data = json.loads(result.stdout)
        assert isinstance(data, dict)
        if mode in ("schema", "preview", "sql"):
            assert "dataset" in data
