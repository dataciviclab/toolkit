"""Test contratto per i nuovi comandi inspect config/summary/runs."""

from __future__ import annotations

import json
import re
from typing import Any

import pytest
from typer.testing import CliRunner

from toolkit.cli.app import app

runner = CliRunner()


def _strip_ansi(text: str) -> str:
    """Rimuove sequenze ANSI escape (colori, bold, bordi) dall'output."""
    return re.sub(r"\x1b\[[0-9;]*[a-zA-Z]", "", text)


CONFIG_PATH = "dataset-incubator/candidates/irpef-comunale/dataset.yml"


@pytest.fixture
def mock_layer_query(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Mocka layer_query in config_ops per evitare dipendenza da parquet reali."""

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


@pytest.fixture
def mock_schema_diff(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Mocka schema_diff_payload in config_ops per test --diff."""

    def _fake_payload(config_path: str) -> dict[str, Any]:
        return {
            "dataset": "test",
            "config_path": config_path,
            "years": [2024],
            "entries": [],
            "comparisons": [],
        }

    monkeypatch.setattr("toolkit.cli.inspect.config_ops.schema_diff_payload", _fake_payload)
    return {}


class TestInspectConfig:
    """inspect config — contratto base."""

    @pytest.mark.contract
    def test_help(self):
        """--help mostra i flag principali."""
        result = runner.invoke(app, ["inspect", "config", "--help"])
        output = _strip_ansi(result.stdout)
        assert result.exit_code == 0
        assert "--mode" in output
        assert "--layer" in output
        assert "--diff" in output
        assert "--config" in output

    @pytest.mark.contract
    def test_help_json_flag(self):
        """--json e' documentato."""
        result = runner.invoke(app, ["inspect", "config", "--help"])
        output = _strip_ansi(result.stdout)
        assert "--json" in output

    @pytest.mark.contract
    @pytest.mark.parametrize("mode", ["schema", "preview", "profile", "sql"])
    def test_config_mode_json_output(self, mode, mock_layer_query):
        """Ogni mode produce JSON parsabile."""
        args = ["inspect", "config", "--mode", mode, "-c", CONFIG_PATH, "--json"]
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

    @pytest.mark.contract
    def test_config_diff_json(self, mock_schema_diff):
        """--diff --json produce JSON parsabile."""
        result = runner.invoke(app, ["inspect", "config", "--diff", "-c", CONFIG_PATH, "--json"])
        assert result.exit_code == 0, result.stdout
        data = json.loads(result.stdout)
        assert isinstance(data, dict)
        assert "dataset" in data

    @pytest.mark.contract
    def test_config_human_output(self, mock_layer_query):
        """Output testo (senza --json) deve funzionare."""
        result = runner.invoke(app, ["inspect", "config", "--mode", "schema", "-c", CONFIG_PATH])
        assert result.exit_code == 0, result.stdout
        assert "Dataset" in result.stdout
        assert "test" in result.stdout

    @pytest.mark.contract
    def test_config_human_preview(self, mock_layer_query):
        """Output testo mode=preview."""
        result = runner.invoke(app, ["inspect", "config", "--mode", "preview", "-c", CONFIG_PATH])
        assert result.exit_code == 0, result.stdout
        assert "Righe" in result.stdout

    @pytest.mark.contract
    def test_config_human_profile(self, mock_layer_query):
        """Output testo mode=profile."""
        result = runner.invoke(
            app, ["inspect", "config", "--mode", "profile", "-l", "raw", "-c", CONFIG_PATH]
        )
        assert result.exit_code == 0, result.stdout
        assert "Encoding" in result.stdout

    @pytest.mark.contract
    def test_config_human_sql(self, mock_layer_query):
        """Output testo mode=sql."""
        result = runner.invoke(
            app, ["inspect", "config", "--mode", "sql", "-c", CONFIG_PATH, "--sql", "SELECT 1"]
        )
        assert result.exit_code == 0, result.stdout
        assert "SQL" in result.stdout

    @pytest.mark.contract
    def test_config_missing_arg_fails(self):
        """Senza --config deve fallire."""
        result = runner.invoke(app, ["inspect", "config", "--mode", "schema"])
        assert result.exit_code != 0


class TestInspectSummary:
    """inspect (default) — summary."""

    @pytest.mark.contract
    def test_help(self):
        """--help mostra i flag principali."""
        result = runner.invoke(app, ["inspect", "--help"])
        output = _strip_ansi(result.stdout)
        assert result.exit_code == 0
        assert "--config" in output
        assert "--year" in output
        assert "--json" in output
        assert "config" in output
        assert "runs" in output

    @pytest.mark.contract
    def test_missing_config_fails(self, tmp_path):
        """Fuori da un dataset deve fallire."""
        import os

        old = os.getcwd()
        os.chdir(tmp_path)
        try:
            result = runner.invoke(app, ["inspect"])
            assert result.exit_code != 0
        finally:
            os.chdir(old)


class TestInspectRuns:
    """inspect runs — contratto base."""

    @pytest.mark.contract
    def test_help(self):
        """--help mostra i flag principali."""
        result = runner.invoke(app, ["inspect", "runs", "--help"])
        output = _strip_ansi(result.stdout)
        assert result.exit_code == 0
        assert "--config" in output
        assert "--resume" in output
        assert "--run-id" in output
        assert "--limit" in output

    @pytest.mark.contract
    def test_missing_config_fails(self):
        """Senza --config deve fallire."""
        result = runner.invoke(app, ["inspect", "runs"])
        assert result.exit_code != 0

    @pytest.mark.contract
    def test_nonexistent_run_id_fails(self, tmp_path):
        """--run-id inesistente deve fallire con errore."""
        cfg = tmp_path / "dataset.yml"
        cfg.write_text(
            "dataset:\n  name: test\n  years: [2024]\n"
            "raw:\n  sources:\n    - type: local_file\n      args:\n        path: dummy.csv\n"
            "clean:\n  sql: dummy.sql\n"
            "mart:\n  tables: []\n",
            encoding="utf-8",
        )
        result = runner.invoke(app, ["inspect", "runs", "-c", str(cfg), "--run-id", "nonexistent"])
        assert result.exit_code != 0


class TestInspectTopLevel:
    """inspect help — mostra opzioni e subcomandi."""

    @pytest.mark.contract
    def test_inspect_help_shows_modes(self):
        """inspect --help mostra opzioni e subcomandi."""
        result = runner.invoke(app, ["inspect", "--help"])
        output = _strip_ansi(result.stdout)
        assert result.exit_code == 0
        assert "--config" in output
        assert "--year" in output
        assert "--json" in output
        assert "config" in output
        assert "runs" in output


class TestAutoDetect:
    """auto-detect config — --config opzionale."""

    @pytest.mark.contract
    def test_missing_config_fails_from_empty_dir(self, tmp_path):
        """Senza dataset.yml nella CWD deve fallire con errore chiaro."""
        import os

        old = os.getcwd()
        os.chdir(tmp_path)
        try:
            result = runner.invoke(app, ["inspect"])
            assert result.exit_code != 0
            assert "non trovato" in result.output
        finally:
            os.chdir(old)

    @pytest.mark.contract
    def test_auto_detect_from_cwd(self, project_example):
        """Con dataset.yml nella CWD, --config non serve."""
        import os

        old = os.getcwd()
        os.chdir(project_example)
        try:
            # run --dry-run deve trovare dataset.yml nella CWD
            result = runner.invoke(app, ["run", "--dry-run"])
            assert result.exit_code == 0, result.output
            assert "Execution Plan" in result.output
        finally:
            os.chdir(old)

    @pytest.mark.contract
    def test_nonexistent_slug_fails(self):
        """Slug inesistente deve fallire con errore chiaro."""
        result = runner.invoke(app, ["inspect", "-c", "this-slug-definitely-does-not-exist"])
        assert result.exit_code != 0
        assert "Nessun dataset trovato" in result.output
