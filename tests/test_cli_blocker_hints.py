"""Tests for toolkit blocker-hints CLI command.

DEPRECATED: delegato a review-readiness. Tests aggiornati per la nuova
implementazione che verifica leggibilita' parquet (non solo esistenza).

contract: blocker-hints CLI public interface (--json output format, exit codes)
policy: missing config is blocker (not warning); relative path resolution from config dir
"""

import json
from pathlib import Path

import duckdb
import pytest
from typer.testing import CliRunner

from toolkit.cli.app import app


def _write_real_parquet(path: Path) -> None:
    """Write a minimal real parquet file via DuckDB (non dummy text)."""
    conn = duckdb.connect()
    conn.execute(f"COPY (SELECT 1 AS id) TO '{path}' (FORMAT PARQUET)")
    conn.close()


# ---------------------------------------------------------------------------
# contract — CLI public interface
# ---------------------------------------------------------------------------

class TestBlockerHintsContract:
    """contract: blocker-hints --json output format and exit code contract."""

    @pytest.mark.contract
    def test_blocker_hints_returns_json_when_flag_set(self, tmp_path: Path, monkeypatch) -> None:
        """--json returns structured dict instead of human-readable output."""
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        config_path = project_dir / "dataset.yml"

        config_path.write_text(
            """
root: "./out"
dataset:
  name: test_ds
  years: [2023]
raw: {}
clean:
  sql: "sql/clean.sql"
mart:
  tables:
    - name: test_table
      sql: "sql/mart/test_table.sql"
            """.strip(),
            encoding="utf-8",
        )

        sql_dir = project_dir / "sql" / "mart"
        sql_dir.mkdir(parents=True, exist_ok=True)
        (project_dir / "sql" / "clean.sql").write_text("select 1 as value", encoding="utf-8")
        (sql_dir / "test_table.sql").write_text("select * from clean_input", encoding="utf-8")

        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("DATACIVICLAB_WORKSPACE", str(tmp_path))
        runner = CliRunner()

        result = runner.invoke(
            app,
            [
                "blocker-hints",
                "--config",
                str(config_path),
                "--year",
                "2023",
                "--json",
            ],
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert "dataset" in payload
        assert "config_path" in payload
        assert "year" in payload
        assert "blocker_count" in payload
        assert "warning_count" in payload
        assert "hints" in payload
        assert "hint_count" in payload

    @pytest.mark.contract
    def test_blocker_hints_exit_code_0_even_with_blockers(self, tmp_path: Path, monkeypatch) -> None:
        """Exit code 0 means hint generation succeeded — blockers are signalled in output, not exit code.

        Exit code 1 means config not found or unexpected error.
        """
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        config_path = project_dir / "dataset.yml"

        config_path.write_text(
            """
root: "./out"
dataset:
  name: test_ds
  years: [2023]
raw: {}
clean:
  sql: "sql/clean.sql"
mart:
  tables:
    - name: test_table
      sql: "sql/mart/test_table.sql"
            """.strip(),
            encoding="utf-8",
        )

        sql_dir = project_dir / "sql" / "mart"
        sql_dir.mkdir(parents=True, exist_ok=True)
        (project_dir / "sql" / "clean.sql").write_text("select 1 as value", encoding="utf-8")
        (sql_dir / "test_table.sql").write_text("select * from clean_input", encoding="utf-8")

        # Only mart dir exists (clean missing) — creates a blocker
        mart_dir = project_dir / "out" / "data" / "mart" / "test_ds" / "2023"
        mart_dir.mkdir(parents=True, exist_ok=True)
        (mart_dir / "manifest.json").write_text(
            json.dumps({"outputs": [{"file": "test_table.parquet"}]}, indent=2),
            encoding="utf-8",
        )

        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("DATACIVICLAB_WORKSPACE", str(tmp_path))
        runner = CliRunner()

        result = runner.invoke(
            app,
            [
                "blocker-hints",
                "--config",
                str(config_path),
                "--year",
                "2023",
            ],
        )

        assert result.exit_code == 0
        assert "blocker" in result.output.lower()

    @pytest.mark.contract
    def test_blocker_hints_help(self) -> None:
        """--help works without config."""
        import re

        runner = CliRunner()
        result = runner.invoke(app, ["blocker-hints", "--help"])
        assert result.exit_code == 0
        clean = re.sub(r"\x1b\[[0-9;]*[a-zA-Z]", "", result.output)
        assert "--config" in clean
        assert "--year" in clean
        assert "--json" in clean


# ---------------------------------------------------------------------------
# policy — non-obvious Lab rules
# ---------------------------------------------------------------------------

class TestBlockerHintsPolicy:
    """policy: missing config → error (not warning); path resolution from config dir."""

    @pytest.mark.policy
    def test_blocker_hints_missing_config(self) -> None:
        """Missing config file exits with code 1 and error message.

        Unlike other commands that fallback gracefully, blocker-hints
        requires a valid config — this is an explicit policy.
        """
        runner = CliRunner()
        result = runner.invoke(
            app,
            ["blocker-hints", "--config", "nonexistent.yml", "--year", "2023"],
        )
        assert result.exit_code == 1
        assert (
            "non trovata" in result.output.lower() or "not found" in result.output.lower()
        )

    @pytest.mark.policy
    def test_blocker_hints_detects_missing_layers(self, tmp_path: Path, monkeypatch) -> None:
        """policy: mart dir without clean/raw is a blocker (delegato a review-readiness)."""
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        config_path = project_dir / "dataset.yml"

        config_path.write_text(
            """
root: "./out"
dataset:
  name: test_ds
  years: [2023]
raw: {}
clean:
  sql: "sql/clean.sql"
mart:
  tables:
    - name: test_table
      sql: "sql/mart/test_table.sql"
            """.strip(),
            encoding="utf-8",
        )

        sql_dir = project_dir / "sql" / "mart"
        sql_dir.mkdir(parents=True, exist_ok=True)
        (project_dir / "sql" / "clean.sql").write_text("select 1 as value", encoding="utf-8")
        (sql_dir / "test_table.sql").write_text("select * from clean_input", encoding="utf-8")

        # Only mart dir exists, not clean/raw dirs
        mart_dir = project_dir / "out" / "data" / "mart" / "test_ds" / "2023"
        mart_dir.mkdir(parents=True, exist_ok=True)
        (mart_dir / "manifest.json").write_text(
            json.dumps({"outputs": [{"file": "test_table.parquet"}]}, indent=2),
            encoding="utf-8",
        )

        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("DATACIVICLAB_WORKSPACE", str(tmp_path))
        runner = CliRunner()

        result = runner.invoke(
            app,
            [
                "blocker-hints",
                "--config",
                str(config_path),
                "--year",
                "2023",
            ],
        )

        assert result.exit_code == 0
        # Delegates to review-readiness: reports missing raw + clean as blockers
        assert "blocker" in result.output.lower()
        assert result.output.count("blocker") >= 1

    @pytest.mark.policy
    def test_blocker_hints_resolves_relative_path_from_config_dir(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """policy: relative config paths resolve from the config file's parent dir, not cwd/WORKSPACE_ROOT.

        This ensures that `toolkit blocker-hints --config subdir/dataset.yml`
        works regardless of where the user runs the command from.
        """
        project_dir = tmp_path / "project" / "subdir"
        project_dir.mkdir(parents=True)
        config_path = project_dir / "dataset.yml"

        config_path.write_text(
            """
root: "./out"
dataset:
  name: test_ds
  years: [2023]
raw: {}
clean:
  sql: "sql/clean.sql"
mart:
  tables:
    - name: test_table
      sql: "sql/mart/test_table.sql"
            """.strip(),
            encoding="utf-8",
        )

        sql_dir = project_dir / "sql" / "mart"
        sql_dir.mkdir(parents=True, exist_ok=True)
        (project_dir / "sql" / "clean.sql").write_text("select 1 as value", encoding="utf-8")
        (sql_dir / "test_table.sql").write_text("select * from clean_input", encoding="utf-8")

        # All outputs exist
        raw_dir = project_dir / "out" / "data" / "raw" / "test_ds" / "2023"
        raw_dir.mkdir(parents=True, exist_ok=True)
        (raw_dir / "raw_data.csv").write_text("id,value\n1,100\n", encoding="utf-8")
        (raw_dir / "manifest.json").write_text(
            json.dumps({"primary_output_file": "raw_data.csv"}, indent=2),
            encoding="utf-8",
        )

        clean_dir = project_dir / "out" / "data" / "clean" / "test_ds" / "2023"
        clean_dir.mkdir(parents=True, exist_ok=True)
        _write_real_parquet(clean_dir / "test_ds_2023_clean.parquet")
        (clean_dir / "manifest.json").write_text(
            json.dumps({"outputs": [{"file": "test_ds_2023_clean.parquet"}]}, indent=2),
            encoding="utf-8",
        )

        mart_dir = project_dir / "out" / "data" / "mart" / "test_ds" / "2023"
        mart_dir.mkdir(parents=True, exist_ok=True)
        _write_real_parquet(mart_dir / "test_table.parquet")
        (mart_dir / "manifest.json").write_text(
            json.dumps({"outputs": [{"file": "test_table.parquet"}]}, indent=2),
            encoding="utf-8",
        )

        run_dir = project_dir / "out" / "data" / "_runs" / "test_ds" / "2023"
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "run-abc.json").write_text(
            json.dumps(
                {
                    "dataset": "test_ds",
                    "year": 2023,
                    "run_id": "run-abc",
                    "status": "SUCCESS",
                    "layers": {
                        "raw": {"status": "SUCCESS"},
                        "clean": {"status": "SUCCESS"},
                        "mart": {"status": "SUCCESS"},
                    },
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("DATACIVICLAB_WORKSPACE", str(tmp_path))
        runner = CliRunner()

        result = runner.invoke(
            app,
            ["blocker-hints", "--config", "project/subdir/dataset.yml", "--year", "2023"],
        )

        assert result.exit_code == 0, f"expected exit 0, got {result.exit_code}: {result.output}"
        assert "blockers: 0" in result.output

    @pytest.mark.contract
    def test_blocker_hints_no_blockers_when_all_present(self, tmp_path: Path, monkeypatch) -> None:
        """contract: no blockers when config and all outputs are consistent."""
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        config_path = project_dir / "dataset.yml"

        config_path.write_text(
            """
root: "./out"
dataset:
  name: test_ds
  years: [2023]
raw: {}
clean:
  sql: "sql/clean.sql"
mart:
  tables:
    - name: test_table
      sql: "sql/mart/test_table.sql"
            """.strip(),
            encoding="utf-8",
        )

        sql_dir = project_dir / "sql" / "mart"
        sql_dir.mkdir(parents=True, exist_ok=True)
        (project_dir / "sql" / "clean.sql").write_text("select 1 as value", encoding="utf-8")
        (sql_dir / "test_table.sql").write_text("select * from clean_input", encoding="utf-8")

        raw_dir = project_dir / "out" / "data" / "raw" / "test_ds" / "2023"
        raw_dir.mkdir(parents=True, exist_ok=True)
        (raw_dir / "raw_data.csv").write_text("id,value\n1,100\n", encoding="utf-8")
        (raw_dir / "manifest.json").write_text(
            json.dumps({"primary_output_file": "raw_data.csv"}, indent=2),
            encoding="utf-8",
        )

        clean_dir = project_dir / "out" / "data" / "clean" / "test_ds" / "2023"
        clean_dir.mkdir(parents=True, exist_ok=True)
        _write_real_parquet(clean_dir / "test_ds_2023_clean.parquet")
        (clean_dir / "manifest.json").write_text(
            json.dumps({"outputs": [{"file": "test_ds_2023_clean.parquet"}]}, indent=2),
            encoding="utf-8",
        )

        mart_dir = project_dir / "out" / "data" / "mart" / "test_ds" / "2023"
        mart_dir.mkdir(parents=True, exist_ok=True)
        _write_real_parquet(mart_dir / "test_table.parquet")
        (mart_dir / "manifest.json").write_text(
            json.dumps({"outputs": [{"file": "test_table.parquet"}]}, indent=2),
            encoding="utf-8",
        )

        run_dir = project_dir / "out" / "data" / "_runs" / "test_ds" / "2023"
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "run-abc.json").write_text(
            json.dumps(
                {
                    "dataset": "test_ds",
                    "year": 2023,
                    "run_id": "run-abc",
                    "status": "SUCCESS",
                    "layers": {
                        "raw": {"status": "SUCCESS"},
                        "clean": {"status": "SUCCESS"},
                        "mart": {"status": "SUCCESS"},
                    },
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("DATACIVICLAB_WORKSPACE", str(tmp_path))
        runner = CliRunner()

        result = runner.invoke(
            app,
            [
                "blocker-hints",
                "--config",
                str(config_path),
                "--year",
                "2023",
            ],
        )

        assert result.exit_code == 0
        assert "blockers: 0" in result.output
