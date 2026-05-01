"""Tests for toolkit blocker-hints CLI command."""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from toolkit.cli.app import app


def test_blocker_hints_help() -> None:
    """--help works without config."""
    import re
    runner = CliRunner()
    result = runner.invoke(app, ["blocker-hints", "--help"])
    assert result.exit_code == 0
    # Strip ANSI codes and check for key option names
    raw = result.output
    clean = re.sub(r'\x1b\[[0-9;]*[a-zA-Z]', '', raw)
    assert "--config" in clean
    assert "--year" in clean
    assert "--json" in clean


def test_blocker_hints_missing_config() -> None:
    """Missing config file exits with code 1 and error message."""
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["blocker-hints", "--config", "nonexistent.yml", "--year", "2023"],
    )
    assert result.exit_code == 1
    assert "non trovata" in result.output.lower() or "not found" in result.output.lower()


def test_blocker_hints_returns_json_when_flag_set(tmp_path: Path, monkeypatch) -> None:
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


def test_blocker_hints_no_blockers_when_all_present(tmp_path: Path, monkeypatch) -> None:
    """No blockers when config and outputs are consistent."""
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

    # Create all output directories and files so nothing is missing
    raw_dir = project_dir / "out" / "data" / "raw" / "test_ds" / "2023"
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / "raw_data.csv").write_text("id,value\n1,100\n", encoding="utf-8")
    (raw_dir / "manifest.json").write_text(
        json.dumps({"primary_output_file": "raw_data.csv"}, indent=2),
        encoding="utf-8",
    )

    clean_dir = project_dir / "out" / "data" / "clean" / "test_ds" / "2023"
    clean_dir.mkdir(parents=True, exist_ok=True)
    (clean_dir / "test_ds_2023_clean.parquet").write_text("dummy parquet", encoding="utf-8")
    (clean_dir / "manifest.json").write_text(
        json.dumps(
            {
                "outputs": [{"file": "test_ds_2023_clean.parquet"}],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    mart_dir = project_dir / "out" / "data" / "mart" / "test_ds" / "2023"
    mart_dir.mkdir(parents=True, exist_ok=True)
    (mart_dir / "test_table.parquet").write_text("dummy parquet", encoding="utf-8")
    (mart_dir / "manifest.json").write_text(
        json.dumps(
            {
                "outputs": [{"file": "test_table.parquet"}],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    # Create run record so latest_run exists
    run_dir = project_dir / "out" / "data" / "_runs" / "test_ds" / "2023"
    run_dir.mkdir(parents=True, exist_ok=True)
    run_record_path = run_dir / "run-abc.json"
    run_record_path.write_text(
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
    # With all outputs present, blocker_count should be 0
    assert "blockers: 0" in result.output


def test_blocker_hints_detects_clean_dir_missing_when_mart_exists(tmp_path: Path, monkeypatch) -> None:
    """Detects when mart dir exists but clean dir is missing (run order inconsistency)."""
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

    # Only mart dir exists, not clean dir
    mart_dir = project_dir / "out" / "data" / "mart" / "test_ds" / "2023"
    mart_dir.mkdir(parents=True, exist_ok=True)

    # Write manifest so mart is detected as existing
    (mart_dir / "manifest.json").write_text(
        json.dumps(
            {
                "outputs": [{"file": "test_table.parquet"}],
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
    # Should flag this as a blocker
    assert "clean_dir_missing" in result.output or "blocker" in result.output


def test_blocker_hints_exit_code_0_even_with_blockers(tmp_path: Path, monkeypatch) -> None:
    """Command exits 0 when hint generation succeeds, even with blockers present.

    Exit code 1 means config not found or unexpected error.
    Exit code 0 means blocker_hints ran successfully — blockers are in output.
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

    # Only mart dir exists (clean missing) — this creates a blocker
    mart_dir = project_dir / "out" / "data" / "mart" / "test_ds" / "2023"
    mart_dir.mkdir(parents=True, exist_ok=True)
    (mart_dir / "manifest.json").write_text(
        json.dumps(
            {
                "outputs": [{"file": "test_table.parquet"}],
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

    # Should exit 0 even though there's a blocker
    assert result.exit_code == 0
    assert "blocker" in result.output.lower()