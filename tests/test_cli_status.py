from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from toolkit.core.run_context import get_run_dir
from toolkit.cli.app import app


def _write_run_record(path: Path, run_id: str, started_at: str, status: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "dataset": "demo_ds",
                "year": 2022,
                "run_id": run_id,
                "started_at": started_at,
                "finished_at": None,
                "status": status,
                "layers": {
                    "raw": {"status": "SUCCESS", "started_at": started_at, "finished_at": started_at},
                    "clean": {"status": "FAILED", "started_at": started_at, "finished_at": started_at},
                    "mart": {"status": "PENDING", "started_at": None, "finished_at": None},
                },
                "validations": {
                    "raw": {"passed": True, "errors_count": 0, "warnings_count": 1, "checks": []},
                    "clean": {"passed": False, "errors_count": 2, "warnings_count": 0, "checks": []},
                    "mart": {},
                },
                "error": "clean validation failed" if status == "FAILED" else None,
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def test_status_uses_same_run_dir_as_writer(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    config_path = project_dir / "dataset.yml"

    config_path.write_text(
        """
root: "./out"
dataset:
  name: demo_ds
  years: [2022]
raw: {}
clean:
  sql: "sql/clean.sql"
mart:
  tables:
    - name: mart_example
      sql: "sql/mart/mart_example.sql"
""".strip(),
        encoding="utf-8",
    )

    sql_dir = project_dir / "sql" / "mart"
    sql_dir.mkdir(parents=True, exist_ok=True)
    (project_dir / "sql" / "clean.sql").write_text("select 1 as value", encoding="utf-8")
    (sql_dir / "mart_example.sql").write_text("select * from clean_input", encoding="utf-8")

    monkeypatch.chdir(tmp_path)
    runner = CliRunner()

    run_result = runner.invoke(app, ["run", "all", "--config", str(config_path), "--dry-run"])
    assert run_result.exit_code == 0

    run_dir = get_run_dir(project_dir / "out", "demo_ds", 2022)
    records = sorted(run_dir.glob("*.json"))
    assert len(records) == 1
    written_run_id = records[0].stem

    result = runner.invoke(
        app,
        ["status", "--dataset", "demo_ds", "--year", "2022", "--latest", "--config", str(config_path)],
    )

    assert result.exit_code == 0
    assert f"run_id: {written_run_id}" in result.output
    assert "status: DRY_RUN" in result.output
