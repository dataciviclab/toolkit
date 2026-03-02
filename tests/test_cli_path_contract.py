from __future__ import annotations

import json
import shutil
from pathlib import Path

from typer.testing import CliRunner

from toolkit.cli.app import app


def _copy_project_example(dst: Path) -> Path:
    src = Path("project-example")
    shutil.copytree(src, dst)
    shutil.rmtree(dst / "_smoke_out", ignore_errors=True)
    return dst / "dataset.yml"


def _write_failed_run_record(path: Path, run_id: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "dataset": "project_example",
                "year": 2022,
                "run_id": run_id,
                "started_at": "2026-03-01T10:00:00+00:00",
                "finished_at": "2026-03-01T10:01:00+00:00",
                "status": "FAILED",
                "layers": {
                    "raw": {"status": "SUCCESS", "started_at": "2026-03-01T10:00:00+00:00", "finished_at": "2026-03-01T10:00:10+00:00"},
                    "clean": {"status": "FAILED", "started_at": "2026-03-01T10:00:10+00:00", "finished_at": "2026-03-01T10:00:20+00:00"},
                    "mart": {"status": "PENDING", "started_at": None, "finished_at": None},
                },
                "validations": {"raw": {}, "clean": {}, "mart": {}},
                "resumed_from": None,
                "error": "clean failed",
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def test_cli_dry_run_resolves_sql_from_config_dir_not_cwd(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project-example"
    config_path = _copy_project_example(project_dir)

    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["run", "all", "--config", str(config_path), "--dry-run", "--strict-config"],
    )

    assert result.exit_code == 0
    assert "Execution Plan" in result.output
    assert "steps: raw, clean, mart" in result.output


def test_cli_commands_use_dataset_yml_dir_as_path_base(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project-example"
    config_path = _copy_project_example(project_dir)

    monkeypatch.chdir(tmp_path)
    runner = CliRunner()

    run_result = runner.invoke(app, ["run", "all", "--config", str(config_path), "--strict-config"])
    assert run_result.exit_code == 0, run_result.output

    validate_result = runner.invoke(
        app,
        ["validate", "all", "--config", str(config_path), "--strict-config"],
    )
    assert validate_result.exit_code == 0, validate_result.output

    profile_result = runner.invoke(
        app,
        ["profile", "raw", "--config", str(config_path), "--strict-config"],
    )
    assert profile_result.exit_code == 0, profile_result.output

    status_result = runner.invoke(
        app,
        [
            "status",
            "--dataset",
            "project_example",
            "--year",
            "2022",
            "--latest",
            "--config",
            str(config_path),
            "--strict-config",
        ],
    )
    assert status_result.exit_code == 0, status_result.output
    assert "status: SUCCESS" in status_result.output

    root = project_dir / "_smoke_out"
    raw_dir = root / "data" / "raw" / "project_example" / "2022"
    clean_dir = root / "data" / "clean" / "project_example" / "2022"
    mart_dir = root / "data" / "mart" / "project_example" / "2022"

    assert (raw_dir / "ispra_dettaglio_comunale_2022.csv").exists()
    assert (raw_dir / "_profile" / "suggested_read.yml").exists()
    assert (clean_dir / "project_example_2022_clean.parquet").exists()
    assert (mart_dir / "rd_by_regione.parquet").exists()
    assert (mart_dir / "rd_by_provincia.parquet").exists()


def test_cli_resume_from_other_cwd_falls_back_and_reuses_relative_paths(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project-example"
    config_path = _copy_project_example(project_dir)
    runs_dir = project_dir / "_smoke_out" / "data" / "_runs" / "project_example" / "2022"
    failed_run_id = "failed-run"
    _write_failed_run_record(runs_dir / f"{failed_run_id}.json", failed_run_id)

    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "resume",
            "--dataset",
            "project_example",
            "--year",
            "2022",
            "--run-id",
            failed_run_id,
            "--config",
            str(config_path),
            "--strict-config",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Falling back to 'raw'" in result.output
    assert "starting at raw" in result.output

    root = project_dir / "_smoke_out"
    assert (root / "data" / "raw" / "project_example" / "2022" / "ispra_dettaglio_comunale_2022.csv").exists()
    assert (root / "data" / "clean" / "project_example" / "2022" / "project_example_2022_clean.parquet").exists()
    assert (root / "data" / "mart" / "project_example" / "2022" / "rd_by_regione.parquet").exists()
