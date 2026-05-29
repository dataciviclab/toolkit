from __future__ import annotations

import json
from pathlib import Path

import pytest
from toolkit.core.run_records import get_run_dir

from toolkit.cli.app import app
from tests.helpers import make_dataset_yml, make_standard_sql

pytestmark = [pytest.mark.contract, pytest.mark.core]


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
                    "raw": {
                        "status": "SUCCESS",
                        "started_at": started_at,
                        "finished_at": started_at,
                    },
                    "clean": {
                        "status": "FAILED",
                        "started_at": started_at,
                        "finished_at": started_at,
                    },
                    "mart": {"status": "PENDING", "started_at": None, "finished_at": None},
                },
                "validations": {
                    "raw": {"passed": True, "errors_count": 0, "warnings_count": 1, "checks": []},
                    "clean": {
                        "passed": False,
                        "errors_count": 2,
                        "warnings_count": 0,
                        "checks": [],
                    },
                    "mart": {},
                },
                "error": "clean validation failed" if status == "FAILED" else None,
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def test_status_uses_same_run_dir_as_writer(
    tmp_path: Path, runner, chdir_tmp: Path
) -> None:
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    config_path = make_dataset_yml(
        project_dir / "dataset.yml",
        name="demo_ds",
        root=project_dir / "out",
        mart_tables=[("mart_example", "sql/mart/mart_example.sql")],
    )
    make_standard_sql(project_dir)

    run_result = runner.invoke(
        app, ["run", "all", "--config", str(config_path), "--dry-run"]
    )
    assert run_result.exit_code == 0

    run_dir = get_run_dir(project_dir / "out", "demo_ds", 2022)
    records = sorted(run_dir.glob("*.json"))
    assert len(records) == 1
    written_run_id = records[0].stem

    result = runner.invoke(
        app,
        [
            "status",
            "--dataset",
            "demo_ds",
            "--year",
            "2022",
            "--latest",
            "--config",
            str(config_path),
        ],
    )

    assert result.exit_code == 0
    assert f"run_id: {written_run_id}" in result.output
    assert "status: DRY_RUN" in result.output


def test_status_reports_raw_hints_when_raw_artifacts_exist(
    tmp_path: Path, runner, chdir_tmp: Path
) -> None:
    project_dir = tmp_path / "project"
    raw_dir = project_dir / "out" / "data" / "raw" / "demo_ds" / "2022"
    raw_dir.mkdir(parents=True)
    (raw_dir / "_profile").mkdir(parents=True)
    config_path = make_dataset_yml(
        project_dir / "dataset.yml",
        name="demo_ds",
        root=project_dir / "out",
        mart_tables=[("mart_example", "sql/mart/mart_example.sql")],
    )
    make_standard_sql(project_dir)

    raw_meta = {
        "primary_output_file": "demo.csv",
        "profile_hints": {
            "encoding_suggested": "utf-8",
            "delim_suggested": ";",
            "decimal_suggested": None,
            "skip_suggested": 1,
            "warnings": ["header_preamble_detected"],
        },
    }
    (raw_dir / "metadata.json").write_text(json.dumps(raw_meta, indent=2), encoding="utf-8")
    (raw_dir / "_profile" / "suggested_read.yml").write_text(
        'clean:\n  read:\n    delim: ";"\n', encoding="utf-8"
    )

    run_dir = get_run_dir(project_dir / "out", "demo_ds", 2022)
    _write_run_record(run_dir / "run-123.json", "run-123", "2026-03-04T10:00:00+00:00", "SUCCESS")

    result = runner.invoke(
        app,
        [
            "status",
            "--dataset",
            "demo_ds",
            "--year",
            "2022",
            "--latest",
            "--config",
            str(config_path),
        ],
    )

    assert result.exit_code == 0
    assert "raw_hints:" in result.output
    assert "primary_output_file: demo.csv" in result.output
    assert "suggested_read_exists: True" in result.output
    assert "encoding: utf-8" in result.output
    assert "delim: ;" in result.output
    assert "skip: 1" in result.output
    assert "header_preamble_detected" in result.output


def test_status_reports_validation_summary_from_layer_artifacts(
    tmp_path: Path, runner, chdir_tmp: Path
) -> None:
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    config_path = project_dir / "dataset.yml"

    config_path.write_text(
        """root: "./out"
dataset:
  name: demo_ds
  years: [2022]
raw: {}
clean:
  sql: "sql/clean.sql"
  required_columns: ["id", "value"]
mart:
  tables:
    - name: mart_ok
      sql: "sql/mart/mart_ok.sql"
    - name: multi_ok
      sql: "sql/multi/multi_ok.sql"
      years: [2022]
  required_tables: ["mart_ok", "mart_missing"]
""",
        encoding="utf-8",
    )

    sql_mart_dir = project_dir / "sql" / "mart"
    sql_multi_dir = project_dir / "sql" / "multi"
    sql_mart_dir.mkdir(parents=True, exist_ok=True)
    sql_multi_dir.mkdir(parents=True, exist_ok=True)
    (project_dir / "sql" / "clean.sql").write_text("select 1 as value", encoding="utf-8")
    (sql_mart_dir / "mart_ok.sql").write_text("select * from clean_input", encoding="utf-8")
    (sql_multi_dir / "multi_ok.sql").write_text("select * from clean_input", encoding="utf-8")

    clean_dir = project_dir / "out" / "data" / "clean" / "demo_ds" / "2022"
    mart_dir = project_dir / "out" / "data" / "mart" / "demo_ds" / "2022"
    multi_dir = project_dir / "out" / "data" / "mart" / "demo_ds"
    (clean_dir / "_validate").mkdir(parents=True, exist_ok=True)
    (mart_dir / "_validate").mkdir(parents=True, exist_ok=True)
    (multi_dir / "_validate").mkdir(parents=True, exist_ok=True)

    (clean_dir / "demo_ds_2022_clean.parquet").write_text("placeholder", encoding="utf-8")
    (multi_dir / "multi_ok.parquet").write_text("placeholder", encoding="utf-8")

    (clean_dir / "metadata.json").write_text(
        json.dumps(
            {
                "validation": "_validate/clean_validation.json",
                "summary": {"ok": True, "errors_count": 0, "warnings_count": 1},
                "outputs": [{"file": "demo_ds_2022_clean.parquet"}],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (clean_dir / "_validate" / "clean_validation.json").write_text(
        json.dumps(
            {
                "ok": True,
                "errors": [],
                "warnings": ["header_preamble_detected"],
                "summary": {
                    "required": ["id", "value"],
                    "columns": ["id"],
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    (mart_dir / "metadata.json").write_text(
        json.dumps(
            {
                "validation": "_validate/mart_validation.json",
                "summary": {"ok": False, "errors_count": 1, "warnings_count": 1},
                "outputs": [{"file": "mart_ok.parquet"}],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (mart_dir / "_validate" / "mart_validation.json").write_text(
        json.dumps(
            {
                "ok": False,
                "errors": ["Missing required MART tables: ['mart_missing']"],
                "warnings": [
                    "MART table_rules reference tables not declared in mart.tables: ['mart_extra']"
                ],
                "summary": {
                    "required_tables": ["mart_ok", "mart_missing"],
                    "tables": ["mart_ok"],
                    "per_table": {},
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    (multi_dir / "metadata.json").write_text(
        json.dumps(
            {
                "layer": "mart_multi_year",
                "tables": [{"name": "multi_ok", "years": [2022]}],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    run_dir = get_run_dir(project_dir / "out", "demo_ds", 2022)
    _write_run_record(run_dir / "run-123.json", "run-123", "2026-03-04T10:00:00+00:00", "FAILED")

    result = runner.invoke(
        app,
        [
            "status",
            "--dataset",
            "demo_ds",
            "--year",
            "2022",
            "--latest",
            "--config",
            str(config_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "validation_summary:" in result.output
    assert "clean: state=passed warnings=1 errors=0" in result.output
    assert "missing_columns=value" in result.output
    assert "mart: state=failed warnings=1 errors=1" in result.output
    assert "missing_tables=mart_missing" in result.output
    assert "multi_year_mart:" in result.output


def test_status_reports_layer_profiles_from_metadata(
    tmp_path: Path, runner, chdir_tmp: Path
) -> None:
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    config_path = make_dataset_yml(
        project_dir / "dataset.yml",
        name="demo_ds",
        root=project_dir / "out",
        mart_tables=[("mart_example", "sql/mart/mart_example.sql")],
    )
    make_standard_sql(project_dir)

    clean_dir = project_dir / "out" / "data" / "clean" / "demo_ds" / "2022"
    mart_dir = project_dir / "out" / "data" / "mart" / "demo_ds" / "2022"
    clean_dir.mkdir(parents=True, exist_ok=True)
    mart_dir.mkdir(parents=True, exist_ok=True)

    (clean_dir / "metadata.json").write_text(
        json.dumps(
            {
                "output_profile": {
                    "row_count": 120,
                    "columns": [
                        {"name": "id", "type": "BIGINT"},
                        {"name": "regione", "type": "VARCHAR"},
                    ],
                }
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (mart_dir / "metadata.json").write_text(
        json.dumps(
            {
                "clean_input_profile": {
                    "row_count": 120,
                    "columns": [
                        {"name": "id", "type": "BIGINT"},
                        {"name": "regione", "type": "VARCHAR"},
                    ],
                },
                "table_profiles": {
                    "mart_example": {
                        "row_count": 20,
                        "columns": [
                            {"name": "regione", "type": "VARCHAR"},
                            {"name": "totale", "type": "DOUBLE"},
                        ],
                    }
                },
                "transition_profiles": [
                    {
                        "target_name": "mart_example",
                        "source_row_count": 120,
                        "target_row_count": 20,
                        "added_columns": ["totale"],
                        "removed_columns": ["id"],
                        "type_changes": [],
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    run_dir = get_run_dir(project_dir / "out", "demo_ds", 2022)
    _write_run_record(run_dir / "run-123.json", "run-123", "2026-03-04T10:00:00+00:00", "SUCCESS")

    result = runner.invoke(
        app,
        [
            "status",
            "--dataset",
            "demo_ds",
            "--year",
            "2022",
            "--latest",
            "--config",
            str(config_path),
        ],
    )

    assert result.exit_code == 0
    assert "layer_profiles:" in result.output
    assert "clean_output: rows=120 columns=2 preview=id:BIGINT, regione:VARCHAR" in result.output
    assert (
        "mart_clean_input: rows=120 columns=2 preview=id:BIGINT, regione:VARCHAR" in result.output
    )
    assert "mart_example: rows=20 columns=2 preview=regione:VARCHAR, totale:DOUBLE" in result.output
    assert "rows 120 -> 20 added=1 removed=1 type_changes=0" in result.output
