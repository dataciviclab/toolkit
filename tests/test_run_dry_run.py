from __future__ import annotations

import json
import logging
from pathlib import Path

import duckdb
from typer.testing import CliRunner

from toolkit.cli.app import app
from toolkit.cli.cmd_run import run_year
from toolkit.core.config import load_config


def test_run_dry_run_prints_plan_and_creates_only_run_record(tmp_path: Path) -> None:
    sql_dir = tmp_path / "sql" / "mart"
    sql_dir.mkdir(parents=True, exist_ok=True)
    (tmp_path / "sql" / "clean.sql").write_text("select 1 as value", encoding="utf-8")
    (sql_dir / "mart_example.sql").write_text("select * from clean_input", encoding="utf-8")

    config_path = tmp_path / "dataset.yml"
    root_dir = tmp_path / "out"
    config_path.write_text(
        "\n".join(
            [
                f'root: "{root_dir.as_posix()}"',
                "dataset:",
                '  name: "demo_ds"',
                "  years: [2022]",
                "raw: {}",
                "clean:",
                '  sql: "sql/clean.sql"',
                "mart:",
                "  tables:",
                '    - name: "mart_example"',
                '      sql: "sql/mart/mart_example.sql"',
            ]
        ),
        encoding="utf-8",
    )

    runner = CliRunner()
    result = runner.invoke(app, ["run", "all", "--config", str(config_path), "--dry-run"])

    assert result.exit_code == 0
    assert "Execution Plan" in result.output
    assert "status: DRY_RUN" in result.output
    assert "steps: raw, clean, mart" in result.output
    assert "sql_validation: OK" in result.output

    runs_dir = root_dir / "data" / "_runs" / "demo_ds" / "2022"
    records = list(runs_dir.glob("*.json"))
    assert len(records) == 1

    record = json.loads(records[0].read_text(encoding="utf-8"))
    assert record["status"] == "DRY_RUN"
    assert record["layers"]["raw"]["status"] == "PENDING"

    assert not (root_dir / "data" / "raw" / "demo_ds" / "2022").exists()
    assert not (root_dir / "data" / "clean" / "demo_ds" / "2022").exists()
    assert not (root_dir / "data" / "mart" / "demo_ds" / "2022").exists()


def test_run_dry_run_fails_on_clean_sql_syntax_error(tmp_path: Path) -> None:
    sql_dir = tmp_path / "sql" / "mart"
    sql_dir.mkdir(parents=True, exist_ok=True)
    (tmp_path / "sql" / "clean.sql").write_text("select from raw_input", encoding="utf-8")
    (sql_dir / "mart_example.sql").write_text("select * from clean_input", encoding="utf-8")

    config_path = tmp_path / "dataset.yml"
    root_dir = tmp_path / "out"
    config_path.write_text(
        "\n".join(
            [
                f'root: "{root_dir.as_posix()}"',
                "dataset:",
                '  name: "demo_ds"',
                "  years: [2022]",
                "raw: {}",
                "clean:",
                '  sql: "sql/clean.sql"',
                "mart:",
                "  tables:",
                '    - name: "mart_example"',
                '      sql: "sql/mart/mart_example.sql"',
            ]
        ),
        encoding="utf-8",
    )

    runner = CliRunner()
    result = runner.invoke(app, ["run", "all", "--config", str(config_path), "--dry-run"])

    assert result.exit_code != 0
    assert "CLEAN SQL dry-run failed" in str(result.exception)


def test_run_dry_run_fails_on_mart_sql_binding_error(tmp_path: Path) -> None:
    sql_dir = tmp_path / "sql" / "mart"
    sql_dir.mkdir(parents=True, exist_ok=True)
    (tmp_path / "sql" / "clean.sql").write_text('select "x" as value from raw_input', encoding="utf-8")
    (sql_dir / "mart_example.sql").write_text("select missing_col from clean_input", encoding="utf-8")

    config_path = tmp_path / "dataset.yml"
    root_dir = tmp_path / "out"
    config_path.write_text(
        "\n".join(
            [
                f'root: "{root_dir.as_posix()}"',
                "dataset:",
                '  name: "demo_ds"',
                "  years: [2022]",
                "raw: {}",
                "clean:",
                '  sql: "sql/clean.sql"',
                "  read:",
                "    columns:",
                '      x: "VARCHAR"',
                "mart:",
                "  tables:",
                '    - name: "mart_example"',
                '      sql: "sql/mart/mart_example.sql"',
            ]
        ),
        encoding="utf-8",
    )

    runner = CliRunner()
    result = runner.invoke(app, ["run", "all", "--config", str(config_path), "--dry-run"])

    assert result.exit_code != 0
    assert "MART SQL dry-run failed" in str(result.exception)


def test_run_dry_run_accepts_unquoted_raw_columns_without_read_columns(tmp_path: Path) -> None:
    sql_dir = tmp_path / "sql" / "mart"
    sql_dir.mkdir(parents=True, exist_ok=True)
    (tmp_path / "sql" / "clean.sql").write_text("select x as value from raw_input", encoding="utf-8")
    (sql_dir / "mart_example.sql").write_text("select * from clean_input", encoding="utf-8")

    config_path = tmp_path / "dataset.yml"
    root_dir = tmp_path / "out"
    config_path.write_text(
        "\n".join(
            [
                f'root: "{root_dir.as_posix()}"',
                "dataset:",
                '  name: "demo_ds"',
                "  years: [2022]",
                "raw: {}",
                "clean:",
                '  sql: "sql/clean.sql"',
                "mart:",
                "  tables:",
                '    - name: "mart_example"',
                '      sql: "sql/mart/mart_example.sql"',
            ]
        ),
        encoding="utf-8",
    )

    runner = CliRunner()
    result = runner.invoke(app, ["run", "all", "--config", str(config_path), "--dry-run"])

    assert result.exit_code == 0
    assert "sql_validation: OK" in result.output


def test_run_year_logs_effective_root_context(tmp_path: Path, caplog) -> None:
    sql_dir = tmp_path / "sql" / "mart"
    sql_dir.mkdir(parents=True, exist_ok=True)
    (tmp_path / "sql" / "clean.sql").write_text("select 1 as value", encoding="utf-8")
    (sql_dir / "mart_example.sql").write_text("select * from clean_input", encoding="utf-8")

    config_path = tmp_path / "dataset.yml"
    root_dir = tmp_path / "out"
    config_path.write_text(
        "\n".join(
            [
                f'root: "{root_dir.as_posix()}"',
                "dataset:",
                '  name: "demo_ds"',
                "  years: [2022]",
                "raw: {}",
                "clean:",
                '  sql: "sql/clean.sql"',
                "mart:",
                "  tables:",
                '    - name: "mart_example"',
                '      sql: "sql/mart/mart_example.sql"',
            ]
        ),
        encoding="utf-8",
    )

    cfg = load_config(config_path)
    logger = logging.getLogger("test.run_dry_run")
    logger.handlers = [caplog.handler]
    logger.propagate = False
    logger.setLevel(logging.INFO)

    with caplog.at_level(logging.INFO, logger="test.run_dry_run"):
        run_year(cfg, 2022, step="all", dry_run=True, logger=logger)

    assert "RUN context | dataset=demo_ds year=2022" in caplog.text
    assert f"base_dir={tmp_path}" in caplog.text
    assert f"effective_root={root_dir}" in caplog.text
    assert "root_source=yml" in caplog.text


def test_run_dry_run_accepts_mart_only_config(tmp_path: Path) -> None:
    mart_sql = tmp_path / "compose" / "sql"
    mart_sql.mkdir(parents=True, exist_ok=True)
    source_path = tmp_path / "external_source.parquet"

    con = duckdb.connect()
    con.execute("COPY (SELECT 1 AS value) TO ? (FORMAT PARQUET)", [str(source_path)])
    con.close()

    (mart_sql / "mart_example.sql").write_text(
        f"select * from read_parquet('{source_path.as_posix()}')",
        encoding="utf-8",
    )

    config_path = tmp_path / "compose" / "dataset.yml"
    root_dir = tmp_path / "out"
    config_path.write_text(
        "\n".join(
            [
                f'root: "{root_dir.as_posix()}"',
                "dataset:",
                '  name: "compose_demo"',
                "  years: [2022]",
                "raw: {}",
                "mart:",
                "  tables:",
                '    - name: "mart_example"',
                '      sql: "sql/mart_example.sql"',
            ]
        ),
        encoding="utf-8",
    )

    runner = CliRunner()
    result = runner.invoke(app, ["run", "mart", "--config", str(config_path), "--dry-run"])

    assert result.exit_code == 0
    assert "Execution Plan" in result.output
    assert "steps: mart" in result.output
    assert "sql_validation: OK" in result.output


def test_run_dry_run_all_fails_readably_on_mart_only_config(tmp_path: Path) -> None:
    mart_sql = tmp_path / "compose" / "sql"
    mart_sql.mkdir(parents=True, exist_ok=True)
    (mart_sql / "mart_example.sql").write_text("select 1 as value", encoding="utf-8")

    config_path = tmp_path / "compose" / "dataset.yml"
    root_dir = tmp_path / "out"
    config_path.write_text(
        "\n".join(
            [
                f'root: "{root_dir.as_posix()}"',
                "dataset:",
                '  name: "compose_demo"',
                "  years: [2022]",
                "raw: {}",
                "mart:",
                "  tables:",
                '    - name: "mart_example"',
                '      sql: "sql/mart_example.sql"',
            ]
        ),
        encoding="utf-8",
    )

    runner = CliRunner()
    result = runner.invoke(app, ["run", "all", "--config", str(config_path), "--dry-run"])

    assert result.exit_code != 0
    assert "run all is not supported for mart-only / compose-only configs" in str(result.exception)


def test_run_mart_executes_mart_only_config(tmp_path: Path) -> None:
    mart_sql = tmp_path / "compose" / "sql"
    mart_sql.mkdir(parents=True, exist_ok=True)
    source_path = tmp_path / "external_source.parquet"

    con = duckdb.connect()
    con.execute("COPY (SELECT 1 AS value) TO ? (FORMAT PARQUET)", [str(source_path)])
    con.close()

    (mart_sql / "mart_example.sql").write_text(
        f"select * from read_parquet('{source_path.as_posix()}')",
        encoding="utf-8",
    )

    config_path = tmp_path / "compose" / "dataset.yml"
    root_dir = tmp_path / "out"
    config_path.write_text(
        "\n".join(
            [
                f'root: "{root_dir.as_posix()}"',
                "dataset:",
                '  name: "compose_demo"',
                "  years: [2022]",
                "raw: {}",
                "mart:",
                "  tables:",
                '    - name: "mart_example"',
                '      sql: "sql/mart_example.sql"',
            ]
        ),
        encoding="utf-8",
    )

    runner = CliRunner()
    result = runner.invoke(app, ["run", "mart", "--config", str(config_path)])

    assert result.exit_code == 0
    mart_dir = root_dir / "data" / "mart" / "compose_demo" / "2022"
    assert (mart_dir / "mart_example.parquet").exists()
    assert (mart_dir / "metadata.json").exists()
    assert not (root_dir / "data" / "clean" / "compose_demo" / "2022").exists()


def test_run_all_fails_readably_on_mart_only_config(tmp_path: Path) -> None:
    mart_sql = tmp_path / "compose" / "sql"
    mart_sql.mkdir(parents=True, exist_ok=True)
    (mart_sql / "mart_example.sql").write_text("select 1 as value", encoding="utf-8")

    config_path = tmp_path / "compose" / "dataset.yml"
    root_dir = tmp_path / "out"
    config_path.write_text(
        "\n".join(
            [
                f'root: "{root_dir.as_posix()}"',
                "dataset:",
                '  name: "compose_demo"',
                "  years: [2022]",
                "raw: {}",
                "mart:",
                "  tables:",
                '    - name: "mart_example"',
                '      sql: "sql/mart_example.sql"',
            ]
        ),
        encoding="utf-8",
    )

    runner = CliRunner()
    result = runner.invoke(app, ["run", "all", "--config", str(config_path)])

    assert result.exit_code != 0
    assert "run all is not supported for mart-only / compose-only configs" in str(result.exception)
