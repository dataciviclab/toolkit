from __future__ import annotations

import textwrap
from pathlib import Path

import shutil
from typer.testing import CliRunner

from toolkit.cli.app import app


FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(content).strip() + "\n", encoding="utf-8")


def _write_batch_project(project_dir: Path, dataset: str, year: int) -> Path:
    (project_dir / "data").mkdir(parents=True, exist_ok=True)
    shutil.copy(FIXTURES_DIR / "it_small.csv", project_dir / "data" / "it_small.csv")

    _write_text(
        project_dir / "sql" / "clean.sql",
        """
        SELECT
          comune,
          CAST(anno AS INTEGER) AS anno,
          CAST(valore AS DOUBLE) AS valore
        FROM raw_input
        """,
    )
    _write_text(
        project_dir / "sql" / "mart.sql",
        """
        SELECT
          anno,
          SUM(valore) AS totale
        FROM clean_input
        GROUP BY anno
        """,
    )
    _write_text(
        project_dir / "dataset.yml",
        f"""
        schema_version: 1
        root: out
        dataset:
          name: {dataset}
          years: [{year}]
        raw:
          output_policy: overwrite
          sources:
            - name: csv_it
              type: local_file
              primary: "true"
              args:
                path: data/it_small.csv
                filename: {dataset}_{year}.csv
        clean:
          sql: sql/clean.sql
          read_mode: strict
          read:
            source: config_only
            header: true
            delim: ";"
            decimal: ","
            mode: explicit
            include: {dataset}_{year}.csv
          required_columns: comune
          validate:
            not_null: valore
        mart:
          tables:
            - name: mart_totali
              sql: sql/mart.sql
          required_tables: mart_totali
          validate:
            table_rules:
              mart_totali:
                required_columns: [anno, totale]
        """,
    )
    return project_dir / "dataset.yml"


def test_batch_runs_configs_in_sequence_and_prints_report(tmp_path: Path) -> None:
    project_a = tmp_path / "project_a"
    project_b = tmp_path / "project_b"
    _write_batch_project(project_a, "batch_a", 2022)
    _write_batch_project(project_b, "batch_b", 2023)

    configs_file = tmp_path / "configs.txt"
    configs_file.write_text(
        "\n".join(
            [
                "project_a/dataset.yml",
                "project_b/dataset.yml",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["batch", "--configs", str(configs_file), "--step", "all"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    assert "Batch Report" in result.output
    assert "dataset" in result.output
    assert "years" in result.output
    assert "status" in result.output
    assert "batch_a" in result.output
    assert "batch_b" in result.output
    assert "SUCCESS" in result.output

    assert (project_a / "out" / "data" / "mart" / "batch_a" / "2022" / "mart_totali.parquet").exists()
    assert (project_b / "out" / "data" / "mart" / "batch_b" / "2023" / "mart_totali.parquet").exists()
