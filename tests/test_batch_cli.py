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


def test_batch_with_years_filter(tmp_path: Path) -> None:
    """batch --years filtra gli anni processati."""
    project = tmp_path / "project"
    _write_batch_project(project, "multi_year", 2022)
    # Aggiunge un secondo anno alla config
    yml_path = project / "dataset.yml"
    content = yml_path.read_text(encoding="utf-8")
    content = content.replace("years: [2022]", "years: [2022, 2023]")
    yml_path.write_text(content, encoding="utf-8")

    configs_file = tmp_path / "configs.txt"
    configs_file.write_text("project/dataset.yml\n", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["batch", "--configs", str(configs_file), "--step", "all", "--years", "2022"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.output
    assert "Batch Report" in result.output
    # Solo 2022 processato, non 2023
    assert "2022" in result.output
    assert "2023" not in result.output
    assert (project / "out" / "data" / "mart" / "multi_year" / "2022" / "mart_totali.parquet").exists()
    # 2023 non deve essere stato processato
    assert not (project / "out" / "data" / "mart" / "multi_year" / "2023" / "mart_totali.parquet").exists()


def test_batch_with_validate(tmp_path: Path) -> None:
    """batch --validate esegue validazione dopo ogni run."""
    project = tmp_path / "project"
    _write_batch_project(project, "validated", 2024)

    configs_file = tmp_path / "configs.txt"
    configs_file.write_text("project/dataset.yml\n", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["batch", "--configs", str(configs_file), "--step", "all", "--validate"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.output
    assert "Batch Report" in result.output
    assert "validate" in result.output
    assert "passed" in result.output

    # I file di validazione devono esistere
    root = project / "out"
    year = 2024
    assert (root / "data" / "raw" / "validated" / str(year) / "raw_validation.json").exists()
    assert (root / "data" / "clean" / "validated" / str(year) / "_validate" / "clean_validation.json").exists()
    assert (root / "data" / "mart" / "validated" / str(year) / "_validate" / "mart_validation.json").exists()


def test_batch_validate_respects_step(tmp_path: Path) -> None:
    """batch --validate --step raw valida solo raw, non clean/mart."""
    project = tmp_path / "project"
    _write_batch_project(project, "step_raw", 2024)

    configs_file = tmp_path / "configs.txt"
    configs_file.write_text("project/dataset.yml\n", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["batch", "--configs", str(configs_file), "--step", "raw", "--validate"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.output
    root = project / "out"
    year = 2024
    # Raw validation esiste
    assert (root / "data" / "raw" / "step_raw" / str(year) / "raw_validation.json").exists()
    # Clean/mart validation NON deve esistere (non richiesti da --step raw)
    assert not (root / "data" / "clean" / "step_raw" / str(year) / "_validate" / "clean_validation.json").exists()
    assert not (root / "data" / "mart" / "step_raw" / str(year) / "_validate" / "mart_validation.json").exists()


def test_batch_validate_failure_exits_nonzero(tmp_path: Path, monkeypatch) -> None:
    """batch --validate esce con codice 1 se una validazione fallisce."""
    from toolkit.clean import validate as clean_validate

    project = tmp_path / "project"
    _write_batch_project(project, "failing", 2024)

    configs_file = tmp_path / "configs.txt"
    configs_file.write_text("project/dataset.yml\n", encoding="utf-8")

    # Forza run_clean_validation a tornare failed.
    # Il nome locale in cmd_batch e' un alias: va patchato per nome qualificato.
    original = clean_validate.run_clean_validation

    def mock_validate(cfg, year, logger):
        result = original(cfg, year, logger)
        result["passed"] = False
        return result

    monkeypatch.setattr("toolkit.clean.validate.run_clean_validation", mock_validate)
    monkeypatch.setattr("toolkit.cli.cmd_batch.run_clean_validation", mock_validate)

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["batch", "--configs", str(configs_file), "--step", "all", "--validate"],
        catch_exceptions=False,
    )

    assert result.exit_code == 1, f"expected exit 1, got {result.exit_code}: {result.output}"
    assert "Failures" in result.output
    assert "validation failed" in result.output
    assert "failing" in result.output
