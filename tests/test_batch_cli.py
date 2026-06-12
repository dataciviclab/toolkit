from __future__ import annotations

import json
import textwrap
from pathlib import Path

import pytest
import shutil
from typer.testing import CliRunner

from toolkit.cli.app import app

pytestmark = pytest.mark.contract

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


def _write_configs_file(tmp_path: Path, *project_names: str) -> Path:
    configs_file = tmp_path / "configs.txt"
    configs_file.write_text(
        "\n".join(f"{p}/dataset.yml" for p in project_names) + "\n",
        encoding="utf-8",
    )
    return configs_file


def test_batch_runs_configs_in_sequence_and_prints_report(tmp_path: Path) -> None:
    project_a = tmp_path / "project_a"
    project_b = tmp_path / "project_b"
    _write_batch_project(project_a, "batch_a", 2022)
    _write_batch_project(project_b, "batch_b", 2023)

    configs_file = _write_configs_file(tmp_path, "project_a", "project_b")

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

    assert (
        project_a / "out" / "data" / "mart" / "batch_a" / "2022" / "mart_totali.parquet"
    ).exists()
    assert (
        project_b / "out" / "data" / "mart" / "batch_b" / "2023" / "mart_totali.parquet"
    ).exists()


def test_batch_smoke_flag(tmp_path: Path) -> None:
    """--smoke usa root/smoke/ come output e sample_rows=1000."""
    project = tmp_path / "project"
    _write_batch_project(project, "batch_smoke", 2023)
    configs_file = _write_configs_file(tmp_path, "project")

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["batch", "--configs", str(configs_file), "--step", "all", "--smoke"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    assert "Batch Report" in result.output
    assert "batch_smoke" in result.output
    assert "SUCCESS" in result.output

    # Smoke NON scrive in out/data/ (dati reali)
    data_clean = project / "out" / "data" / "clean" / "batch_smoke" / "2023"
    assert not data_clean.exists(), "smoke must NOT write to out/data/"

    # Smoke scrive in out/smoke/data/
    smoke_clean = (
        project
        / "out"
        / "smoke"
        / "data"
        / "clean"
        / "batch_smoke"
        / "2023"
        / "batch_smoke_2023_clean.parquet"
    )
    assert smoke_clean.exists(), f"smoke clean output not found: {smoke_clean}"
    smoke_mart = (
        project / "out" / "smoke" / "data" / "mart" / "batch_smoke" / "2023" / "mart_totali.parquet"
    )
    assert smoke_mart.exists(), f"smoke mart output not found: {smoke_mart}"


def test_batch_dry_run_flag(tmp_path: Path) -> None:
    """--dry-run stampa il piano ma non crea file (usa --step raw per evitare
    la validazione SQL dry-run che ha una limitazione DuckDB con alias)."""
    project = tmp_path / "project"
    _write_batch_project(project, "batch_dry", 2023)
    configs_file = _write_configs_file(tmp_path, "project")

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["batch", "--configs", str(configs_file), "--step", "raw", "--dry-run"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    assert "batch_dry" in result.output
    assert "DRY_RUN" in result.output

    # --dry-run non deve creare file di output
    raw_out = project / "out" / "data" / "raw" / "batch_dry" / "2023"
    assert not raw_out.exists(), f"dry-run should not create output directory: {raw_out}"


def test_batch_json_output(tmp_path: Path) -> None:
    """--json produce output JSON puro su stdout (log silenziato)."""
    project = tmp_path / "project"
    _write_batch_project(project, "batch_json", 2023)
    configs_file = _write_configs_file(tmp_path, "project")

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["batch", "--configs", str(configs_file), "--step", "all", "--json"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    # stdout deve essere JSON puro, parsabile direttamente
    report = json.loads(result.output)
    assert report["summary"]["total"] == 1
    assert report["summary"]["passed"] == 1
    assert report["rows"][0]["dataset"] == "batch_json"
    assert report["rows"][0]["status"] == "SUCCESS"


def test_batch_step_probe(tmp_path: Path) -> None:
    """--step probe esegue probe (salta local_file) e produce report JSON."""
    project = tmp_path / "project"
    _write_batch_project(project, "batch_probe", 2023)
    configs_file = _write_configs_file(tmp_path, "project")

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["batch", "--configs", str(configs_file), "--step", "probe"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    assert "Batch Report" in result.output
    assert "batch_probe" in result.output
    assert "SUCCESS" in result.output


def test_batch_step_probe_json_output(tmp_path: Path) -> None:
    """--step probe --json produce JSON valido."""
    project = tmp_path / "project"
    _write_batch_project(project, "batch_probe_json", 2023)
    configs_file = _write_configs_file(tmp_path, "project")

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["batch", "--configs", str(configs_file), "--step", "probe", "--json"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    report = json.loads(result.output)
    assert report["summary"]["total"] == 1
    assert report["summary"]["passed"] == 1
    assert report["rows"][0]["dataset"] == "batch_probe_json"
    assert report["rows"][0]["step"] == "probe"
    assert report["rows"][0]["status"] == "SUCCESS"


def test_batch_dry_run_with_json(tmp_path: Path) -> None:
    """--dry-run --json: stdout JSON puro (execution plan silenziato)."""
    project = tmp_path / "project"
    _write_batch_project(project, "batch_dry_json", 2023)
    configs_file = _write_configs_file(tmp_path, "project")

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["batch", "--configs", str(configs_file), "--step", "raw", "--dry-run", "--json"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    # stdout deve essere JSON puro, parsabile direttamente
    report = json.loads(result.output)
    assert report["summary"]["total"] == 1
    assert report["summary"]["passed"] == 1
    assert report["rows"][0]["dataset"] == "batch_dry_json"
    assert report["rows"][0]["status"] == "DRY_RUN"

    # Nessun file creato (dry-run)
    raw_out = project / "out" / "data" / "raw" / "batch_dry_json" / "2023"
    assert not raw_out.exists()


@pytest.mark.policy
def test_batch_step_probe_shared_pool(monkeypatch, tmp_path: Path) -> None:
    """Batch --step probe riusa lo stesso ProbePool tra config diversi.

    Il circuit breaker deve persistere tra le probe di piu' dataset.
    """
    pools_seen: list[object] = []

    def _tracking_run_probe(cfg, year, logger, pool=None):
        if pool is not None:
            pools_seen.append(pool)

    monkeypatch.setattr(
        "toolkit.cli.cmd_run._run_probe",
        _tracking_run_probe,
    )

    # Scrivi due progetti distinti
    project_a = tmp_path / "proj_a"
    _write_batch_project(project_a, "batch_pool_a", 2023)
    project_b = tmp_path / "proj_b"
    _write_batch_project(project_b, "batch_pool_b", 2024)

    configs_file = tmp_path / "configs.txt"
    configs_file.write_text(
        f"{project_a}/dataset.yml\n{project_b}/dataset.yml\n", encoding="utf-8"
    )

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["batch", "--configs", str(configs_file), "--step", "probe"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    # Devono aver ricevuto lo stesso pool (stessa identita')
    assert len(pools_seen) >= 2, (
        f"Attese almeno 2 chiamate a _run_probe con pool, ricevute {len(pools_seen)}"
    )
    first_pool = pools_seen[0]
    for i, p in enumerate(pools_seen):
        assert p is first_pool, (
            f"Pool alla chiamata {i} ha identita' diversa: "
            f"ci si aspetta che batch riusi lo stesso ProbePool"
        )
