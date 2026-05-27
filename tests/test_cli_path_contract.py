from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
from typer.testing import CliRunner

from toolkit.cli.app import app

pytestmark = pytest.mark.contract


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
    assert "steps: probe, raw, clean, mart" in result.output


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
        ["inspect", "profile", "--config", str(config_path), "--strict-config"],
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


def test_cli_version_flag() -> None:
    """contract: toolkit --version stampa versione ed esce con 0."""
    runner = CliRunner()
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0, result.output
    assert "toolkit " in result.output
    # Verifica che sia una versione semver valida (es. 1.9.1)
    version_part = result.output.strip().replace("toolkit ", "")
    parts = version_part.split(".")
    assert len(parts) == 3, f"versione non semver: {version_part}"
    assert all(p.isdigit() for p in parts), f"versione non semver: {version_part}"


# ---------------------------------------------------------------------------
# smoke / sample-rows / sample-bytes / root flags (contratto CLI)
# ---------------------------------------------------------------------------


def test_cli_smoke_flag_parses(tmp_path: Path) -> None:
    """contract: --smoke flag e' accettato da run mart --dry-run."""
    project_dir = tmp_path / "project-example"
    config_path = _copy_project_example(project_dir)
    runner = CliRunner()
    result = runner.invoke(app, ["run", "mart", "--config", str(config_path), "--dry-run", "--smoke"])
    assert result.exit_code == 0, result.output
    assert "Execution Plan" in result.output


def test_cli_sample_rows_flag_parses(tmp_path: Path) -> None:
    """contract: --sample-rows flag e' accettato da run all --dry-run."""
    project_dir = tmp_path / "project-example"
    config_path = _copy_project_example(project_dir)
    runner = CliRunner()
    result = runner.invoke(app, ["run", "all", "--config", str(config_path), "--dry-run", "--sample-rows", "500"])
    assert result.exit_code == 0, result.output
    assert "Execution Plan" in result.output


def test_cli_sample_bytes_flag_parses(tmp_path: Path) -> None:
    """contract: --sample-bytes flag e' accettato da run all --dry-run."""
    project_dir = tmp_path / "project-example"
    config_path = _copy_project_example(project_dir)
    runner = CliRunner()
    result = runner.invoke(app, ["run", "all", "--config", str(config_path), "--dry-run", "--sample-bytes", "5000"])
    assert result.exit_code == 0, result.output
    assert "Execution Plan" in result.output


def test_cli_root_flag_overrides_output(tmp_path: Path) -> None:
    """contract: --root flag cambia la directory di output."""
    project_dir = tmp_path / "project-example"
    config_path = _copy_project_example(project_dir)
    custom_root = tmp_path / "custom_out"
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["run", "all", "--config", str(config_path), "--root", str(custom_root), "--strict-config"],
    )
    assert result.exit_code == 0, result.output
    assert (custom_root / "data" / "clean" / "project_example" / "2022" / "project_example_2022_clean.parquet").exists()
    assert (custom_root / "data" / "mart" / "project_example" / "2022" / "rd_by_regione.parquet").exists()


# ---------------------------------------------------------------------------
# smoke flag — output isolation + run record marker
# ---------------------------------------------------------------------------


def test_cli_run_smoke_isolates_output(tmp_path: Path) -> None:
    """contract: --smoke in 'run all' scrive in {root}/smoke/ non in {root}/data/."""
    project_dir = tmp_path / "project-example"
    config_path = _copy_project_example(project_dir)
    root_dir = project_dir / "_smoke_out"
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["run", "all", "--config", str(config_path), "--smoke", "--strict-config"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    # Smoke output DEVE essere in _smoke_out/smoke/data/
    smoke_clean = root_dir / "smoke" / "data" / "clean" / "project_example" / "2022" / "project_example_2022_clean.parquet"
    assert smoke_clean.exists(), f"smoke clean not found: {smoke_clean}"
    smoke_mart = root_dir / "smoke" / "data" / "mart" / "project_example" / "2022" / "rd_by_regione.parquet"
    assert smoke_mart.exists(), f"smoke mart not found: {smoke_mart}"

    # NO output in _smoke_out/data/ (root normale non contaminata)
    clean_out = root_dir / "data" / "clean" / "project_example" / "2022"
    assert not clean_out.exists(), "smoke must NOT write to root/data/"


def test_cli_run_sample_rows_isolates_output(tmp_path: Path) -> None:
    """contract: --sample-rows (senza --smoke) isola output in {root}/smoke/."""
    project_dir = tmp_path / "project-example"
    config_path = _copy_project_example(project_dir)
    root_dir = project_dir / "_smoke_out"
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["run", "all", "--config", str(config_path), "--sample-rows", "500", "--strict-config"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    # Output in _smoke_out/smoke/data/ (come --smoke)
    smoke_clean = root_dir / "smoke" / "data" / "clean" / "project_example" / "2022" / "project_example_2022_clean.parquet"
    assert smoke_clean.exists(), f"sampled clean not found: {smoke_clean}"
    smoke_mart = root_dir / "smoke" / "data" / "mart" / "project_example" / "2022" / "rd_by_regione.parquet"
    assert smoke_mart.exists(), f"sampled mart not found: {smoke_mart}"

    # NO output in _smoke_out/data/
    clean_out = root_dir / "data" / "clean" / "project_example" / "2022"
    assert not clean_out.exists(), "--sample-rows must NOT write to root/data/"


def test_cli_run_full_smoke_isolates_output(tmp_path: Path) -> None:
    """contract: --smoke in 'run full' scrive in {root}/smoke/ non in {root}/data/."""
    project_dir = tmp_path / "project-example"
    config_path = _copy_project_example(project_dir)
    root_dir = project_dir / "_smoke_out"
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["run", "full", "--config", str(config_path), "--smoke", "--strict-config"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    # Smoke output in _smoke_out/smoke/data/
    smoke_clean = root_dir / "smoke" / "data" / "clean" / "project_example" / "2022" / "project_example_2022_clean.parquet"
    assert smoke_clean.exists(), f"smoke clean not found: {smoke_clean}"
    smoke_mart = root_dir / "smoke" / "data" / "mart" / "project_example" / "2022" / "rd_by_regione.parquet"
    assert smoke_mart.exists(), f"smoke mart not found: {smoke_mart}"

    # NO output in _smoke_out/data/
    clean_out = root_dir / "data" / "clean" / "project_example" / "2022"
    assert not clean_out.exists(), "run full --smoke must NOT write to root/data/"


def test_cli_run_smoke_run_record_marked(tmp_path: Path) -> None:
    """contract: run record da 'run all --smoke' contiene smoke: true."""
    project_dir = tmp_path / "project-example"
    config_path = _copy_project_example(project_dir)
    root_dir = project_dir / "_smoke_out"
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["run", "all", "--config", str(config_path), "--smoke", "--strict-config"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    # Trova il run record più recente in _smoke_out/smoke/data/_runs/project_example/2022/
    runs_dir = root_dir / "smoke" / "data" / "_runs" / "project_example" / "2022"
    assert runs_dir.exists(), f"runs dir not found: {runs_dir}"
    records = sorted(runs_dir.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    assert len(records) >= 1, "no run record found"

    latest = json.loads(records[0].read_text(encoding="utf-8"))
    assert latest.get("smoke") is True, f"expected smoke=True in run record, got: {latest.get('smoke')}"


def test_cli_run_full_smoke_isolates_support_output(tmp_path: Path) -> None:
    """contract: 'run full --smoke' isola output di candidate E support in {root}/smoke/."""
    project_dir = tmp_path / "project"
    config_path = _copy_project_example(project_dir)
    root_dir = project_dir / "_smoke_out"

    # Crea un support dataset minimale
    support_dir = tmp_path / "support_ds"
    (support_dir / "data").mkdir(parents=True)
    (support_dir / "sql").mkdir(parents=True)
    (support_dir / "sql" / "clean.sql").write_text(
        "SELECT 1 AS ok FROM raw_input\n", encoding="utf-8"
    )
    (support_dir / "data" / "dummy.csv").write_text("a;b\n1;2\n", encoding="utf-8")
    (support_dir / "sql" / "mart.sql").write_text(
        "SELECT * FROM clean_input\n", encoding="utf-8"
    )
    (support_dir / "dataset.yml").write_text(
        """schema_version: 1
root: out
dataset:
  name: support_ds
  years: [2022]
raw:
  sources:
    - name: csv
      type: local_file
      args:
        path: data/dummy.csv
        filename: support_ds_2022.csv
clean:
  sql: sql/clean.sql
mart:
  tables:
    - name: support_mart
      sql: sql/mart.sql
""",
        encoding="utf-8",
    )

    # Aggiunge il support al candidate dataset.yml
    config_path.write_text(
        config_path.read_text(encoding="utf-8")
        + f"""
support:
  - name: "sup"
    config: "{support_dir / 'dataset.yml'}"
    years: [2022]
""",
        encoding="utf-8",
    )

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["run", "full", "--config", str(config_path), "--smoke", "--years", "2022"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    # Candidate output in _smoke_out/smoke/data/
    smoke_clean = root_dir / "smoke" / "data" / "clean" / "project_example" / "2022" / "project_example_2022_clean.parquet"
    assert smoke_clean.exists(), f"candidate smoke clean not found: {smoke_clean}"
    smoke_mart = root_dir / "smoke" / "data" / "mart" / "project_example" / "2022" / "rd_by_regione.parquet"
    assert smoke_mart.exists(), f"candidate smoke mart not found: {smoke_mart}"

    # Niente candidate in _smoke_out/data/
    clean_out = root_dir / "data" / "clean" / "project_example" / "2022"
    assert not clean_out.exists(), "candidate smoke must NOT write to root/data/"

    # Support output in support_ds/out/smoke/data/
    sup_root = support_dir / "out"
    sup_clean = sup_root / "smoke" / "data" / "clean" / "support_ds" / "2022" / "support_ds_2022_clean.parquet"
    assert sup_clean.exists(), f"support smoke clean not found: {sup_clean}"

    # Niente support in support_ds/out/data/
    sup_clean_out = sup_root / "data" / "clean" / "support_ds" / "2022"
    assert not sup_clean_out.exists(), "support smoke must NOT write to support root/data/"


def test_cli_run_full_sample_rows_isolates_support_output(tmp_path: Path) -> None:
    """contract: 'run full --sample-rows' isola output di candidate E support in {root}/smoke/."""
    project_dir = tmp_path / "project"
    config_path = _copy_project_example(project_dir)
    root_dir = project_dir / "_smoke_out"

    # Crea un support dataset minimale
    support_dir = tmp_path / "support_ds"
    (support_dir / "data").mkdir(parents=True)
    (support_dir / "sql").mkdir(parents=True)
    (support_dir / "sql" / "clean.sql").write_text(
        "SELECT 1 AS ok FROM raw_input\n", encoding="utf-8"
    )
    (support_dir / "data" / "dummy.csv").write_text("a;b\n1;2\n", encoding="utf-8")
    (support_dir / "sql" / "mart.sql").write_text(
        "SELECT * FROM clean_input\n", encoding="utf-8"
    )
    (support_dir / "dataset.yml").write_text(
        """schema_version: 1
root: out
dataset:
  name: support_ds
  years: [2022]
raw:
  sources:
    - name: csv
      type: local_file
      args:
        path: data/dummy.csv
        filename: support_ds_2022.csv
clean:
  sql: sql/clean.sql
mart:
  tables:
    - name: support_mart
      sql: sql/mart.sql
""",
        encoding="utf-8",
    )

    # Aggiunge il support al candidate dataset.yml
    config_path.write_text(
        config_path.read_text(encoding="utf-8")
        + f"""
support:
  - name: "sup"
    config: "{support_dir / 'dataset.yml'}"
    years: [2022]
""",
        encoding="utf-8",
    )

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["run", "full", "--config", str(config_path), "--sample-rows", "500", "--years", "2022"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    # Candidate output in _smoke_out/smoke/data/
    smoke_clean = root_dir / "smoke" / "data" / "clean" / "project_example" / "2022" / "project_example_2022_clean.parquet"
    assert smoke_clean.exists(), f"candidate sampled clean not found: {smoke_clean}"
    smoke_mart = root_dir / "smoke" / "data" / "mart" / "project_example" / "2022" / "rd_by_regione.parquet"
    assert smoke_mart.exists(), f"candidate sampled mart not found: {smoke_mart}"

    # Niente candidate in _smoke_out/data/
    clean_out = root_dir / "data" / "clean" / "project_example" / "2022"
    assert not clean_out.exists(), "candidate --sample-rows must NOT write to root/data/"

    # Support output in support_ds/out/smoke/data/
    sup_root = support_dir / "out"
    sup_clean = sup_root / "smoke" / "data" / "clean" / "support_ds" / "2022" / "support_ds_2022_clean.parquet"
    assert sup_clean.exists(), f"support sampled clean not found: {sup_clean}"

    # Niente support in support_ds/out/data/
    sup_clean_out = sup_root / "data" / "clean" / "support_ds" / "2022"
    assert not sup_clean_out.exists(), "support --sample-rows must NOT write to support root/data/"


# ---------------------------------------------------------------------------
# toolkit.contracts path API
# ---------------------------------------------------------------------------


def test_contracts_layer_year_dir() -> None:
    """contract: layer_year_dir restituisce path corretto."""
    from toolkit.contracts import layer_year_dir
    path = layer_year_dir("/base", "clean", "mio_dataset", 2024)
    assert str(path) == "/base/data/clean/mio_dataset/2024"


def test_contracts_clean_parquet_path() -> None:
    """contract: clean_parquet_path restituisce path al parquet."""
    from toolkit.contracts import clean_parquet_path
    path = clean_parquet_path("/base", "mio_dataset", 2024)
    assert str(path) == "/base/data/clean/mio_dataset/2024/mio_dataset_2024_clean.parquet"


def test_contracts_mart_table_path() -> None:
    """contract: mart_table_path restituisce path corretto."""
    from toolkit.contracts import mart_table_path
    path = mart_table_path("/base", "mio_dataset", 2024, "rd_by_regione")
    assert str(path) == "/base/data/mart/mio_dataset/2024/rd_by_regione.parquet"


def test_contracts_run_record_dir() -> None:
    """contract: run_record_dir restituisce path corretto."""
    from toolkit.contracts import run_record_dir
    path = run_record_dir("/base", "mio_dataset", 2024)
    assert str(path) == "/base/data/_runs/mio_dataset/2024"


def test_contracts_constants() -> None:
    """contract: costanti METADATA_JSON e MANIFEST_JSON esistono."""
    from toolkit.contracts import MANIFEST_JSON, METADATA_JSON
    assert METADATA_JSON == "metadata.json"
    assert MANIFEST_JSON == "manifest.json"
