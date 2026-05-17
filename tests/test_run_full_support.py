"""Tests for ``toolkit run full --dry-run`` con support dataset."""

from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from toolkit.cli.app import app

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
PROJECT_EXAMPLE = ROOT / "project-example"


def test_run_full_dry_run_with_support(tmp_path: Path, monkeypatch) -> None:
    """run full --dry-run esegue i support (per output reali) ma candidate dry.

    Regressione: resolve_support_payloads richiede file reali per i support,
    quindi anche in dry-run i support vanno eseguiti realmente.
    """
    import shutil

    monkeypatch.chdir(tmp_path)

    # Crea un support dataset minimale (solo raw, senza validazioni)
    support_dir = tmp_path / "support_ds"
    (support_dir / "data").mkdir(parents=True, exist_ok=True)
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

    # Candidate che usa il support
    cand_dir = tmp_path / "candidate"
    shutil.copytree(PROJECT_EXAMPLE, cand_dir)
    cand_yml = cand_dir / "dataset.yml"

    cand_yml.write_text(
        cand_yml.read_text(encoding="utf-8")
        + f"""
support:
  - name: "sup"
    config: "{support_dir / 'dataset.yml'}"
    years: [2022]
""",
        encoding="utf-8",
    )

    runner = CliRunner()

    # Prima esegui il support (per popolare output reali)
    sup_result = runner.invoke(
        app,
        ["run", "all", "--config", str(support_dir / "dataset.yml")],
        catch_exceptions=False,
    )
    assert sup_result.exit_code == 0, sup_result.output

    # Dry-run del candidate: deve funzionare perche' il support ha output reali
    result = runner.invoke(
        app,
        ["run", "full", "--config", str(cand_yml), "--dry-run", "--years", "2022"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.output
    assert "DRY_RUN" in result.output
    assert "sql_validation: OK" in result.output or "status: passed" in result.output


def test_run_full_dry_run_support_nonexistent_config_fails(tmp_path: Path, monkeypatch) -> None:
    """run full --dry-run fallisce se un support ha config inesistente."""
    import shutil

    monkeypatch.chdir(tmp_path)
    cand_dir = tmp_path / "candidate"
    shutil.copytree(PROJECT_EXAMPLE, cand_dir)
    cand_yml = cand_dir / "dataset.yml"

    cand_yml.write_text(
        cand_yml.read_text(encoding="utf-8")
        + """
support:
  - name: "ghost"
    config: "/nonexistent/path/dataset.yml"
    years: [2022]
""",
        encoding="utf-8",
    )

    runner = CliRunner()

    # Il caricamento del config del support fallisce -> exit non-zero
    result = runner.invoke(app, ["run", "full", "--config", str(cand_yml), "--dry-run", "--years", "2022"])

    assert result.exit_code != 0
