"""Tests for ``toolkit run full --dry-run`` con support dataset."""

from __future__ import annotations

from pathlib import Path

import pytest

from toolkit.cli.app import app

pytestmark = pytest.mark.regression


def test_run_full_dry_run_with_support(project_example: Path, runner, tmp_path: Path) -> None:
    """run full --dry-run deve funzionare anche se il support non e' mai stato eseguito.

    Regressione: resolve_support_payloads in dry-run usa require_exists=False,
    quindi la validazione SQL del candidate non richiede file reali dei support.
    """
    # Crea un support dataset minimale
    support_dir = tmp_path / "support_ds"
    (support_dir / "data").mkdir(parents=True, exist_ok=True)
    (support_dir / "sql").mkdir(parents=True)
    (support_dir / "sql" / "clean.sql").write_text(
        "SELECT 1 AS ok FROM raw_input\n", encoding="utf-8"
    )
    (support_dir / "data" / "dummy.csv").write_text("a;b\n1;2\n", encoding="utf-8")
    (support_dir / "sql" / "mart.sql").write_text("SELECT * FROM clean_input\n", encoding="utf-8")
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
    cand_yml = project_example / "dataset.yml"
    cand_yml.write_text(
        cand_yml.read_text(encoding="utf-8")
        + f"""
support:
  - name: "sup"
    config: "{support_dir / "dataset.yml"}"
    years: [2022]
""",
        encoding="utf-8",
    )

    # NON eseguiamo il support prima. run full --dry-run deve funzionare
    # comunque grazie a require_exists=False.
    result = runner.invoke(
        app,
        ["run", "--config", str(cand_yml), "--dry-run", "--years", "2022"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.output
    assert "DRY_RUN" in result.output
    assert "sql_validation: OK" in result.output or "status: passed" in result.output
    # Il support deve essere annunciato in dry-run
    assert "support: sup" in result.output


def test_run_full_dry_run_support_nonexistent_config_fails(project_example: Path, runner) -> None:
    """run full --dry-run fallisce se un support ha config inesistente."""
    cand_yml = project_example / "dataset.yml"
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

    # Il caricamento del config del support fallisce -> exit non-zero
    result = runner.invoke(
        app,
        ["run", "--config", str(cand_yml), "--dry-run", "--years", "2022"],
    )
    assert result.exit_code != 0


def test_run_full_skips_existing_support(
    project_example: Path, runner, tmp_path: Path, monkeypatch
) -> None:
    """ADR-005: se gli output del support (clean+mart) esistono già, il run
    lo riusa senza rieseguirlo (skip-if-exists)."""
    # Support dataset con output già materializzati
    support_dir = tmp_path / "support_ds"
    (support_dir / "sql").mkdir(parents=True)
    (support_dir / "sql" / "clean.sql").write_text(
        "SELECT 1 AS ok FROM raw_input\n", encoding="utf-8"
    )
    (support_dir / "sql" / "mart.sql").write_text("SELECT * FROM clean_input\n", encoding="utf-8")
    (support_dir / "data").mkdir()
    (support_dir / "data" / "dummy.csv").write_text("a;b\n1;2\n", encoding="utf-8")
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
    # Output attesi già presenti (ADR-005: clean + mart)
    clean_dir = support_dir / "out" / "data" / "clean" / "support_ds" / "2022"
    mart_dir = support_dir / "out" / "data" / "mart" / "support_ds" / "2022"
    clean_dir.mkdir(parents=True)
    mart_dir.mkdir(parents=True)
    (clean_dir / "support_ds_2022_clean.parquet").write_bytes(b"")
    (mart_dir / "support_mart.parquet").write_bytes(b"")

    cand_yml = project_example / "dataset.yml"
    cand_yml.write_text(
        cand_yml.read_text(encoding="utf-8")
        + f"""
support:
  - name: "sup"
    config: "{support_dir / "dataset.yml"}"
    years: [2022]
""",
        encoding="utf-8",
    )

    # Spy: run_year non deve essere chiamata per il support (skip)
    import toolkit.cli.cmd_run as cmd_run_mod

    support_runs: list[int] = []
    orig_run_year = cmd_run_mod.run_year

    def spy_run_year(cfg, year, **kwargs):
        if getattr(cfg, "dataset", None) == "support_ds":
            support_runs.append(year)
        return orig_run_year(cfg, year, **kwargs)

    monkeypatch.setattr(cmd_run_mod, "run_year", spy_run_year)

    result = runner.invoke(
        app,
        ["run", "--config", str(cand_yml), "--years", "2022"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.output
    assert support_runs == [], f"support rieseguito nonostante output presenti: {support_runs}"
    assert "reuse support sup" in result.output


class _FakeSupportCtx:
    validations = {
        "raw": {"passed": True},
        "clean": {"passed": True},
        "mart": {"passed": True},
    }


def test_run_full_runs_missing_support(
    project_example: Path, runner, tmp_path: Path, monkeypatch
) -> None:
    """ADR-005: se gli output del support mancano, il run lo esegue (invariato)."""
    support_dir = tmp_path / "support_ds"
    (support_dir / "sql").mkdir(parents=True)
    (support_dir / "sql" / "clean.sql").write_text(
        "SELECT 1 AS ok FROM raw_input\n", encoding="utf-8"
    )
    (support_dir / "sql" / "mart.sql").write_text("SELECT * FROM clean_input\n", encoding="utf-8")
    (support_dir / "data").mkdir()
    (support_dir / "data" / "dummy.csv").write_text("a;b\n1;2\n", encoding="utf-8")
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

    cand_yml = project_example / "dataset.yml"
    cand_yml.write_text(
        cand_yml.read_text(encoding="utf-8")
        + f"""
support:
  - name: "sup"
    config: "{support_dir / "dataset.yml"}"
    years: [2022]
""",
        encoding="utf-8",
    )

    import toolkit.cli.cmd_run as cmd_run_mod

    support_runs: list[int] = []
    orig_run_year = cmd_run_mod.run_year

    def spy_run_year(cfg, year, **kwargs):
        if getattr(cfg, "dataset", None) == "support_ds":
            support_runs.append(year)
            # Simula il run: crea gli output attesi (clean + mart)
            clean_dir = support_dir / "out" / "data" / "clean" / "support_ds" / str(year)
            mart_dir = support_dir / "out" / "data" / "mart" / "support_ds" / str(year)
            clean_dir.mkdir(parents=True, exist_ok=True)
            mart_dir.mkdir(parents=True, exist_ok=True)
            (clean_dir / f"support_ds_{year}_clean.parquet").write_bytes(b"")
            (mart_dir / "support_mart.parquet").write_bytes(b"")
            return _FakeSupportCtx()
        return orig_run_year(cfg, year, **kwargs)

    monkeypatch.setattr(cmd_run_mod, "run_year", spy_run_year)

    result = runner.invoke(
        app,
        ["run", "--config", str(cand_yml), "--years", "2022"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.output
    assert support_runs == [2022], (
        f"support non eseguito nonostante output mancanti: {support_runs}"
    )
