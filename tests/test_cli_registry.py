"""Test per `toolkit registry build` — generazione registry.json centralizzata.

Il comando sostituisce i wrapper scripts/build_registry.py per-repo: scopre le
sezioni dati per convenzione (repo_dataset_dirs), deriva source_repo dal git
remote e preserva i run dei mart da existing (il fix del bug #465 che i wrapper
legacy non passavano).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from toolkit.cli.app import app

pytest.importorskip("duckdb")

from tests.test_registry_builder import _make_repo  # noqa: E402

runner = CliRunner()


def _write_existing(out_dir: Path, with_mart_run: bool = True) -> None:
    """Registry esistente con un mart con run storico (simula CI post-merge)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    existing = {
        "schema_version": 1,
        "repo": "open-siope",
        "source_repo": "dataciviclab/open-siope",
        "updated_at": "2026-08-01",
        "datasets": [],
        "marts": [],
        "signals": [],
    }
    if with_mart_run:
        existing["marts"] = [
            {
                "slug": "my_dataset__mart_trend",
                "dataset": "my_dataset",
                "table": "mart_trend",
                "run": {"run_id": "OLD", "year": 2025, "status": "SUCCESS"},
            }
        ]
    (out_dir / "registry.json").write_text(
        json.dumps(existing, indent=2, ensure_ascii=False), encoding="utf-8"
    )


@pytest.mark.pure_unit
def test_build_dry_run_reports_counts(tmp_path: Path) -> None:
    """Dry-run: stampa il riepilogo senza scrivere il file."""
    _make_repo(tmp_path, with_run=True)
    _write_existing(tmp_path / "registry")
    out = tmp_path / "outreg"
    result = runner.invoke(
        app, ["registry", "build", "--repo", str(tmp_path), "--out", str(out)], obj={}
    )
    assert result.exit_code == 0, result.stdout + result.stderr
    assert "datasets 1" in result.stdout
    assert "Dry-run" in result.stdout
    assert not (out / "registry.json").is_file()


@pytest.mark.pure_unit
def test_build_write_preserves_mart_runs(tmp_path: Path) -> None:
    """Il comando scrive registry.json e preserva i run dei mart da existing.

    È il fix del bug #465: i wrapper legacy passavano existing_catalog senza
    la sezione marts → i run dei mart sparivano a ogni rigenerazione CI.
    with_run=False simula la CI (nessun run record locale → existing prevale).
    """
    _make_repo(tmp_path, with_run=False)
    out = tmp_path / "outreg"
    _write_existing(out, with_mart_run=True)
    result = runner.invoke(
        app, ["registry", "build", "--repo", str(tmp_path), "--write", "--out", str(out)], obj={}
    )
    assert result.exit_code == 0, result.stdout + result.stderr
    payload = json.loads((out / "registry.json").read_text(encoding="utf-8"))
    mart = next(m for m in payload["marts"] if m["slug"] == "my_dataset__mart_trend")
    assert mart["run"]["run_id"] == "OLD"
    assert payload["source_repo"] == "dataciviclab/toolkit" or payload["source_repo"]


@pytest.mark.pure_unit
def test_build_no_datasets_dir_fails(tmp_path: Path) -> None:
    """Repo senza sezioni dati → errore pulito."""
    result = runner.invoke(app, ["registry", "build", "--repo", str(tmp_path)], obj={})
    assert result.exit_code != 0
    assert "nessuna sezione dati" in result.stderr
