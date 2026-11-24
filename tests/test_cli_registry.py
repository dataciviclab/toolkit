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


def _write_existing(out_dir: Path, with_last_run: bool = True) -> None:
    """Registry esistente con stato (simula CI post-merge: dataset non rieseguito)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    existing = {
        "schema_version": 2,
        "repo": "open-siope",
        "source_repo": "dataciviclab/open-siope",
        "updated_at": "2026-08-01",
        "gcs": {"prefix": "siope/", "clean": "year"},
        "datasets": [],
        "marts": [],
        "checks": [],
    }
    if with_last_run:
        existing["datasets"] = [
            {
                "slug": "my_dataset",
                "partitions": {"2023": 40},
                "last_run": {"at": "2025-12-31T00:00:00Z", "year": 2023, "status": "SUCCESS"},
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
def test_build_write_v2_shape(tmp_path: Path) -> None:
    """Il comando scrive registry v2: contratto gcs, partitions, last_run, checks.

    Niente location per voce, niente signals/entities/stage: i path si derivano
    dal contratto (lab_connectors.registry.locations).
    """
    _make_repo(tmp_path, with_run=False)
    out = tmp_path / "outreg"
    _write_existing(out, with_last_run=True)
    result = runner.invoke(
        app, ["registry", "build", "--repo", str(tmp_path), "--write", "--out", str(out)], obj={}
    )
    assert result.exit_code == 0, result.stdout + result.stderr
    payload = json.loads((out / "registry.json").read_text(encoding="utf-8"))
    assert payload["schema_version"] == 2
    assert payload["gcs"]["clean"] == "year"
    mart = next(m for m in payload["marts"] if m["slug"] == "my_dataset__mart_trend")
    assert "location" not in mart
    assert mart["layout"] == "glob"
    ds = next(d for d in payload["datasets"] if d["slug"] == "my_dataset")
    assert "location" not in ds
    assert "stage" not in ds
    # partitions: locali (2024, 2025) + preserved (2023) — chiavi stringa nel JSON
    assert set(ds["partitions"]) == {"2023", "2024", "2025"}
    # last_run preservato da existing (nessun run locale)
    assert ds["last_run"]["year"] == 2023
    assert "signals" not in payload
    assert "entities" not in payload
    assert payload["checks"] == []
    assert payload["source_repo"] == "dataciviclab/toolkit" or payload["source_repo"]


@pytest.mark.pure_unit
def test_build_no_datasets_dir_fails(tmp_path: Path) -> None:
    """Repo senza sezioni dati → errore pulito."""
    result = runner.invoke(app, ["registry", "build", "--repo", str(tmp_path)], obj={})
    assert result.exit_code != 0
    assert "nessuna sezione dati" in result.stderr
