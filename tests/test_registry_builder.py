"""Test del registry builder condiviso (fixture mini-repo, nessuna rete).

Il builder riusa il runtime del toolkit (config model, path resolver,
run_state, parquet_schema): la fixture crea un mini-repo con dataset.yml
(root relativo), parquet locali e run records — stesso layout reale.

Marker: contract (artefatti validi contro gli schemi), policy (namespace
canonico = dataset.name), pure_unit (logica pura).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from toolkit.registry.builders import (
    build_clean_catalog,
    build_mart_catalog,
    build_signals,
)
from toolkit.registry.layout import RepoLayout
from toolkit.registry.paths import PathContract
from toolkit.registry.runs import run_block

pytest.importorskip("duckdb")

MART_YML = """
root: "../../out"
schema_version: 1
dataset:
  name: "{name}"
  source_id: "{source_id}"
  tags: [test, demo]
  category: demo
  years: [2024, 2025]
  time_coverage:
    start_year: 2000
    end_year: 2024
mart:
  tables:
    - name: "mart_trend"
      sql: "sql/mart_trend.sql"
  validate:
    table_rules:
      mart_trend:
        required_columns: [year, geo, value]
        primary_key: [year, geo]
        min_rows: 10
"""

SIMPLE_YML = """
root: "../../out"
schema_version: 1
dataset:
  name: "{name}"
  source_id: "{source_id}"
  tags: [test]
  category: demo
  years: [2024]
"""

RUN_RECORD = {
    "dataset": "my_dataset",
    "year": 2024,
    "run_id": "20260101T000000Z_abc123",
    "status": "SUCCESS",
    "started_at": "2026-01-01T00:00:00Z",
    "finished_at": "2026-01-01T00:00:01Z",
    "duration_seconds": 1.0,
    "layers": {
        "raw": {
            "status": "SUCCESS",
            "metrics": {"output_rows": None, "output_bytes": 6961784},
        },
        "clean": {
            "status": "SUCCESS",
            "metrics": {"output_rows": 120, "output_bytes": 9757664},
        },
        "mart": {"status": "SUCCESS", "metrics": {"output_rows": 30}},
    },
    "validations": {
        "raw": {"quality_score": 100},
        "clean": {"quality_score": 95},
        "mart": {"quality_score": 100},
    },
}


def _write_parquet(path: Path) -> None:
    import duckdb

    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect() as con:
        con.execute("CREATE TABLE t (geo VARCHAR, year INTEGER, value DOUBLE)")
        con.execute("INSERT INTO t VALUES ('ITC4', 2024, 1.5), ('ITH3', 2024, 2.5)")
        con.execute(f"COPY t TO '{path}' (FORMAT parquet)")


def _write_run_record(runs_root: Path, slug: str, year: int = 2024) -> None:
    run_dir = runs_root / slug / str(year)
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "20260101T000000Z_abc123.json").write_text(json.dumps(RUN_RECORD), encoding="utf-8")


def _make_repo(
    tmp_path: Path, *, with_mart: bool = True, dirname: str = "my-dataset"
) -> RepoLayout:
    """Mini-repo: datasets/{dirname}/dataset.yml + parquet locale + run record.

    Il run record copre solo il 2024 mentre il config dichiara anche il 2025:
    verifica il fallback su ``years_seen`` di run_state.
    """
    ds_dir = tmp_path / "datasets" / dirname
    ds_dir.mkdir(parents=True)
    name = "my_dataset"
    yml = MART_YML if with_mart else SIMPLE_YML
    (ds_dir / "dataset.yml").write_text(yml.format(name=name, source_id="src1"), encoding="utf-8")
    if with_mart:
        (ds_dir / "sql").mkdir()
        (ds_dir / "sql" / "mart_trend.sql").write_text("SELECT 1", encoding="utf-8")
    else:
        (ds_dir / "sql").mkdir()
        (ds_dir / "sql" / "clean.sql").write_text("SELECT 1", encoding="utf-8")

    out = tmp_path / "out"
    _write_parquet(out / "data" / "clean" / name / "2024" / f"{name}_2024_clean.parquet")
    _write_parquet(out / "data" / "clean" / name / "2025" / f"{name}_2025_clean.parquet")
    _write_run_record(out / "data" / "_runs", name)

    return RepoLayout(
        repo_root=tmp_path,
        dataset_dirs=("datasets",),
        source_repo="dataciviclab/test",
    )


# ---------------------------------------------------------------------------
# clean_catalog
# ---------------------------------------------------------------------------


def test_clean_catalog_contract(tmp_path: Path) -> None:
    """Il catalogo generato è valido contro lo schema condiviso (contract)."""
    layout = _make_repo(tmp_path)
    catalog, errors = build_clean_catalog(layout)

    assert errors == {"derive": [], "validation": []}, f"errori inattesi: {errors}"
    assert len(catalog["datasets"]) == 1
    ds = catalog["datasets"][0]
    assert ds["slug"] == "my_dataset"
    assert ds["period"] == {"start": 2000, "end": 2024}  # da time_coverage
    assert [c["name"] for c in ds["columns"]] == ["geo", "year", "value"]
    assert ds["mart_refs"] == ["my_dataset__mart_trend"]
    assert ds["run"]["status"] == "SUCCESS"
    assert ds["run"]["quality_score"] == {"raw": 100, "clean": 95, "mart": 100}
    assert ds["run"]["output_bytes"] == {"raw": 6961784, "clean": 9757664}


def test_clean_catalog_dirname_not_identity(tmp_path: Path) -> None:
    """La dir con trattini non è identità: la chiave è dataset.name (policy)."""
    layout = _make_repo(tmp_path, dirname="my-dataset")
    catalog, _ = build_clean_catalog(layout)
    assert catalog["datasets"][0]["slug"] == "my_dataset"


def test_clean_catalog_missing_parquet_reported(tmp_path: Path) -> None:
    """Dataset dichiarato senza parquet locale → errore, non catalogo finto."""
    layout = _make_repo(tmp_path)
    import shutil

    shutil.rmtree(layout.repo_root / "out" / "data" / "clean")
    catalog, errors = build_clean_catalog(layout)
    assert any("nessun parquet clean locale" in e for e in errors["derive"])
    assert catalog["datasets"] == []


# ---------------------------------------------------------------------------
# mart_catalog
# ---------------------------------------------------------------------------


def test_mart_catalog_contract(tmp_path: Path) -> None:
    """Il mart catalog è valido e usa la convention {dataset}__{mart} (contract)."""
    layout = _make_repo(tmp_path)
    catalog, errors = build_mart_catalog(layout)

    assert errors == {"derive": [], "validation": []}, f"errori inattesi: {errors}"
    assert len(catalog["marts"]) == 1
    mart = catalog["marts"][0]
    assert mart["slug"] == "my_dataset__mart_trend"
    assert mart["dataset"] == "my_dataset"
    assert mart["primary_key"] == ["year", "geo"]
    assert mart["required_columns"] == ["year", "geo", "value"]
    assert mart["min_rows"] == 10
    assert mart["run"]["status"] == "SUCCESS"


def test_mart_catalog_year_layout_without_years(tmp_path: Path) -> None:
    """Layout 'year' + mart senza years → errore di derive, non crash (regression)."""
    from toolkit.registry.layout import RepoLayout as RL

    ds_dir = tmp_path / "datasets" / "no-years"
    ds_dir.mkdir(parents=True)
    no_years_yml = """
root: "../../out"
schema_version: 1
dataset:
  name: "no_years"
  years: []
mart:
  tables:
    - name: "mart_trend"
      sql: "sql/mart_trend.sql"
"""
    (ds_dir / "dataset.yml").write_text(no_years_yml, encoding="utf-8")
    layout = RL(repo_root=tmp_path, dataset_dirs=("datasets",), source_repo="dataciviclab/test")

    catalog, errors = build_mart_catalog(layout, path_contract=PathContract())  # layout year

    assert catalog["marts"] == []
    assert any("location mart non risolvibile" in e for e in errors["derive"])


# ---------------------------------------------------------------------------
# pipeline_signals
# ---------------------------------------------------------------------------


def test_signals_contract(tmp_path: Path) -> None:
    """Segnale ok con mart; campi years/run presenti (contract)."""
    layout = _make_repo(tmp_path)
    payload, errors = build_signals(layout)

    assert errors == {"derive": [], "validation": []}, f"errori inattesi: {errors}"
    assert payload["summary"]["total"] == 1
    assert payload["summary"]["by_status"] == {"ok": 1, "warn": 0, "error": 0}
    sig = payload["signals"][0]
    assert sig["id"] == "my_dataset"
    assert sig["status"] == "ok"
    assert sig["years"] == [2024, 2025]
    assert sig["run"]["run_id"] == "20260101T000000Z_abc123"


def test_signals_warn_without_mart(tmp_path: Path) -> None:
    """Senza mart il segnale è warn, non ok (policy)."""
    layout = _make_repo(tmp_path, with_mart=False)
    payload, errors = build_signals(layout)
    assert errors == {"derive": [], "validation": []}
    assert payload["signals"][0]["status"] == "warn"
    assert payload["signals"][0]["action"].startswith("aggiungere mart")


# ---------------------------------------------------------------------------
# path contract (pure_unit)
# ---------------------------------------------------------------------------


def test_path_contract_year_layout() -> None:
    contract = PathContract()  # default DI
    assert (
        contract.clean_parquet_url("irpef_comunale", 2024)
        == "gs://dataciviclab-clean/irpef_comunale/2024/irpef_comunale_2024_clean.parquet"
    )
    assert (
        contract.mart_parquet_url("irpef_comunale", "mart_trend", year=2024)
        == "gs://dataciviclab-mart/irpef_comunale/2024/mart_trend.parquet"
    )
    loc = contract.clean_location("multi_year", [2023, 2024])
    assert loc["multi_file"] is True
    assert "/*/" in loc["path"]


def test_path_contract_flat_layout() -> None:
    contract = PathContract(prefix="eurostat", clean_layout="flat", mart_layout="flat")
    assert (
        contract.clean_parquet_url("eurostat_gdp_nuts3", 2026)
        == "gs://dataciviclab-clean/eurostat/eurostat_gdp_nuts3/eurostat_gdp_nuts3_2026_clean.parquet"
    )
    assert (
        contract.mart_parquet_url("eurostat_gdp_nuts3", "mart_geo_benchmark")
        == "gs://dataciviclab-mart/eurostat/eurostat_gdp_nuts3/mart_geo_benchmark.parquet"
    )


# ---------------------------------------------------------------------------
# run block (pure_unit)
# ---------------------------------------------------------------------------


def test_run_block_mapping() -> None:
    block = run_block(RUN_RECORD)
    assert block == {
        "run_id": "20260101T000000Z_abc123",
        "year": 2024,
        "status": "SUCCESS",
        "quality_score": {"raw": 100, "clean": 95, "mart": 100},
        # raw: output_rows null → omesso; output_bytes preservato
        "output_rows": {"clean": 120, "mart": 30},
        "output_bytes": {"raw": 6961784, "clean": 9757664},
        "started_at": "2026-01-01T00:00:00Z",
        "finished_at": "2026-01-01T00:00:01Z",
        "duration_seconds": 1.0,
    }


def test_run_block_none_for_incomplete() -> None:
    assert run_block(None) is None
    assert run_block({"year": 2024, "status": "SUCCESS"}) is None  # no run_id


def test_registry_builder_imports() -> None:
    """Export pubblico del package (contract)."""
    import toolkit.registry as reg

    assert all(
        hasattr(reg, name)
        for name in ("build_clean_catalog", "build_mart_catalog", "build_signals", "RepoLayout")
    )
