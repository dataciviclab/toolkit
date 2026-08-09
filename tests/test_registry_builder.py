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
    build_entity_graph,
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
registry:
  description: "Test dataset per il registry builder"
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
    tmp_path: Path,
    *,
    with_mart: bool = True,
    with_run: bool = True,
    dirname: str = "my-dataset",
) -> RepoLayout:
    """Mini-repo: datasets/{dirname}/dataset.yml + parquet locale + run record.

    Il run record copre solo il 2024 mentre il config dichiara anche il 2025:
    verifica il fallback su ``years_seen`` di run_state. ``with_run=False``
    simula la CI (nessun run record locale).
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
    if with_run:
        _write_run_record(out / "data" / "_runs", name)

    return RepoLayout(
        repo_root=tmp_path,
        dataset_dirs=("datasets",),
        source_repo="dataciviclab/test",
    )


# ---------------------------------------------------------------------------
# clean_catalog
# ---------------------------------------------------------------------------


@pytest.mark.contract
def test_clean_catalog_contract(tmp_path: Path) -> None:
    """Il catalogo generato è valido contro lo schema condiviso (contract)."""
    layout = _make_repo(tmp_path)
    catalog, errors = build_clean_catalog(layout)

    assert errors == {"derive": [], "validation": []}, f"errori inattesi: {errors}"
    assert len(catalog["datasets"]) == 1
    ds = catalog["datasets"][0]
    assert ds["slug"] == "my_dataset"
    assert ds["period"] == {"start": 2000, "end": 2024}  # da time_coverage
    assert ds["description"] == "Test dataset per il registry builder"  # da registry.description
    assert [c["name"] for c in ds["columns"]] == ["geo", "year", "value"]
    assert ds["mart_refs"] == ["my_dataset__mart_trend"]
    assert ds["run"]["status"] == "SUCCESS"
    assert ds["run"]["quality_score"] == {"raw": 100, "clean": 95, "mart": 100}
    assert ds["run"]["output_bytes"] == {"raw": 6961784, "clean": 9757664}


@pytest.mark.policy
def test_clean_catalog_dirname_not_identity(tmp_path: Path) -> None:
    """La dir con trattini non è identità: la chiave è dataset.name (policy)."""
    layout = _make_repo(tmp_path, dirname="my-dataset")
    catalog, _ = build_clean_catalog(layout)
    assert catalog["datasets"][0]["slug"] == "my_dataset"


@pytest.mark.pure_unit
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


@pytest.mark.contract
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


@pytest.mark.pure_unit
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


@pytest.mark.contract
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


@pytest.mark.policy
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


@pytest.mark.contract
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


@pytest.mark.contract
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


@pytest.mark.pure_unit
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


@pytest.mark.pure_unit
def test_run_block_none_for_incomplete() -> None:
    assert run_block(None) is None
    assert run_block({"year": 2024, "status": "SUCCESS"}) is None  # no run_id


@pytest.mark.pure_unit
def test_registry_builder_imports() -> None:
    """Export pubblico del package (contract)."""
    import toolkit.registry as reg

    assert all(
        hasattr(reg, name)
        for name in ("build_clean_catalog", "build_mart_catalog", "build_signals", "RepoLayout")
    )


# ---------------------------------------------------------------------------
# codelists
# ---------------------------------------------------------------------------


@pytest.mark.contract
def test_codelists_contract(tmp_path: Path) -> None:
    """codelists.json espone i valori delle dimensioni dal repo (contract)."""
    from toolkit.registry.builders import build_codelists

    layout = _make_repo(tmp_path)
    cl_dir = tmp_path / "codelists"
    cl_dir.mkdir()
    (cl_dir / "units.csv").write_text(
        "unit,label_en\nEUR_HAB,EUR per inhabitant\nPPS_HAB,PPS per inhabitant\n",
        encoding="utf-8",
    )
    (cl_dir / "geo.csv").write_text(
        "code,label_en,nuts_level,parent_code\nITC4,Lombardia,3,ITH\n",
        encoding="utf-8",
    )

    payload, errors = build_codelists(layout)
    assert errors == {"derive": [], "validation": []}, f"errori inattesi: {errors}"
    assert payload["codelists"]["units"] == [
        {"unit": "EUR_HAB", "label_en": "EUR per inhabitant"},
        {"unit": "PPS_HAB", "label_en": "PPS per inhabitant"},
    ]
    assert payload["codelists"]["geo"] == [
        {"code": "ITC4", "label_en": "Lombardia", "nuts_level": "3", "parent_code": "ITH"}
    ]


@pytest.mark.pure_unit
def test_codelists_large_external(tmp_path: Path) -> None:
    """Codelist oltre la soglia → dichiarato in large, non embedded (policy)."""
    from toolkit.registry.builders import build_codelists, MAX_CODELIST_ROWS

    layout = _make_repo(tmp_path)
    cl_dir = tmp_path / "codelists"
    cl_dir.mkdir()
    rows = "".join(f"code{i},label{i}\n" for i in range(MAX_CODELIST_ROWS + 5))
    (cl_dir / "big.csv").write_text("code,label_en\n" + rows, encoding="utf-8")

    payload, errors = build_codelists(layout)
    assert errors == {"derive": [], "validation": []}
    assert "big" not in payload["codelists"]
    assert payload["large"] == ["big"]


@pytest.mark.pure_unit
def test_codelists_empty_without_dir(tmp_path: Path) -> None:
    """Senza dir codelists → payload vuoto, nessun errore (opzionale)."""
    from toolkit.registry.builders import build_codelists

    layout = _make_repo(tmp_path)
    payload, errors = build_codelists(layout)
    assert errors == {"derive": [], "validation": []}
    assert payload["codelists"] == {}


class TestSignalsExisting:
    """existing_signals: preserva i run storici quando il run locale manca."""

    @pytest.mark.contract
    def test_preserves_run_from_existing_signals(self, tmp_path: Path) -> None:
        """CI: nessun run record locale → il run viene dal signals committato."""
        layout = _make_repo(tmp_path, with_run=False)
        existing = {
            "schema_version": "1",
            "signals": [
                {
                    "id": "my_dataset",
                    "status": "ok",
                    "run": {"run_id": "20260101T000000Z_abc123", "year": 2025, "status": "SUCCESS"},
                }
            ],
        }
        payload, errors = build_signals(layout, existing_signals=existing)
        assert errors == {"derive": [], "validation": []}
        sig = payload["signals"][0]
        assert sig["id"] == "my_dataset"
        assert sig["status"] == "ok"  # struttura derivata dal manifest
        assert sig["run"]["run_id"] == "20260101T000000Z_abc123"  # run preservato
        assert sig["run"]["status"] == "SUCCESS"

    @pytest.mark.contract
    def test_local_run_wins_over_existing(self, tmp_path: Path) -> None:
        """Run locale presente → vince su existing_signals (non lo sovrascrive)."""
        layout = _make_repo(tmp_path, with_run=True)
        existing = {
            "schema_version": "1",
            "signals": [
                {
                    "id": "my_dataset",
                    "run": {"run_id": "OLD_run", "year": 2025, "status": "FAILED"},
                }
            ],
        }
        payload, _errors = build_signals(layout, existing_signals=existing)
        sig = payload["signals"][0]
        assert sig["run"]["run_id"] == "20260101T000000Z_abc123"  # run locale, non OLD


class TestEntityGraph:
    """build_entity_graph: entità + bridge dal clean_catalog (5° artifact)."""

    @pytest.mark.contract
    def test_entity_graph_from_catalog(self, tmp_path: Path) -> None:
        catalog = {
            "schema_version": 1,
            "datasets": [
                {
                    "slug": "anac_bandi_gara",
                    "name": "Bandi",
                    "columns": [
                        {
                            "name": "cig",
                            "type": "VARCHAR",
                            "role": "dimension",
                            "semantic_type": "cig_code",
                        },
                        {
                            "name": "comune",
                            "type": "VARCHAR",
                            "role": "dimension",
                            "semantic_type": "municipality_code",
                        },
                    ],
                },
                {
                    "slug": "demo_ds",
                    "name": "Demo",
                    "columns": [
                        {
                            "name": "anno",
                            "type": "INTEGER",
                            "role": "metric",
                            "semantic_type": "year",
                        },
                    ],
                },
            ],
        }
        graph = build_entity_graph(catalog)

        assert graph["summary"]["total_entities"] >= 2
        entities = graph["entities"]
        assert "Gara" in entities  # cig_code → entity Gara
        assert "Comune" in entities  # municipality_code → entity Comune
        # bridge cig_code → municipality_code
        bridges = graph["bridges"]
        assert any(
            b["from"]["via"] == "cig_code" and b["to"]["semantic_type"] == "municipality_code"
            for b in bridges
        )
        assert graph["schema_version"] == 1


# ---------------------------------------------------------------------------
# Scoperta sezioni dati per convenzione (risoluzione univoca layout)
# ---------------------------------------------------------------------------


class TestRepoDatasetDirs:
    """Scoperta per convenzione: dir con {slug}/dataset.yml = sezione dati.

    Sostituisce la vecchia mappa REPO_DATASET_DIRS: nessun repo va dichiarato,
    la sezione si scopre dalla struttura (scalabile a qualsiasi layout).
    """

    @pytest.mark.contract
    def test_discovers_flat_datasets(self, tmp_path: Path) -> None:
        """Repo con datasets/{slug}/dataset.yml → sezione datasets."""
        from toolkit.registry.layout import repo_dataset_dirs

        repo = tmp_path / "eurostat"
        (repo / "datasets" / "crime").mkdir(parents=True)
        (repo / "datasets" / "crime" / "dataset.yml").write_text("x", encoding="utf-8")

        assert repo_dataset_dirs(repo) == ("datasets",)

    @pytest.mark.contract
    def test_discovers_multiple_sections(self, tmp_path: Path) -> None:
        """Repo con più sezioni (datasets + support) → tutte scoperte."""
        from toolkit.registry.layout import repo_dataset_dirs

        repo = tmp_path / "open-conto-annuale"
        (repo / "datasets" / "personale").mkdir(parents=True)
        (repo / "datasets" / "personale" / "dataset.yml").write_text("x", encoding="utf-8")
        (repo / "support" / "anag-enti").mkdir(parents=True)
        (repo / "support" / "anag-enti" / "dataset.yml").write_text("x", encoding="utf-8")

        assert repo_dataset_dirs(repo) == ("datasets", "support")

    @pytest.mark.contract
    def test_discovers_candidates_compose_support(self, tmp_path: Path) -> None:
        """DI: candidates/compose/support_datasets scoperti (nessuna mappa)."""
        from toolkit.registry.layout import repo_dataset_dirs

        repo = tmp_path / "dataset-incubator"
        for section in ("candidates", "compose", "support_datasets"):
            d = repo / section / "ds"
            d.mkdir(parents=True)
            (d / "dataset.yml").write_text("x", encoding="utf-8")

        assert repo_dataset_dirs(repo) == (
            "candidates",
            "compose",
            "support_datasets",
        )

    @pytest.mark.contract
    def test_excludes_templates_and_hidden(self, tmp_path: Path) -> None:
        """templates/ e dir nascoste non sono sezioni dati."""
        from toolkit.registry.layout import repo_dataset_dirs

        repo = tmp_path / "dataset-incubator"
        (repo / "candidates" / "ds").mkdir(parents=True)
        (repo / "candidates" / "ds" / "dataset.yml").write_text("x", encoding="utf-8")
        (repo / "templates" / "candidate").mkdir(parents=True)
        (repo / "templates" / "candidate" / "dataset.yml").write_text("x", encoding="utf-8")
        (repo / ".github" / "ISSUE_TEMPLATE").mkdir(parents=True)
        (repo / ".github" / "ISSUE_TEMPLATE" / "dataset.yml").write_text("x", encoding="utf-8")

        assert repo_dataset_dirs(repo) == ("candidates",)

    @pytest.mark.contract
    def test_no_sections_returns_default(self, tmp_path: Path) -> None:
        """Repo senza sezioni → default ('datasets',) — compat."""
        from toolkit.registry.layout import repo_dataset_dirs

        repo = tmp_path / "nuovo-repo"
        repo.mkdir()
        (repo / "README.md").write_text("x", encoding="utf-8")

        assert repo_dataset_dirs(repo) == ("datasets",)
