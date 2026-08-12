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
    # role derivato: year ha semantic_type → dimension (non metric)
    roles = {c["name"]: c["role"] for c in ds["columns"]}
    assert roles == {"geo": "dimension", "year": "dimension", "value": "metric"}
    # blocco run NON presente (nessun consumer lo legge dai datasets)
    assert "run" not in ds
    assert "years" not in ds
    assert "registry_source" not in ds


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


@pytest.mark.contract
def test_clean_catalog_slims_editorial_entry_without_parquet(tmp_path: Path) -> None:
    """Entry editoriale (no parquet locale) preservata MA filtrata dalle chiavi morte.

    La path editoriale è l'unico percorso dove le chiavi legacy dell'existing
    (run, years, registry_source) potrebbero rientrare nel registry: senza
    parquet locale l'entry non passa dal builder derivato. Il filtro
    (_slim_dataset_entry) deve comunque applicarsi, così come il fix role
    (year → dimension).
    """
    layout = _make_repo(tmp_path)
    import shutil

    shutil.rmtree(layout.repo_root / "out" / "data" / "clean")

    existing = {
        "datasets": [
            {
                "slug": "my_dataset",
                "name": "My Dataset",
                "description": "Editoriale",
                "source_id": "src1",
                "period": {"start": 2020, "end": 2024},
                "columns": [
                    {"name": "anno", "type": "INTEGER", "role": "metric", "semantic_type": "year"},
                    {"name": "importo", "type": "DOUBLE", "role": "metric"},
                ],
                "location": {"type": "gcs", "path": "gs://dataciviclab-clean/my_dataset/"},
                "stage": "published",
                # chiavi morte legacy: non devono tornare nel registry
                "run": {"run_id": "OLD", "year": 2025, "status": "SUCCESS"},
                "years": [2025],
                "registry_source": "editorial",
            }
        ]
    }

    catalog, errors = build_clean_catalog(layout, existing=existing)
    assert errors == {"derive": [], "validation": []}, f"errori inattesi: {errors}"
    ds = catalog["datasets"][0]
    assert ds["slug"] == "my_dataset"
    # chiavi morte filtrate
    assert "run" not in ds
    assert "years" not in ds
    assert "registry_source" not in ds
    # campi editoriali preservati
    assert ds["stage"] == "published"
    assert ds["description"] == "Editoriale"
    # role corretto anche sull'entry editoriale (year → dimension)
    roles = {c["name"]: c["role"] for c in ds["columns"]}
    assert roles == {"anno": "dimension", "importo": "metric"}


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
    assert mart["table"] == "mart_trend"
    # registry snello: niente run/description/validation rules (non consumati)
    assert "run" not in mart
    assert "description" not in mart
    assert "primary_key" not in mart
    assert "required_columns" not in mart
    assert "min_rows" not in mart


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
    assert sig["run"]["run_id"] == "20260101T000000Z_abc123"
    # registry snello: years del signal rimosso (non consumato)
    assert "years" not in sig


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
    """codelists espone i NOMI dei codelist del repo (contract)."""
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
    assert payload["codelists"] == ["geo", "units"]


@pytest.mark.pure_unit
def test_codelists_empty_without_dir(tmp_path: Path) -> None:
    """Senza dir codelists → lista vuota, nessun errore (opzionale)."""
    from toolkit.registry.builders import build_codelists

    layout = _make_repo(tmp_path)
    payload, errors = build_codelists(layout)
    assert errors == {"derive": [], "validation": []}
    assert payload["codelists"] == []


class TestRegistryExistingRun:
    """build_registry preserva i run storici dei signals da existing (CI).

    Unico pass centralizzato (_merge_existing_runs) sui signals: in CI
    post-merge i dataset non rieseguiti non hanno run record locali. I
    datasets/marts nel registry snello non hanno run (nessun consumer).
    """

    def _build(self, tmp_path, with_run):
        from toolkit.registry.builders import build_registry

        layout = _make_repo(tmp_path, with_run=with_run)
        existing = {
            "datasets": [
                {
                    "slug": "my_dataset",
                    "run": {"run_id": "OLD_dataset", "year": 2025, "status": "SUCCESS"},
                }
            ],
            "marts": [
                {
                    "slug": "my_dataset__mart_trend",
                    "dataset": "my_dataset",
                    "table": "mart_trend",
                    "run": {"run_id": "OLD_mart", "year": 2025, "status": "SUCCESS"},
                }
            ],
        }
        existing_signals = {
            "signals": [
                {
                    "id": "my_dataset",
                    "run": {"run_id": "OLD_signal", "year": 2025, "status": "SUCCESS"},
                },
            ]
        }
        result = build_registry(
            layout,
            existing_catalog=existing,
            existing_signals=existing_signals,
        )
        return result["registry"]

    @pytest.mark.contract
    def test_preserves_signals_run_when_local_missing(self, tmp_path: Path) -> None:
        """CI: nessun run locale → il run dei signals viene da existing."""
        reg = self._build(tmp_path, with_run=False)
        ds = next(d for d in reg["datasets"] if d["slug"] == "my_dataset")
        assert "run" not in ds  # datasets snello: nessun run
        mart = next(m for m in reg["marts"] if m["slug"] == "my_dataset__mart_trend")
        assert "run" not in mart  # marts snello: nessun run
        sig = next(s for s in reg["signals"] if s["id"] == "my_dataset")
        assert sig["run"]["run_id"] == "OLD_signal"

    @pytest.mark.contract
    def test_local_signal_run_wins_over_existing(self, tmp_path: Path) -> None:
        """Run locale presente → vince su existing (non lo sovrascrive)."""
        reg = self._build(tmp_path, with_run=True)
        sig = next(s for s in reg["signals"] if s["id"] == "my_dataset")
        assert sig["run"]["run_id"] == "20260101T000000Z_abc123"  # run locale, non OLD


class TestCanonicalize:
    """Ordine chiavi deterministico nelle entry (diff post-merge puliti).

    Le entry derivano da path diversi (builder, existing, run restore):
    l'ordine di inserimento del dict cambiava a ogni rigenerazione → diff
    rumorosi. _canonicalize_registry finalizza un ordine canonico per sezione;
    le chiavi extra vengono accodate, mai perse.
    """

    def _build_with_existing(self, tmp_path: Path) -> dict:
        from toolkit.registry.builders import build_registry

        layout = _make_repo(tmp_path, with_run=True)
        existing = {
            "datasets": [
                {"slug": "my_dataset", "run": {"run_id": "OLD", "year": 2025, "status": "SUCCESS"}}
            ],
            "marts": [
                {
                    "slug": "my_dataset__mart_trend",
                    "dataset": "my_dataset",
                    "table": "mart_trend",
                    "run": {"run_id": "OLD_mart", "year": 2025, "status": "SUCCESS"},
                }
            ],
        }
        result = build_registry(
            layout,
            existing_catalog=existing,
            existing_signals={"signals": [{"id": "my_dataset", "run": {"run_id": "OLD_sig"}}]},
        )
        return result["registry"]

    @pytest.mark.contract
    def test_mart_canonical_order(self, tmp_path: Path) -> None:
        """Ordine canonico mart: slug, dataset, table, location."""
        reg = self._build_with_existing(tmp_path)
        mart = next(m for m in reg["marts"] if m["slug"] == "my_dataset__mart_trend")
        assert list(mart.keys()) == ["slug", "dataset", "table", "location"]

    @pytest.mark.contract
    def test_dataset_canonical_order(self, tmp_path: Path) -> None:
        reg = self._build_with_existing(tmp_path)
        ds = next(d for d in reg["datasets"] if d["slug"] == "my_dataset")
        keys = list(ds.keys())
        canonical = [
            "slug",
            "name",
            "description",
            "source",
            "source_id",
            "period",
            "tags",
            "category",
            "columns",
            "location",
            "stage",
            "mart_refs",
        ]
        assert keys == canonical

    @pytest.mark.contract
    def test_extra_keys_preserved_at_end(self, tmp_path: Path) -> None:
        """Chiavi non previste dal canonico vengono accodate, non perse."""
        from toolkit.registry.builders import _canonicalize_entry

        entry = {"z_key": 1, "slug": "a", "a_key": 2}
        out = _canonicalize_entry(entry, ("slug", "name"))
        assert list(out.keys()) == ["slug", "z_key", "a_key"]
        assert out["z_key"] == 1 and out["a_key"] == 2

    @pytest.mark.contract
    def test_deterministic_across_regenerations(self, tmp_path: Path) -> None:
        """Due build con existing diverso producono lo stesso ordine chiavi."""
        from toolkit.registry.builders import build_registry

        layout = _make_repo(tmp_path, with_run=True)
        e1 = {
            "datasets": [
                {"slug": "my_dataset", "run": {"run_id": "OLD", "year": 2025, "status": "SUCCESS"}}
            ],
            "marts": [],
        }
        e2 = {
            "datasets": [
                {"slug": "my_dataset", "run": {"run_id": "NEW", "year": 2026, "status": "SUCCESS"}}
            ],
            "marts": [],
        }
        r1 = build_registry(layout, existing_catalog=e1)["registry"]
        r2 = build_registry(layout, existing_catalog=e2)["registry"]
        ds1 = next(d for d in r1["datasets"] if d["slug"] == "my_dataset")
        ds2 = next(d for d in r2["datasets"] if d["slug"] == "my_dataset")
        assert list(ds1.keys()) == list(ds2.keys())


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
        # registry snello: niente summary/generated_from (non consumati)
        assert "summary" not in graph
        assert "generated_from" not in graph


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
        """templates/, dir nascoste e smoke non sono sezioni dati."""
        from toolkit.registry.layout import repo_dataset_dirs

        repo = tmp_path / "dataset-incubator"
        (repo / "candidates" / "ds").mkdir(parents=True)
        (repo / "candidates" / "ds" / "dataset.yml").write_text("x", encoding="utf-8")
        (repo / "templates" / "candidate").mkdir(parents=True)
        (repo / "templates" / "candidate" / "dataset.yml").write_text("x", encoding="utf-8")
        (repo / ".github" / "ISSUE_TEMPLATE").mkdir(parents=True)
        (repo / ".github" / "ISSUE_TEMPLATE" / "dataset.yml").write_text("x", encoding="utf-8")
        (repo / "smoke" / "bdap_http_csv").mkdir(parents=True)
        (repo / "smoke" / "bdap_http_csv" / "dataset.yml").write_text("x", encoding="utf-8")

        assert repo_dataset_dirs(repo) == ("candidates",)

    @pytest.mark.contract
    def test_no_sections_returns_empty(self, tmp_path: Path) -> None:
        """Repo senza sezioni → tuple vuota (non è un repo dati)."""
        from toolkit.registry.layout import repo_dataset_dirs

        repo = tmp_path / "nuovo-repo"
        repo.mkdir()
        (repo / "README.md").write_text("x", encoding="utf-8")

        assert repo_dataset_dirs(repo) == ()


class TestMartMultiYearLocation:
    """Mart con years esplicite → location flat; per-anno → year.

    Regressione issue #463: il runner scrive i mart multi-anno flat
    (data/mart/{dataset}/{table}.parquet), ma il builder li pubblicava con
    path anno inesistente (year=max). Ora tabella con years → flat.
    """

    @pytest.mark.contract
    def test_multi_year_mart_flat(self, tmp_path: Path) -> None:
        """Tabella con years esplicite → path flat (no dir anno)."""
        from toolkit.registry.builders import build_mart_catalog
        from toolkit.registry.layout import RepoLayout
        from toolkit.registry.paths import PathContract

        ds_dir = tmp_path / "datasets" / "my-ds"
        ds_dir.mkdir(parents=True)
        (ds_dir / "dataset.yml").write_text(
            "root: '../../out'\n"
            "dataset:\n"
            "  name: 'my_dataset'\n"
            "  source_id: 'src'\n"
            "  years: [2024, 2025]\n"
            "mart:\n"
            "  tables:\n"
            "    - name: 'mart_trend'\n"
            "      sql: 'sql/mart_trend.sql'\n"
            "      years: [2024, 2025]\n"
            "    - name: 'mart_sintesi'\n"
            "      sql: 'sql/mart_sintesi.sql'\n",
            encoding="utf-8",
        )

        layout = RepoLayout(
            repo_root=tmp_path, dataset_dirs=("datasets",), source_repo="dataciviclab/test"
        )
        contract = PathContract(prefix="test", clean_layout="year", mart_layout="year")
        catalog, errors = build_mart_catalog(layout, path_contract=contract)

        assert not errors["derive"], errors["derive"]
        by_table = {m["table"]: m for m in catalog["marts"]}
        # multi-anno → flat
        assert by_table["mart_trend"]["location"]["path"] == (
            "gs://dataciviclab-mart/test/my_dataset/mart_trend.parquet"
        )
        # per-anno → year
        assert by_table["mart_sintesi"]["location"]["path"] == (
            "gs://dataciviclab-mart/test/my_dataset/2025/mart_sintesi.parquet"
        )
