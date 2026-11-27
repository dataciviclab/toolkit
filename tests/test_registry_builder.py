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

from tests.helpers import write_parquet

import pytest

from toolkit.registry.builders import (
    build_checks,
    build_clean_catalog,
    build_mart_catalog,
    build_registry,
)
from toolkit.registry.layout import RepoLayout
from toolkit.registry.paths import PathContract
from toolkit.registry.runs import last_run

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
clean:
  sql: "sql/clean.sql"
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


def _make_parquet(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_parquet(
        path,
        "CREATE TABLE t (geo VARCHAR, year INTEGER, value DOUBLE); "
        "INSERT INTO t VALUES ('ITC4', 2024, 1.5), ('ITH3', 2024, 2.5)",
    )


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
    _make_parquet(out / "data" / "clean" / name / "2024" / f"{name}_2024_clean.parquet")
    _make_parquet(out / "data" / "clean" / name / "2025" / f"{name}_2025_clean.parquet")
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

    assert errors["derive"] == [] and errors["validation"] == [], f"errori inattesi: {errors}"
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
    # v2: partizioni pubblicate con le righe clean (2 righe per anno nel fixture)
    assert ds["partitions"] == {2024: 2, 2025: 2}
    # last_run: esecuzione reale, proiezione compatta (niente run_id/quality/bytes)
    assert ds["last_run"] == {
        "at": "2026-01-01T00:00:00Z",
        "year": 2024,
        "status": "SUCCESS",
    }
    # v2: niente location per voce (derivata da gcs+partitions), niente stage/source
    assert "location" not in ds
    assert "stage" not in ds
    assert "source" not in ds
    assert "registry_source" not in ds


@pytest.mark.policy
def test_clean_catalog_does_not_narrow_glob_location(tmp_path: Path) -> None:
    """Out/ parziale: una glob esistente non diventa un path puntuale.

    camera_interventi (open-politica): config 2022-2026, out/ locale con il
    solo 2025, pubblicate 5 partizioni. La derivazione v2 da `partitions`
    emetterebbe un punto sul 2025 e nasconderebbe le altre: la location v1
    glob resta come override.
    """
    layout = _make_repo(tmp_path, with_mart=False)
    existing = {
        "schema_version": 1,
        "datasets": [
            {
                "slug": "my_dataset",
                "location": {
                    "type": "gcs",
                    "path": "gs://dataciviclab-clean/my_dataset/*/my_dataset_*_clean.parquet",
                    "multi_file": True,
                },
            }
        ],
    }
    catalog, _ = build_clean_catalog(layout, existing=existing)
    ds = catalog["datasets"][0]
    # out/ locale parziale: una sola partizione nota (il fixture dichiara 2024)
    assert ds["partitions"] == {2024: 2}
    # la glob pubblicata non viene ristretta a un path puntuale
    assert ds["location"].endswith("*/my_dataset_*_clean.parquet")


@pytest.mark.policy
def test_clean_catalog_dirname_not_identity(tmp_path: Path) -> None:
    """La dir con trattini non è identità: la chiave è dataset.name (policy)."""
    layout = _make_repo(tmp_path, dirname="my-dataset")
    catalog, _ = build_clean_catalog(layout)
    assert catalog["datasets"][0]["slug"] == "my_dataset"


@pytest.mark.pure_unit
def test_clean_catalog_missing_parquet_reported(tmp_path: Path) -> None:
    """Dataset con clean.sql ma senza parquet locale → errore, non catalogo finto."""
    layout = _make_repo(tmp_path, with_mart=False)
    import shutil

    shutil.rmtree(layout.repo_root / "out" / "data" / "clean")
    catalog, errors = build_clean_catalog(layout)
    assert any("nessun parquet clean locale" in e for e in errors["derive"])
    assert catalog["datasets"] == []


@pytest.mark.contract
def test_clean_catalog_registers_mart_only_dataset(tmp_path: Path) -> None:
    """Mart-only (no clean.sql né parquet): dataset con mart_refs, senza columns/location."""
    layout = _make_repo(tmp_path)  # MART_YML: nessun clean.sql
    import shutil

    shutil.rmtree(layout.repo_root / "out" / "data" / "clean")
    built = build_registry(layout)

    assert built["errors"]["registry"]["validation"] == []
    ds = built["registry"]["datasets"][0]
    assert ds["slug"] == "my_dataset"
    assert ds["period"] == {"start": 2000, "end": 2024}
    assert ds["mart_refs"] == ["my_dataset__mart_trend"]
    assert "columns" not in ds
    assert "location" not in ds


@pytest.mark.contract
def test_clean_catalog_slims_editorial_entry_without_parquet(tmp_path: Path) -> None:
    """Entry editoriale (no parquet locale) preservata MA filtrata dalle chiavi morte.

    La path editoriale è l'unico percorso dove le chiavi legacy dell'existing
    (run, years, registry_source) potrebbero rientrare nel registry: senza
    parquet locale l'entry non passa dal builder derivato. Il filtro
    (_slim_dataset_entry) deve comunque applicarsi, così come il fix role
    (year → dimension).
    """
    layout = _make_repo(tmp_path, with_mart=False)
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
    assert errors["derive"] == [] and errors["validation"] == [], f"errori inattesi: {errors}"
    ds = catalog["datasets"][0]
    assert ds["slug"] == "my_dataset"
    # chiavi morte v1 filtrate
    assert "run" not in ds
    assert "years" not in ds
    assert "registry_source" not in ds
    assert "stage" not in ds
    # campi editoriali preservati
    assert ds["description"] == "Editoriale"
    # location v1 conservata come override (l'entry non ha partitions)
    assert ds["location"] == "gs://dataciviclab-clean/my_dataset/"
    # role corretto anche sull'entry editoriale (year → dimension)
    roles = {c["name"]: c["role"] for c in ds["columns"]}
    assert roles == {"anno": "dimension", "importo": "metric"}


@pytest.mark.pure_unit
def test_clean_catalog_normalizes_semantic_type_from_editorial(tmp_path: Path) -> None:
    """semantic_type editoriale viene normalizzato: alias → tipo valido, invalido → droppato.

    Il parquet del fixture ha colonne (geo, year, value). L'editorial merge
    sovrascrive solo semantic_type per colonne che esistono nel derivato.
    """
    layout = _make_repo(tmp_path)

    existing = {
        "datasets": [
            {
                "slug": "my_dataset",
                "name": "My Dataset",
                "description": "",
                "source_id": "src1",
                "period": {"start": 2020, "end": 2024},
                "columns": [
                    # alias valido: atto_num → ddl_id (ma "geo" non è un alias, serve uno che matchi)
                    {
                        "name": "year",
                        "type": "INTEGER",
                        "role": "metric",
                        "semantic_type": "atto_num",
                    },
                    # tipo valido: resta invariato
                    {
                        "name": "geo",
                        "type": "VARCHAR",
                        "role": "dimension",
                        "semantic_type": "nuts_code",
                    },
                    # tipo invalido: droppato
                    {
                        "name": "value",
                        "type": "DOUBLE",
                        "role": "metric",
                        "semantic_type": "tipo_fantasma",
                    },
                ],
                "location": {"type": "gcs", "path": "gs://dataciviclab-clean/my_dataset/"},
                "stage": "incubating",
            }
        ]
    }

    catalog, _ = build_clean_catalog(layout, existing=existing)
    ds = catalog["datasets"][0]
    cols = {c["name"]: c for c in ds["columns"]}

    # alias normalizzato: year ha semantic_type "atto_num" → "ddl_id"
    assert cols["year"].get("semantic_type") == "ddl_id"
    # tipo valido preservato
    assert cols["geo"].get("semantic_type") == "nuts_code"
    # tipo invalido droppato
    assert "semantic_type" not in cols["value"]


@pytest.mark.policy
def test_period_time_coverage_beats_editorial(tmp_path: Path) -> None:
    """policy: time_coverage del config vince sull'period editoriale stale.

    Regression energia-italia #6: l'espansione della serie (2015→2000) non
    propagava al registry perché il merge editoriale sovrascriveva sempre
    il period con quello storico.
    """
    layout = _make_repo(tmp_path)
    existing = {
        "datasets": [
            {
                "slug": "my_dataset",
                "name": "My Dataset",
                "description": "",
                "source_id": "src1",
                "period": {"start": 2015, "end": 2024},  # stale
                "columns": [],
                "location": {"type": "gcs", "path": "gs://dataciviclab-clean/my_dataset/"},
                "stage": "incubating",
            }
        ]
    }

    catalog, _ = build_clean_catalog(layout, existing=existing)
    ds = catalog["datasets"][0]

    # MART_YML dichiara time_coverage 2000-2024: deve vincere su 2015-2024
    assert ds["period"] == {"start": 2000, "end": 2024}


@pytest.mark.policy
def test_period_editorial_fills_when_no_time_coverage(tmp_path: Path) -> None:
    """policy: senza time_coverage nel config, l'period editoriale resta la fonte."""
    # with_mart=False → SIMPLE_YML (nessun time_coverage), parquet già nel fixture
    layout = _make_repo(tmp_path, with_mart=False)
    existing = {
        "datasets": [
            {
                "slug": "my_dataset",
                "name": "Simple",
                "description": "",
                "source_id": "src1",
                "period": {"start": 2020, "end": 2026},
                "columns": [],
                "location": {"type": "gcs", "path": "gs://dataciviclab-clean/my_dataset/"},
                "stage": "incubating",
            }
        ]
    }

    catalog, _ = build_clean_catalog(layout, existing=existing)
    ds = catalog["datasets"][0]

    assert ds["period"] == {"start": 2020, "end": 2026}


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
    # v2: layout esplicito (dataset 2024+2025 → glob), niente path per voce
    assert mart["layout"] == "glob"
    assert "year" not in mart
    assert "location" not in mart
    # primary_key dichiarata in dataset.yml (mart.validate.table_rules)
    assert mart["primary_key"] == ["year", "geo"]
    # le regole di validazione non sono discovery metadata
    assert "required_columns" not in mart
    assert "min_rows" not in mart
    assert "description" not in mart


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
    assert any("layout 'year' senza years dichiarati" in e for e in errors["derive"])


# ---------------------------------------------------------------------------
# checks (anomalie)
# ---------------------------------------------------------------------------


@pytest.mark.contract
def test_checks_empty_when_healthy(tmp_path: Path) -> None:
    """Dataset con clean.sql e mart → nessun check (il file non ha rumore)."""
    layout = _make_repo(tmp_path)
    payload, errors = build_checks(layout)

    assert errors == {"derive": [], "validation": []}, f"errori inattesi: {errors}"
    assert payload["checks"] == []


@pytest.mark.policy
def test_checks_warn_without_mart(tmp_path: Path) -> None:
    """Senza mart il dataset è warn (policy), non una riga di stato per dataset."""
    layout = _make_repo(tmp_path, with_mart=False)
    payload, errors = build_checks(layout)
    assert errors == {"derive": [], "validation": []}
    assert payload["checks"] == [
        {
            "id": "my_dataset",
            "status": "warn",
            "detail": "nessun mart",
            "action": "aggiungere mart SQL per completare il dataset",
        }
    ]


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

    # mart per-anno su più anni: stesso trattamento del clean
    mart_loc = contract.mart_location("multi_year", "mart_trend", year=2024, years=[2023, 2024])
    assert mart_loc["multi_file"] is True
    assert mart_loc["path"] == "gs://dataciviclab-mart/multi_year/*/mart_trend.parquet"


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
# last_run (pure_unit)
# ---------------------------------------------------------------------------


@pytest.mark.pure_unit
def test_last_run_mapping() -> None:
    """Solo i tre fatti letti dai consumer: niente run_id/quality/bytes/durate."""
    assert last_run(RUN_RECORD) == {
        "at": "2026-01-01T00:00:00Z",
        "year": 2024,
        "status": "SUCCESS",
    }


@pytest.mark.pure_unit
def test_last_run_none_for_incomplete() -> None:
    assert last_run(None) is None
    assert last_run({"year": 2024, "status": "SUCCESS"}) is None  # manca started_at
    assert (
        last_run({"started_at": "2026-01-01T00:00:00Z", "status": "SUCCESS"}) is None
    )  # manca year


@pytest.mark.pure_unit
def test_registry_builder_imports() -> None:
    """Export pubblico del package (contract)."""
    import toolkit.registry as reg

    assert all(
        hasattr(reg, name)
        for name in ("build_clean_catalog", "build_mart_catalog", "build_checks", "RepoLayout")
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


class TestRegistryExistingState:
    """build_registry preserva lo stato delle entry non rieseguite (CI).

    In CI post-merge i dataset non rieseguiti non hanno parquet né run record
    locali: ``partitions`` e ``last_run`` vengono dall'existing. Il locale
    vince sempre sulla stessa chiave.
    """

    @staticmethod
    def _existing(partitions: dict[str, int], last_run_block: dict) -> dict:
        return {
            "schema_version": 2,
            "datasets": [
                {
                    "slug": "my_dataset",
                    "partitions": partitions,
                    "last_run": last_run_block,
                }
            ],
            "marts": [],
        }

    @pytest.mark.contract
    def test_preserves_last_run_when_local_missing(self, tmp_path: Path) -> None:
        """CI: nessun run locale → il last_run viene da existing."""
        from toolkit.registry.builders import build_registry

        layout = _make_repo(tmp_path, with_run=False)
        existing = self._existing(
            {"2023": 40},
            {"at": "2025-12-31T00:00:00Z", "year": 2023, "status": "SUCCESS"},
        )
        reg = build_registry(layout, existing=existing)["registry"]
        ds = next(d for d in reg["datasets"] if d["slug"] == "my_dataset")
        assert ds["last_run"] == {
            "at": "2025-12-31T00:00:00Z",
            "year": 2023,
            "status": "SUCCESS",
        }

    @pytest.mark.contract
    def test_preserves_whole_entry_when_partitions_but_no_parquet(self, tmp_path: Path) -> None:
        """Entry già pubblicata e non rieseguita: si preserva INTERA, anche con partitions.

        Regressione dataset-incubator#959: in una run post-merge selettiva
        ``out/`` contiene solo il dataset appena eseguito. Per ogni altro
        dataset ``columns`` è None, ma le ``partitions`` arrivano dall'existing;
        il guard ``not partitions`` escludeva la preservazione, l'entry veniva
        ricostruita dal manifest e perdeva ``columns`` (``gse_cer_elenco``:
        12 → 0) e la ``location`` puntuale, che il blocco no-narrowing non
        recupera (salva solo le glob). Copiare le sole colonne non basta:
        ``location`` e ``mart_refs`` resterebbero fuori.
        """
        import shutil

        from toolkit.registry.builders import build_registry

        layout = _make_repo(tmp_path, with_mart=False)
        # Run selettiva: nessun parquet clean locale per questo dataset.
        shutil.rmtree(layout.repo_root / "out" / "data" / "clean")

        existing = {
            "schema_version": 2,
            "datasets": [
                {
                    "slug": "my_dataset",
                    "name": "My Dataset",
                    "partitions": {"2024": 40},
                    "columns": [
                        {"name": "anno", "type": "INTEGER", "role": "dimension"},
                        {"name": "importo", "type": "DOUBLE", "role": "metric"},
                    ],
                    "location": (
                        "gs://dataciviclab-clean/my_dataset/2024/my_dataset_2024_clean.parquet"
                    ),
                    "last_run": {"at": "2026-01-01T00:00:00Z", "year": 2024, "status": "SUCCESS"},
                    # chiave morta v1: la preservazione passa da _slim_dataset_entry
                    "stage": "published",
                }
            ],
            "marts": [],
        }

        reg = build_registry(layout, existing=existing)["registry"]
        ds = next(d for d in reg["datasets"] if d["slug"] == "my_dataset")
        assert ds["columns"] == existing["datasets"][0]["columns"]
        assert ds["location"] == existing["datasets"][0]["location"]
        assert ds["partitions"] == {2024: 40}
        assert ds["last_run"] == existing["datasets"][0]["last_run"]
        assert "stage" not in ds

    @pytest.mark.contract
    def test_preserves_last_run_from_v1_signals(self, tmp_path: Path) -> None:
        """Primo build v2: la freschezza di un registry v1 arriva dai signals."""
        from toolkit.registry.builders import build_registry

        layout = _make_repo(tmp_path, with_run=False)
        existing = {
            "schema_version": 1,
            "datasets": [{"slug": "my_dataset", "location": "gs://bucket/x.parquet"}],
            "signals": [
                {
                    "id": "compose:my_dataset",
                    "label": "my_dataset",
                    "run": {
                        "run_id": "r1",
                        "year": 2025,
                        "status": "SUCCESS",
                        "started_at": "2026-02-01T00:00:00Z",
                    },
                }
            ],
        }
        reg = build_registry(layout, existing=existing)["registry"]
        ds = next(d for d in reg["datasets"] if d["slug"] == "my_dataset")
        assert ds["last_run"] == {
            "at": "2026-02-01T00:00:00Z",
            "year": 2025,
            "status": "SUCCESS",
        }

    @pytest.mark.contract
    def test_local_last_run_wins_over_existing(self, tmp_path: Path) -> None:
        """Run locale presente → vince su existing."""
        from toolkit.registry.builders import build_registry

        layout = _make_repo(tmp_path, with_run=True)
        existing = self._existing(
            {"2023": 40},
            {"at": "2025-12-31T00:00:00Z", "year": 2023, "status": "SUCCESS"},
        )
        reg = build_registry(layout, existing=existing)["registry"]
        ds = next(d for d in reg["datasets"] if d["slug"] == "my_dataset")
        assert ds["last_run"]["at"] == "2026-01-01T00:00:00Z"
        assert ds["last_run"]["year"] == 2024

    @pytest.mark.contract
    def test_partitions_merged_with_existing(self, tmp_path: Path) -> None:
        """Le partizioni non rieseguite restano: union, locale vince."""
        from toolkit.registry.builders import build_registry

        layout = _make_repo(tmp_path, with_run=True)
        existing = self._existing({"2023": 40, "2024": 999}, {})
        reg = build_registry(layout, existing=existing)["registry"]
        ds = next(d for d in reg["datasets"] if d["slug"] == "my_dataset")
        assert ds["partitions"] == {2023: 40, 2024: 2, 2025: 2}


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
                {"slug": "my_dataset", "partitions": {"2023": 5}},
            ],
            "marts": [],
        }
        result = build_registry(layout, existing=existing)
        return result["registry"]

    @pytest.mark.contract
    def test_mart_canonical_order(self, tmp_path: Path) -> None:
        """Ordine canonico mart: slug, dataset, table, layout, primary_key."""
        reg = self._build_with_existing(tmp_path)
        mart = next(m for m in reg["marts"] if m["slug"] == "my_dataset__mart_trend")
        assert list(mart.keys()) == ["slug", "dataset", "table", "layout", "primary_key"]

    @pytest.mark.contract
    def test_dataset_canonical_order(self, tmp_path: Path) -> None:
        reg = self._build_with_existing(tmp_path)
        ds = next(d for d in reg["datasets"] if d["slug"] == "my_dataset")
        keys = list(ds.keys())
        canonical = [
            "slug",
            "name",
            "description",
            "source_id",
            "tags",
            "category",
            "period",
            "partitions",
            "columns",
            "mart_refs",
            "last_run",
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
            "datasets": [{"slug": "my_dataset", "partitions": {"2023": 1}}],
            "marts": [],
        }
        e2 = {
            "datasets": [{"slug": "my_dataset", "partitions": {"2022": 1}}],
            "marts": [],
        }
        r1 = build_registry(layout, existing=e1)["registry"]
        r2 = build_registry(layout, existing=e2)["registry"]
        ds1 = next(d for d in r1["datasets"] if d["slug"] == "my_dataset")
        ds2 = next(d for d in r2["datasets"] if d["slug"] == "my_dataset")
        assert list(ds1.keys()) == list(ds2.keys())


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
    """Mart con years esplicite → flat; mart per-anno → glob multi-anno.

    Regressione issue #463: il runner scrive i mart multi-anno flat
    (data/mart/{dataset}/{table}.parquet), ma il builder li pubblicava con
    path anno inesistente (year=max). Ora tabella con years → flat.

    Regressione mart per-anno: un path puntuale all'anno massimo esponeva un
    solo anno ai consumer del registry (dcl-mcp). Ora glob su tutti gli anni.
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
        # multi-anno (years esplicite) → flat, un file
        assert by_table["mart_trend"]["layout"] == "flat"
        assert "location" not in by_table["mart_trend"]
        # per-anno su dataset multi-anno → glob su tutti gli anni
        assert by_table["mart_sintesi"]["layout"] == "glob"
        assert "year" not in by_table["mart_sintesi"]

    @pytest.mark.contract
    def test_per_year_mart_single_year_stays_exact(self, tmp_path: Path) -> None:
        """Mart per-anno su dataset mono-anno → path puntuale, multi_file False."""
        from toolkit.registry.builders import build_mart_catalog
        from toolkit.registry.layout import RepoLayout
        from toolkit.registry.paths import PathContract

        ds_dir = tmp_path / "datasets" / "one-year"
        ds_dir.mkdir(parents=True)
        (ds_dir / "dataset.yml").write_text(
            "root: '../../out'\n"
            "dataset:\n"
            "  name: 'one_year'\n"
            "  source_id: 'src'\n"
            "  years: [2024]\n"
            "mart:\n"
            "  tables:\n"
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
        mart = catalog["marts"][0]
        assert mart["layout"] == "year"
        assert mart["year"] == 2024
        assert "location" not in mart


@pytest.mark.contract
def test_internal_mart_fuori_dal_contratto_pubblico(tmp_path: Path) -> None:
    """`internal: true` = staging: materializzata, ma fuori da mart_refs/marts.

    Il parquet non viene caricato su GCS: se finisse nel registry il catalogo
    annuncerebbe un oggetto inesistente. Il caso reale è lo staging condiviso
    di un compose (una lettura unica di un support usata da nodi, archi e
    copertura).
    """
    ds_dir = tmp_path / "compose" / "demo"
    ds_dir.mkdir(parents=True)
    (ds_dir / "dataset.yml").write_text(
        "root: '../../out'\n"
        "dataset:\n"
        "  name: demo\n"
        "  years: [2024]\n"
        "mart:\n"
        "  tables:\n"
        "    - name: stg_base\n"
        "      sql: 'sql/stg_base.sql'\n"
        "      internal: true\n"
        "    - name: mart_pubblico\n"
        "      sql: 'sql/mart_pubblico.sql'\n",
        encoding="utf-8",
    )
    layout = RepoLayout(
        repo_root=tmp_path, dataset_dirs=("compose",), source_repo="dataciviclab/test"
    )

    catalog, errors = build_clean_catalog(layout)
    assert not errors["derive"], errors["derive"]
    ds = catalog["datasets"][0]
    assert ds["mart_refs"] == ["demo__mart_pubblico"]

    contract = PathContract(prefix="test", clean_layout="year", mart_layout="year")
    marts_catalog, mart_errors = build_mart_catalog(layout, path_contract=contract)
    assert not mart_errors["derive"], mart_errors["derive"]
    assert [m["table"] for m in marts_catalog["marts"]] == ["mart_pubblico"]
