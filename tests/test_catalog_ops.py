"""Test contratto per CatalogResolver (condiviso CLI+MCP).

Protegge:
- Risoluzione slug → path parquet su GCS
- Ricerca dataset per testo e layer
- Schema DuckDB su parquet GCS (con mock)
- Filtri layer (clean/mart)
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from lab_connectors.gcs.paths import CLEAN_BUCKET, MART_BUCKET
from toolkit.domain.catalog import (
    CatalogResolver,
    _parse_clean_filename,
    _scan_committed_catalogs,
)

pytestmark = pytest.mark.contract

# ---------------------------------------------------------------------------
# Helper: manifest finto
# ---------------------------------------------------------------------------


class TestParseCleanFilename:
    def test_standard(self) -> None:
        """{slug}_{year}_clean.parquet → (slug, year)."""
        assert _parse_clean_filename("anac_bandi_gara_2024_clean.parquet") == (
            "anac_bandi_gara",
            2024,
        )

    def test_multi_underscore_slug(self) -> None:
        """Slug con piu' underscore."""
        assert _parse_clean_filename("popolazione_istat_comunale_2019_2025_2024_clean.parquet") == (
            "popolazione_istat_comunale_2019_2025",
            2024,
        )

    def test_not_clean(self) -> None:
        """File che non termina con _clean.parquet → None."""
        assert _parse_clean_filename("pipeline_run.json") is None
        assert _parse_clean_filename("data.parquet") is None

    def test_bad_year(self) -> None:
        """Anno non a 4 cifre → None."""
        assert _parse_clean_filename("slug_999_clean.parquet") is None

    def test_no_slug(self) -> None:
        """Solo anno → None."""
        assert _parse_clean_filename("_2024_clean.parquet") is None


def _make_manifest_entry(
    slug: str,
    bucket: str,
    year: int | None = 2024,
    path: str | None = None,
    size: int = 1000,
) -> dict[str, Any]:
    """Crea un entry finto del manifest."""
    if path is None:
        fname = f"{slug}_{year}_clean.parquet" if bucket == CLEAN_BUCKET else f"mart_{slug}.parquet"
        path = f"{slug}/{year}/{fname}"
    return {
        "url": f"s3://{bucket}/{path}",
        "slug": slug,
        "bucket": bucket,
        "year": year,
        "path": path,
        "size_bytes": size,
        "updated": "2026-07-28T08:00:00Z",
    }


def _make_manifest(files: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "generated_at": "2026-07-28T08:00:00Z",
        "file_count": len(files),
        "total_size_bytes": sum(f.get("size_bytes", 0) for f in files),
        "buckets": [CLEAN_BUCKET, MART_BUCKET],
        "files": files,
    }


_FAKE_MANIFEST = _make_manifest(
    [
        # Clean datasets
        _make_manifest_entry("anac_bandi_gara", CLEAN_BUCKET, 2024),
        _make_manifest_entry("anac_bandi_gara", CLEAN_BUCKET, 2023),
        _make_manifest_entry("anac_aggiudicazioni", CLEAN_BUCKET, 2024),
        _make_manifest_entry("terna_electricity_by_source", CLEAN_BUCKET, 2024),
        _make_manifest_entry("terna_electricity_by_source", CLEAN_BUCKET, 2023),
        _make_manifest_entry("popolazione_istat_comunale_2019_2025", CLEAN_BUCKET, 2024),
        # Mart datasets
        _make_manifest_entry(
            "anac_bandi_gara", MART_BUCKET, 2024, path="anac_bandi_gara/2024/mart_top_sa.parquet"
        ),
        _make_manifest_entry(
            "terna_electricity_by_source",
            MART_BUCKET,
            2024,
            path="terna_electricity_by_source/2024/mart_mensile.parquet",
        ),
        _make_manifest_entry(
            "popolazione_istat_comunale_2019_2025",
            MART_BUCKET,
            2025,
            path="popolazione_istat_comunale_2019_2025/2025/popolazione_by_comune.parquet",
        ),
        # Non-data file (escluso dai risultati)
        {
            "url": "s3://dataciviclab-clean/anac_bandi_gara/2024/pipeline_run.json",
            "slug": "anac_bandi_gara",
            "bucket": CLEAN_BUCKET,
            "year": 2024,
            "path": "anac_bandi_gara/2024/pipeline_run.json",
            "size_bytes": 500,
            "updated": "2026-07-28T08:00:00Z",
        },
    ]
)


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def resolver(monkeypatch: pytest.MonkeyPatch) -> CatalogResolver:
    """CatalogResolver con manifest mockato, senza scan locale."""
    resolver = CatalogResolver(
        manifest_url="http://fake/manifest.json",
        include_local=False,
    )

    def _fake_gcs_files(workspace=None):
        return _FAKE_MANIFEST["files"]

    monkeypatch.setattr("toolkit.domain.catalog._gcs_files_from_registry", _fake_gcs_files)
    return resolver


@pytest.fixture
def resolver_with_local(monkeypatch: pytest.MonkeyPatch) -> CatalogResolver:
    """CatalogResolver con manifest mockato E scan locale mockata."""
    resolver = CatalogResolver(
        manifest_url="http://fake/manifest.json",
        include_local=True,
    )

    def _fake_gcs_files(workspace=None):
        return _FAKE_MANIFEST["files"]

    monkeypatch.setattr("toolkit.domain.catalog._gcs_files_from_registry", _fake_gcs_files)

    # Mock della scan locale: restituisce un dataset locale aggiuntivo
    fake_local = [
        {
            "url": "/tmp/fake/workspace/dataset-incubator/candidates/mio_dataset_locale/data/clean/mio_dataset_locale/2024/mio_dataset_locale_2024_clean.parquet",
            "slug": "mio_dataset_locale",
            "bucket": "local",
            "year": 2024,
            "path": "dataset-incubator/candidates/mio_dataset_locale/data/clean/mio_dataset_locale/2024/mio_dataset_locale_2024_clean.parquet",
            "size_bytes": 5000,
            "updated": "2026-07-28T10:00:00Z",
            "_local": True,
        },
    ]

    monkeypatch.setattr(
        "toolkit.domain.catalog._scan_workspace_parquets",
        lambda _workspace: fake_local,
    )
    # Mock anche scan configs per il dataset locale
    monkeypatch.setattr(
        "toolkit.domain.catalog._scan_workspace_configs",
        lambda _workspace, stage="all": {
            "mio_dataset_locale": {
                "dataset_name": "mio_dataset_locale",
                "stage": "candidates",
                "years": [2024],
                "has_clean": True,
                "has_mart": False,
                "last_run_status": None,
                "config_path": "/tmp/fake/dataset.yml",
                "root": "/tmp/fake",
            },
        },
    )
    # Catalogo semantico committato: isolato nei test (nessuna semantica)
    monkeypatch.setattr("toolkit.domain.catalog._scan_committed_catalogs", lambda _workspace: {})
    return resolver


# Catalogo semantico fake: arricchisce entry già presenti nel manifest.
_FAKE_SEMANTIC: dict[str, dict[str, Any]] = {
    "anac_bandi_gara": {
        "slug": "anac_bandi_gara",
        "name": "Bandi di gara ANAC",
        "description": "Bandi di gara pubblici",
        "tags": ["appalti"],
        "category": "appalti",
        "period": {"start": 2023, "end": 2024},
        "columns": [
            {
                "name": "cig_code",
                "type": "VARCHAR",
                "role": "dimension",
                "description": "Codice CIG",
            },
            {"name": "importo", "type": "DOUBLE", "role": "metric", "description": "Importo gara"},
        ],
        "_repo": "dataset-incubator",
    },
    "terna_electricity_by_source": {
        "slug": "terna_electricity_by_source",
        "name": "Elettricità per fonte",
        "description": "Produzione elettrica per fonte",
        "tags": ["energia"],
        "category": "energia",
        "period": {"start": 2023, "end": 2024},
        "columns": [
            {"name": "fonte", "type": "VARCHAR", "role": "dimension", "description": ""},
            {"name": "gwh", "type": "DOUBLE", "role": "metric", "description": "GWh prodotti"},
        ],
        "_repo": "dataset-incubator",
    },
    "popolazione_istat_comunale_2019_2025": {
        "slug": "popolazione_istat_comunale_2019_2025",
        "name": "Popolazione comunale",
        "description": "Popolazione residente per comune",
        "tags": [],
        "category": "popolazione",
        "columns": [{"name": "comune", "type": "VARCHAR", "role": "dimension", "description": ""}],
        "_repo": "dataset-incubator",
    },
}


@pytest.fixture
def resolver_semantic(monkeypatch: pytest.MonkeyPatch) -> CatalogResolver:
    """Resolver con manifest mockato E catalogo semantico mockato."""
    resolver = CatalogResolver(
        manifest_url="http://fake/manifest.json",
        include_local=True,
    )

    def _fake_gcs_files(workspace=None):
        return _FAKE_MANIFEST["files"]

    monkeypatch.setattr("toolkit.domain.catalog._gcs_files_from_registry", _fake_gcs_files)
    monkeypatch.setattr("toolkit.domain.catalog._scan_workspace_parquets", lambda _w: [])
    monkeypatch.setattr(
        "toolkit.domain.catalog._scan_workspace_configs", lambda _w, stage="all": {}
    )
    monkeypatch.setattr(
        "toolkit.domain.catalog._scan_committed_catalogs", lambda _w: _FAKE_SEMANTIC
    )
    return resolver


# ---------------------------------------------------------------------------
# list_datasets
# ---------------------------------------------------------------------------


class TestListDatasets:
    def test_list_all(self, resolver: CatalogResolver) -> None:
        """list_datasets senza filtri restituisce tutti i dataset unici."""
        res = resolver.list_datasets()
        datasets = res["datasets"]
        slugs = {d["slug"] for d in datasets}
        assert slugs == {
            "anac_bandi_gara",
            "anac_aggiudicazioni",
            "terna_electricity_by_source",
            "popolazione_istat_comunale_2019_2025",
        }
        # pipeline_run.json non deve apparire
        assert res["total_count"] == 4
        assert res["truncated"] is False

    @pytest.mark.regression
    def test_merge_gcs_workspace_mixed_years(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Merge source=all con file GCS senza anno + parquet locale non crasha.

        Regressione: i file GCS dai registry hanno year=None (path flat), lo
        scan workspace int → sorted(set{years}) crashava con TypeError
        ('<' int vs NoneType). Il None non deve entrare nel set years.
        """
        resolver = CatalogResolver(include_local=True)
        # GCS: file senza anno (dal registry)
        monkeypatch.setattr(
            "toolkit.domain.catalog._gcs_files_from_registry",
            lambda _w: [
                {
                    "url": "gs://dataciviclab-clean/mio_dataset/*.parquet",
                    "slug": "mio_dataset",
                    "bucket": CLEAN_BUCKET,
                    "year": None,
                    "path": "gs://dataciviclab-clean/mio_dataset/*.parquet",
                    "_gcs": True,
                }
            ],
        )
        # Workspace: parquet locale con anno
        monkeypatch.setattr(
            "toolkit.domain.catalog._scan_workspace_parquets",
            lambda _w: [
                {
                    "url": "/tmp/mio_dataset_2024_clean.parquet",
                    "slug": "mio_dataset",
                    "bucket": "local",
                    "year": 2024,
                    "path": "mio_dataset/2024/mio_dataset_2024_clean.parquet",
                    "size_bytes": 100,
                    "updated": "2026-01-01T00:00:00Z",
                    "_local": True,
                }
            ],
        )
        monkeypatch.setattr("toolkit.domain.catalog._scan_workspace_configs", lambda _w, **_: {})
        monkeypatch.setattr("toolkit.domain.catalog._scan_committed_catalogs", lambda _w: {})

        res = resolver.list_datasets(query="mio_dataset", source="all")
        assert res["total_count"] == 1
        entry = res["datasets"][0]
        assert entry["years"] == [2024]  # solo anni reali, nessun None
        assert entry["has_remote"] is True
        assert entry["has_local"] is True

    def test_list_by_query(self, resolver: CatalogResolver) -> None:
        """Filtro query per slug (case-insensitive, substring)."""
        res = resolver.list_datasets(query="ANAC")
        slugs = [d["slug"] for d in res["datasets"]]
        assert all("anac" in s for s in slugs)
        assert "terna_electricity_by_source" not in slugs
        # "ANAC" matcha sia anac_bandi_gara che anac_aggiudicazioni
        assert res["total_count"] == 2
        assert res["truncated"] is False

    def test_list_by_layer_clean(self, resolver: CatalogResolver) -> None:
        """layer='clean' restituisce solo dataset nel bucket clean."""
        res = resolver.list_datasets(layer="clean")
        for d in res["datasets"]:
            assert d["layer"] == "clean"

    def test_list_by_layer_mart(self, resolver: CatalogResolver) -> None:
        """layer='mart' restituisce solo dataset nel bucket mart."""
        res = resolver.list_datasets(layer="mart")
        for d in res["datasets"]:
            assert d["layer"] == "mart"

    def test_list_limit_truncates(self, resolver: CatalogResolver) -> None:
        """Con limit basso, i risultati sono troncati e truncated=True."""
        res = resolver.list_datasets(limit=2)
        assert len(res["datasets"]) == 2
        assert res["total_count"] == 4  # 4 slug nel finto manifest
        assert res["truncated"] is True

    def test_list_limit_zero(self, resolver: CatalogResolver) -> None:
        """limit=0 restituisce tutto senza troncare."""
        res = resolver.list_datasets(limit=0)
        assert len(res["datasets"]) == res["total_count"]  # nessun taglio
        assert res["truncated"] is False

    def test_list_layer_detection(self, resolver: CatalogResolver) -> None:
        """Con layer=None, la detection rileva i bucket reali di ogni slug."""
        res = resolver.list_datasets()
        lookup = {d["slug"]: d["layer"] for d in res["datasets"]}

        # anac_bandi_gara ha sia clean che mart nel manifest finto
        assert lookup.get("anac_bandi_gara") == "clean,mart"
        # anac_aggiudicazioni ha solo clean
        assert lookup.get("anac_aggiudicazioni") == "clean"

    def test_list_empty_query(self, resolver: CatalogResolver) -> None:
        """Query senza match restituisce lista vuota, total_count=0."""
        res = resolver.list_datasets(query="zzz_nonexistent")
        assert res["datasets"] == []
        assert res["total_count"] == 0
        assert res["truncated"] is False

    def test_list_years_aggregated(self, resolver: CatalogResolver) -> None:
        """Gli anni sono aggregati correttamente per slug."""
        res = resolver.list_datasets(query="anac_bandi_gara")
        assert len(res["datasets"]) == 1
        entry = res["datasets"][0]
        assert set(entry["years"]) == {2023, 2024}
        assert entry["file_count"] == 3  # 2 clean + 1 mart
        assert res["total_count"] == 1
        assert res["truncated"] is False


# ---------------------------------------------------------------------------
# resolve_slug
# ---------------------------------------------------------------------------


class TestResolveSlug:
    def test_resolve_clean(self, resolver: CatalogResolver) -> None:
        """resolve_slug per slug clean restituisce solo parquet clean."""
        files = resolver.resolve_slug("anac_bandi_gara", layer="clean")
        assert len(files) == 2  # 2023 + 2024
        for f in files:
            assert f["bucket"] == CLEAN_BUCKET
            assert f["path"].endswith(".parquet")

    def test_resolve_mart(self, resolver: CatalogResolver) -> None:
        """resolve_slug per slug mart restituisce solo parquet mart."""
        files = resolver.resolve_slug("anac_bandi_gara", layer="mart")
        assert len(files) == 1
        assert files[0]["bucket"] == MART_BUCKET

    def test_resolve_all_layers(self, resolver: CatalogResolver) -> None:
        """resolve_slug senza layer restituisce clean + mart."""
        files = resolver.resolve_slug("anac_bandi_gara")
        assert len(files) == 3  # 2 clean + 1 mart

    def test_resolve_by_year(self, resolver: CatalogResolver) -> None:
        """Filtro year restituisce solo file dell'anno."""
        files = resolver.resolve_slug("anac_bandi_gara", year=2023)
        assert len(files) == 1
        assert files[0]["year"] == 2023

    def test_resolve_not_found(self, resolver: CatalogResolver) -> None:
        """Slug inesistente solleva FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            resolver.resolve_slug("slug_che_non_esiste")

    def test_resolve_sorted_by_year_desc(self, resolver: CatalogResolver) -> None:
        """I risultati sono ordinati per anno discendente."""
        files = resolver.resolve_slug("terna_electricity_by_source", layer="clean")
        years = [f["year"] for f in files]
        assert years == sorted(years, reverse=True)


# ---------------------------------------------------------------------------
# describe_slug — con DuckDB mock
# ---------------------------------------------------------------------------


class TestDescribeSlug:
    def test_describe_with_mock(
        self, resolver: CatalogResolver, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """describe_slug restituisce struttura con slug, year, layer, columns."""
        fake_preview = {
            "path": "s3://dataciviclab-clean/anac_bandi_gara/2024/anac_bandi_gara_2024_clean.parquet",
            "column_count": 3,
            "columns": [
                {"name": "cig", "type": "VARCHAR"},
                {"name": "anno", "type": "INTEGER"},
                {"name": "importo", "type": "DOUBLE"},
            ],
            "row_count": 100,
            "preview": [{"cig": "ABC123", "anno": 2024, "importo": 50000.0}],
            "truncated": False,
            "slug": "anac_bandi_gara",
            "year": 2024,
        }

        monkeypatch.setattr(
            "toolkit.domain.catalog.parquet_preview", lambda path, limit=5: fake_preview
        )

        # Default layer="clean"
        result = resolver.describe_slug("anac_bandi_gara")
        assert result["slug"] == "anac_bandi_gara"
        assert result["year"] == 2024
        assert result["layer"] == "clean"
        assert result["column_count"] == 3
        assert result["row_count"] == 100

        # Layer esplicito
        result_mart = resolver.describe_slug("anac_bandi_gara", layer="mart")
        assert result_mart["slug"] == "anac_bandi_gara"
        assert result_mart["layer"] == "mart"

    def test_describe_not_found(self, resolver: CatalogResolver) -> None:
        """describe_slug per slug inesistente solleva eccezione."""
        with pytest.raises(FileNotFoundError):
            resolver.describe_slug("slug_che_non_esiste")


# ---------------------------------------------------------------------------
# Local workspace merge
# ---------------------------------------------------------------------------


class TestLocalMerge:
    def test_find_gcs_only(self, resolver: CatalogResolver) -> None:
        """source='gcs' restituisce solo dataset GCS."""
        res = resolver.list_datasets(source="gcs", limit=0)
        slugs = {d["slug"] for d in res["datasets"]}
        assert "anac_bandi_gara" in slugs
        # locale mock non incluso
        assert "mio_dataset_locale" not in slugs

    def test_find_workspace_only(self, resolver_with_local: CatalogResolver) -> None:
        """source='workspace' restituisce solo dataset workspace."""
        # Usa query vuota per avere tutti
        res = resolver_with_local.list_datasets(source="workspace", query="", limit=0)
        slugs = {d["slug"] for d in res["datasets"]}
        assert "mio_dataset_locale" in slugs
        # GCS non incluso (source=workspace)
        assert "anac_bandi_gara" not in slugs

    def test_list_includes_local(self, resolver_with_local: CatalogResolver) -> None:
        """list_datasets include dataset dal workspace locale."""
        res = resolver_with_local.list_datasets(query="mio_dataset_locale")
        assert len(res["datasets"]) == 1
        d = res["datasets"][0]
        assert d["slug"] == "mio_dataset_locale"
        assert d["has_local"] is True
        assert d["has_remote"] is False
        assert d["layer"] in ("clean",)

    def test_list_merges_local_and_gcs(self, resolver_with_local: CatalogResolver) -> None:
        """list_datasets unisce slug locali e GCS (source='all')."""
        res = resolver_with_local.list_datasets(source="all", query="", limit=10)
        slugs = {d["slug"] for d in res["datasets"]}
        assert "anac_bandi_gara" in slugs  # GCS
        assert "mio_dataset_locale" in slugs  # locale

    def test_resolve_prefers_local(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """resolve_slug preferisce file locali su stessi slug."""
        # Crea resolver senza fixture per controllo totale
        resolver = CatalogResolver(
            manifest_url="http://fake/manifest.json",
            include_local=True,
        )

        def _fake_gcs_files(workspace=None):
            return _FAKE_MANIFEST["files"]

        monkeypatch.setattr("toolkit.domain.catalog._gcs_files_from_registry", _fake_gcs_files)

        # Mock scan parquet locale: stesso slug del GCS (anac_bandi_gara)
        monkeypatch.setattr(
            "toolkit.domain.catalog._scan_workspace_parquets",
            lambda _workspace: [
                {
                    "url": "/tmp/fake/anac_bandi_gara_2024_clean.parquet",
                    "slug": "anac_bandi_gara",
                    "bucket": "local",
                    "year": 2024,
                    "path": "anac_bandi_gara/2024/anac_bandi_gara_2024_clean.parquet",
                    "size_bytes": 999,
                    "updated": "2026-07-28T10:00:00Z",
                    "_local": True,
                },
            ],
        )
        # Mock scan configs vuota (non serve per resolve_slug)
        monkeypatch.setattr(
            "toolkit.domain.catalog._scan_workspace_configs",
            lambda _workspace, stage="all": {},
        )

        files = resolver.resolve_slug("anac_bandi_gara", year=2024)
        # Il locale deve venire prima (priorita')
        assert _is_local(files[0]), f"Primo file non locale: {files[0]}"
        # Deve avere size=999 (locale), non 1000 (GCS)
        assert files[0]["size_bytes"] == 999

    def test_describe_local_returns_local_flag(
        self, resolver_with_local: CatalogResolver, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """describe_slug su slug locale include _local=True."""
        fake_preview = {
            "path": "/tmp/fake/local.parquet",
            "column_count": 2,
            "columns": [{"name": "x", "type": "INTEGER"}],
            "row_count": 10,
            "preview": [{"x": 1}],
            "truncated": False,
        }
        monkeypatch.setattr(
            "toolkit.domain.catalog.parquet_preview", lambda path, limit=5: fake_preview
        )

        result = resolver_with_local.describe_slug("mio_dataset_locale")
        assert result["slug"] == "mio_dataset_locale"
        assert result["_local"] is True


def _is_local(file: dict) -> bool:
    return file.get("_local", False) or file.get("bucket") == "local"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_non_data_files_excluded(self, resolver: CatalogResolver) -> None:
        """pipeline_run.json e altri non-parquet sono esclusi."""
        files = resolver.resolve_slug("anac_bandi_gara", layer="clean")
        paths = [f["path"] for f in files]
        assert all(p.endswith(".parquet") for p in paths)
        assert not any("pipeline_run" in p for p in paths)

    def test_resolver_cache_independence(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Due resolver non condividono la cache dei file GCS."""
        r1 = CatalogResolver()
        r2 = CatalogResolver()

        calls: list[int] = []

        def tracking_gcs_files(workspace=None):
            calls.append(1)
            return _FAKE_MANIFEST["files"]

        monkeypatch.setattr("toolkit.domain.catalog._gcs_files_from_registry", tracking_gcs_files)

        r1.list_datasets(query="anac")
        r2.list_datasets(query="anac")

        # Ogni resolver fa la propria scansione
        assert len(calls) == 2

    def test_gcs_cache(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Due chiamate a list_datasets producono una sola scansione registry.

        Regression: _gcs_files deve cacheare come _load_local.
        """
        call_count = 0

        def counting_gcs_files(workspace=None):
            nonlocal call_count
            call_count += 1
            return _FAKE_MANIFEST["files"]

        monkeypatch.setattr("toolkit.domain.catalog._gcs_files_from_registry", counting_gcs_files)

        r = CatalogResolver(include_local=False)

        r.list_datasets()
        assert call_count == 1, f"Prima chiamata: 1 fetch, ma {call_count}"

        r.list_datasets()
        assert call_count == 1, f"Seconda chiamata: nessun fetch, ma {call_count}"

    def test_truncated_flag_accurate(self, resolver: CatalogResolver) -> None:
        """truncated=True solo quando il limit e' inferiore a total_count."""
        # limit esatto = total_count → non troncato
        res = resolver.list_datasets(limit=4)
        assert res["truncated"] is False
        assert len(res["datasets"]) == 4

        # limit > total_count → non troncato
        res = resolver.list_datasets(limit=100)
        assert res["truncated"] is False
        assert len(res["datasets"]) == 4

        # limit < total_count → troncato
        res = resolver.list_datasets(limit=3)
        assert res["truncated"] is True
        assert len(res["datasets"]) == 3


# ---------------------------------------------------------------------------
# Semantic find (mossa 1: catalogo committato nel resolver)
# ---------------------------------------------------------------------------


class TestSemanticFind:
    def test_find_by_description(self, resolver_semantic: CatalogResolver) -> None:
        """Query su description del catalogo committato trova il dataset."""
        res = resolver_semantic.list_datasets(query="bandi di gara")
        slugs = {d["slug"] for d in res["datasets"]}
        assert "anac_bandi_gara" in slugs
        entry = next(d for d in res["datasets"] if d["slug"] == "anac_bandi_gara")
        assert entry["description"] == "Bandi di gara pubblici"
        assert entry["meta_match"] is True
        assert entry["_repo"] == "dataset-incubator"

    def test_find_by_column(self, resolver_semantic: CatalogResolver) -> None:
        """Query su nome colonna → matched_columns popolato."""
        res = resolver_semantic.list_datasets(query="importo")
        entry = next(d for d in res["datasets"] if d["slug"] == "anac_bandi_gara")
        assert entry["meta_match"] is not True
        assert entry["matched_columns"] == [
            {"name": "importo", "type": "DOUBLE", "role": "metric", "description": "Importo gara"}
        ]

    def test_find_metric_only(self, resolver_semantic: CatalogResolver) -> None:
        """metric_only esclude i dataset senza colonne metric."""
        res = resolver_semantic.list_datasets(metric_only=True)
        slugs = {d["slug"] for d in res["datasets"]}
        assert "anac_bandi_gara" in slugs
        assert "terna_electricity_by_source" in slugs
        assert "popolazione_istat_comunale_2019_2025" not in slugs

    def test_find_metric_only_respects_query(self, resolver_semantic: CatalogResolver) -> None:
        """metric_only + query semantica si combinano."""
        res = resolver_semantic.list_datasets(query="produzione elettrica", metric_only=True)
        slugs = {d["slug"] for d in res["datasets"]}
        assert "terna_electricity_by_source" in slugs
        assert "anac_bandi_gara" not in slugs

    def test_describe_semantic_enrichment(
        self, resolver_semantic: CatalogResolver, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """describe_slug arricchito: description/tags/period + colonne con role."""
        fake_preview = {
            "path": "x",
            "column_count": 1,
            "columns": [{"name": "importo", "type": "DOUBLE"}],
            "row_count": 10,
            "preview": [],
            "truncated": False,
        }
        monkeypatch.setattr(
            "toolkit.domain.catalog.parquet_preview", lambda *a, **k: dict(fake_preview)
        )

        res = resolver_semantic.describe_slug("anac_bandi_gara")
        assert res["description"] == "Bandi di gara pubblici"
        assert res["tags"] == ["appalti"]
        assert res["period"] == {"start": 2023, "end": 2024}
        assert res["columns"][0]["role"] == "metric"
        assert res["columns"][0]["description"] == "Importo gara"


# ---------------------------------------------------------------------------
# Scan cataloghi semantici committati (fusion ADR)
# ---------------------------------------------------------------------------


class TestScanCommittedCatalogs:
    """Scan semantico cross-repo: registry.json unico (fusion ADR).

    Regressione fusion ADR: prima leggeva solo ``clean_catalog.json``, quindi
    i repo migrati a ``registry.json`` (eurostat, dcl-bologna) sparivano dal
    find semantico. Ora usa ``load_repo_registry`` (stesso reader del resolver
    GCS).
    """

    def test_reads_fusion_registries(self, tmp_path: "Any") -> None:
        """Repo multipli con registry.json indicizzati."""
        # Repo migrato: registry.json unico con sezione datasets
        migrato = tmp_path / "eurostat"
        (migrato / "registry").mkdir(parents=True)
        (migrato / "registry" / "registry.json").write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "repo": "eurostat",
                    "datasets": [
                        {
                            "slug": "eurostat_crime_nuts3",
                            "name": "Criminalità NUTS3",
                            "tags": ["giustizia", "reati"],
                            "columns": [
                                {"name": "geo", "role": "dimension", "semantic_type": "nuts_code"}
                            ],
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        # Secondo repo: registry.json con un altro dataset
        di = tmp_path / "dataset-incubator"
        (di / "registry").mkdir(parents=True)
        (di / "registry" / "registry.json").write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "repo": "dataset-incubator",
                    "datasets": [
                        {
                            "slug": "anac_bandi_gara",
                            "name": "Bandi ANAC",
                            "tags": ["appalti"],
                            "columns": [{"name": "cig", "role": "dimension"}],
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )

        sem = _scan_committed_catalogs(tmp_path)

        assert "eurostat_crime_nuts3" in sem
        assert sem["eurostat_crime_nuts3"]["_repo"] == "eurostat"
        assert sem["eurostat_crime_nuts3"]["tags"] == ["giustizia", "reati"]

        assert "anac_bandi_gara" in sem
        assert sem["anac_bandi_gara"]["_repo"] == "dataset-incubator"
        assert sem["anac_bandi_gara"]["name"] == "Bandi ANAC"

    def test_skips_repo_without_registry(self, tmp_path: "Any") -> None:
        """Repo senza registry non produce entry."""
        (tmp_path / "no-registry").mkdir()
        sem = _scan_committed_catalogs(tmp_path)
        assert sem == {}


# ---------------------------------------------------------------------------
# Scan configs workspace — cross-repo (gap fusion: configs DI-centrico)
# ---------------------------------------------------------------------------


class TestScanWorkspaceConfigs:
    """Scan dataset.yml cross-repo: DI (candidates/compose/support) + migrati.

    Regressione fusion ADR: prima scansionava solo dataset-incubator
    candidates/support, quindi i repo migrati (eurostat, dcl-bologna) avevano
    stage=None/has_clean=False pur avendo dataset.yml e parquet locali. Ora
    usa la mappa canonica REPO_DATASET_DIRS (scalabile ai nuovi repo).
    """

    def test_reads_migrated_repo_datasets(self, tmp_path: "Any") -> None:
        """Dataset.yml in datasets/ di un repo migrato viene indicizzato."""
        migrated = tmp_path / "eurostat"
        (migrated / "datasets" / "eurostat-crime-nuts3").mkdir(parents=True)
        (migrated / "datasets" / "eurostat-crime-nuts3" / "dataset.yml").write_text(
            "root: '../../out'\n"
            "dataset:\n"
            "  name: 'eurostat_crime_nuts3'\n"
            "  source_id: 'eurostat'\n"
            "  years: [2026]\n",
            encoding="utf-8",
        )
        # Parquet locale finto → has_clean True
        (migrated / "out" / "data" / "clean" / "eurostat_crime_nuts3").mkdir(parents=True)
        (migrated / "out" / "data" / "clean" / "eurostat_crime_nuts3" / "x.parquet").touch()

        from toolkit.domain.catalog import _scan_workspace_configs

        configs = _scan_workspace_configs(tmp_path)

        assert "eurostat_crime_nuts3" in configs
        entry = configs["eurostat_crime_nuts3"]
        assert entry["stage"] == "datasets"
        assert entry["has_clean"] is True
        assert "dataset.yml" in entry["config_path"]

    def test_keeps_di_candidates_compose_and_support(self, tmp_path: "Any") -> None:
        """DI candidates/compose/support_datasets restano indicizzati."""
        for section in ("candidates", "compose", "support_datasets"):
            d = tmp_path / "dataset-incubator" / section
            d.mkdir(parents=True)
            (d / "dataset.yml").write_text("dataset:\n  name: 'x'\n", encoding="utf-8")
        (tmp_path / "dataset-incubator" / "candidates" / "anac-bandi-gara").mkdir(parents=True)
        (
            tmp_path / "dataset-incubator" / "candidates" / "anac-bandi-gara" / "dataset.yml"
        ).write_text("dataset:\n  name: 'anac_bandi_gara'\n", encoding="utf-8")
        (tmp_path / "dataset-incubator" / "compose" / "anac-appalti-master").mkdir(parents=True)
        (
            tmp_path / "dataset-incubator" / "compose" / "anac-appalti-master" / "dataset.yml"
        ).write_text("dataset:\n  name: 'anac_appalti_master'\n", encoding="utf-8")
        supp = tmp_path / "dataset-incubator" / "support_datasets" / "anac-collaudo"
        supp.mkdir(parents=True)
        (supp / "dataset.yml").write_text("dataset:\n  name: 'anac_collaudo'\n", encoding="utf-8")

        from toolkit.domain.catalog import _scan_workspace_configs

        configs = _scan_workspace_configs(tmp_path)

        assert configs["anac_bandi_gara"]["stage"] == "candidates"
        assert configs["anac_appalti_master"]["stage"] == "compose"
        assert configs["anac_collaudo"]["stage"] == "support"


# ---------------------------------------------------------------------------
# Scan mart locali (gap: resolver ignorava i mart su disco)
# ---------------------------------------------------------------------------


class TestScanWorkspaceParquetsMarts:
    """Lo scan parquet locale include i mart (bucket local-mart).

    Regressione: resolve_slug(layer='mart') risolveva solo i mart GCS (dal
    registry), ignorando i mart locali su disco — i 23+ mart di dcl-bologna
    non erano leggibili in catalog mode.
    """

    @pytest.mark.contract
    def test_scans_mart_with_year(self, tmp_path: "Any") -> None:
        """Mart con anno: {slug}/{year}/mart_{table}.parquet."""
        repo = tmp_path / "dcl-bologna"
        mart = repo / "out" / "data" / "mart" / "centraline_aria" / "2026"
        mart.mkdir(parents=True)
        (mart / "mart_aria_ora.parquet").touch()
        (mart / "mart_aria_stazione.parquet").touch()
        # Repo dati: registry/ presente
        (repo / "registry").mkdir()

        from toolkit.domain.catalog import _scan_workspace_parquets

        marts = [
            f
            for f in _scan_workspace_parquets(tmp_path)
            if f["slug"] == "centraline_aria" and f["bucket"] == "local-mart"
        ]
        assert len(marts) == 2
        tables = {f["table"] for f in marts}
        assert tables == {"mart_aria_ora", "mart_aria_stazione"}
        assert all(f["year"] == 2026 for f in marts)

    @pytest.mark.contract
    def test_scans_mart_flat_no_year(self, tmp_path: "Any") -> None:
        """Mart flat legacy senza anno: {slug}/mart_{table}.parquet."""
        repo = tmp_path / "dcl-bologna"
        mart = repo / "out" / "data" / "mart" / "spire_traffico"
        mart.mkdir(parents=True)
        (mart / "mart_spire_trend.parquet").touch()
        (repo / "registry").mkdir()

        from toolkit.domain.catalog import _scan_workspace_parquets

        marts = [
            f
            for f in _scan_workspace_parquets(tmp_path)
            if f["slug"] == "spire_traffico" and f["bucket"] == "local-mart"
        ]
        assert len(marts) == 1
        assert marts[0]["table"] == "mart_spire_trend"
        assert marts[0]["year"] is None

    @pytest.mark.contract
    def test_skips_non_data_repos(self, tmp_path: "Any") -> None:
        """Repo senza registry/ né datasets/ non viene scansionato."""
        repo = tmp_path / "open-conto-annuale"
        mart = repo / "out" / "data" / "mart" / "x" / "2026"
        mart.mkdir(parents=True)
        (mart / "mart_x.parquet").touch()

        from toolkit.domain.catalog import _scan_workspace_parquets

        entries = _scan_workspace_parquets(tmp_path)
        assert entries == []

    @pytest.mark.contract
    def test_resolve_mart_local_and_gcs(
        self, tmp_path: "Any", monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """resolve_slug layer=mart fonde mart locali + GCS."""
        from toolkit.domain.catalog import (
            CatalogResolver,
            _scan_workspace_parquets,
        )

        repo = tmp_path / "dcl-bologna"
        mart = repo / "out" / "data" / "mart" / "centraline_aria" / "2026"
        mart.mkdir(parents=True)
        (mart / "mart_aria_ora.parquet").touch()
        (repo / "registry").mkdir()

        # GCS: mart dal registry (path flat, year=None)
        gcs = [
            {
                "url": "gs://dataciviclab-mart/centraline_aria/2026/mart_aria_ora.parquet",
                "slug": "centraline_aria",
                "bucket": "dataciviclab-mart",
                "year": None,
                "path": "gs://dataciviclab-mart/centraline_aria/2026/mart_aria_ora.parquet",
                "table": "mart_aria_ora",
                "_gcs": True,
            }
        ]
        monkeypatch.setattr("toolkit.domain.catalog._gcs_files_from_registry", lambda _w: gcs)
        monkeypatch.setattr(
            "toolkit.domain.catalog._scan_workspace_parquets",
            lambda _w: _scan_workspace_parquets(tmp_path),
        )
        monkeypatch.setattr("toolkit.domain.catalog._scan_workspace_configs", lambda _w, **_: {})
        monkeypatch.setattr("toolkit.domain.catalog._scan_committed_catalogs", lambda _w: {})

        r = CatalogResolver(workspace=tmp_path)
        res = r.resolve_slug("centraline_aria", layer="mart")
        buckets = {(f["bucket"], f.get("table")) for f in res}
        assert ("local-mart", "mart_aria_ora") in buckets
        assert ("dataciviclab-mart", "mart_aria_ora") in buckets


class TestScanWorkspaceConfigsCanonicalSlug:
    """Lo slug viene da dataset.name (chiave canonica), non dalla dir.

    Regressione: _scan_workspace_configs usava data['slug'] or dir_slug,
    producendo slug dalla dir (es. 'precipitazioni') invece del name
    canonico (es. 'precipitazioni_bologna') per 22/188 dataset.
    """

    @pytest.mark.contract
    def test_slug_from_dataset_name(self, tmp_path: "Any") -> None:
        """Dir diversa dal name → slug = dataset.name."""
        repo = tmp_path / "dcl-bologna"
        (repo / "datasets" / "precipitazioni").mkdir(parents=True)
        (repo / "datasets" / "precipitazioni" / "dataset.yml").write_text(
            "dataset:\n  name: 'precipitazioni_bologna'\n  source_id: 'comune_bologna_opendata'\n"
            "  years: [2026]\n",
            encoding="utf-8",
        )

        from toolkit.domain.catalog import _scan_workspace_configs

        configs = _scan_workspace_configs(tmp_path)

        assert "precipitazioni_bologna" in configs
        assert "precipitazioni" not in configs

    @pytest.mark.contract
    def test_slug_fallback_to_dir_without_name(self, tmp_path: "Any") -> None:
        """Config legacy senza dataset.name → fallback alla dir."""
        repo = tmp_path / "legacy-repo"
        (repo / "datasets" / "vecchio-dataset").mkdir(parents=True)
        (repo / "datasets" / "vecchio-dataset" / "dataset.yml").write_text(
            "dataset:\n  source_id: 'x'\n  years: [2024]\n",
            encoding="utf-8",
        )

        from toolkit.domain.catalog import _scan_workspace_configs

        configs = _scan_workspace_configs(tmp_path)

        assert "vecchio_dataset" in configs
