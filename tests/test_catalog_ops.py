"""Test contratto per CatalogResolver (condiviso CLI+MCP).

Protegge:
- Risoluzione slug → path parquet su GCS
- Ricerca dataset per testo e layer
- Schema DuckDB su parquet GCS (con mock)
- Filtri layer (clean/mart)
"""

from __future__ import annotations

from typing import Any

import pytest

from toolkit.cli.catalog_ops import (
    CatalogResolver,
    CLEAN_BUCKET,
    MART_BUCKET,
    _parse_clean_filename,
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

    def _fake_read_manifest(*args: Any, **kwargs: Any) -> dict[str, Any]:
        return _FAKE_MANIFEST

    monkeypatch.setattr("toolkit.cli.catalog_ops.read_manifest", _fake_read_manifest)
    return resolver


@pytest.fixture
def resolver_with_local(monkeypatch: pytest.MonkeyPatch) -> CatalogResolver:
    """CatalogResolver con manifest mockato E scan locale mockata."""
    resolver = CatalogResolver(
        manifest_url="http://fake/manifest.json",
        include_local=True,
    )

    def _fake_read_manifest(*args: Any, **kwargs: Any) -> dict[str, Any]:
        return _FAKE_MANIFEST

    monkeypatch.setattr("toolkit.cli.catalog_ops.read_manifest", _fake_read_manifest)

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
        "toolkit.cli.catalog_ops._scan_workspace_parquets",
        lambda _workspace: fake_local,
    )
    # Mock anche scan configs per il dataset locale
    monkeypatch.setattr(
        "toolkit.cli.catalog_ops._scan_workspace_configs",
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
            "toolkit.cli.catalog_ops.parquet_preview", lambda path, limit=5: fake_preview
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

        def _fake_read_manifest(*args: Any, **kwargs: Any) -> dict[str, Any]:
            return _FAKE_MANIFEST

        monkeypatch.setattr("toolkit.cli.catalog_ops.read_manifest", _fake_read_manifest)

        # Mock scan parquet locale: stesso slug del GCS (anac_bandi_gara)
        monkeypatch.setattr(
            "toolkit.cli.catalog_ops._scan_workspace_parquets",
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
            "toolkit.cli.catalog_ops._scan_workspace_configs",
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
            "toolkit.cli.catalog_ops.parquet_preview", lambda path, limit=5: fake_preview
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
        """Due resolver con URL diverso non condividono cache."""
        r1 = CatalogResolver(manifest_url="http://fake/a.json")
        r2 = CatalogResolver(manifest_url="http://fake/b.json")

        calls: list[str] = []

        def tracking_read_manifest(url: str | None = None) -> dict[str, Any]:
            calls.append(url or "default")
            return _FAKE_MANIFEST

        monkeypatch.setattr("toolkit.cli.catalog_ops.read_manifest", tracking_read_manifest)

        r1.list_datasets(query="anac")
        r2.list_datasets(query="anac")

        # Ogni resolver fa la propria chiamata
        assert len(calls) == 2

    def test_gcs_cache(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Due chiamate a list_datasets producono una sola fetch del manifest.

        Regression: _load_gcs deve cacheare il manifest come _load_local.
        """
        call_count = 0

        def counting_manifest(*args: Any, **kwargs: Any) -> dict[str, Any]:
            nonlocal call_count
            call_count += 1
            return _FAKE_MANIFEST

        monkeypatch.setattr("toolkit.cli.catalog_ops.read_manifest", counting_manifest)

        r = CatalogResolver(manifest_url="http://fake/manifest.json", include_local=False)

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
