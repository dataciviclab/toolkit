"""Test contratto per catalog mode in toolkit_layer (cross-dataset SQL)."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest
from pytest import MonkeyPatch

from toolkit.cli.layer_ops import layer_query

pytestmark = pytest.mark.contract


# ---------------------------------------------------------------------------
# Mock helpers
# ---------------------------------------------------------------------------


class _FakeResult:
    description = [("n", None, None, None, None, None, None)]

    def fetchall(self) -> list[tuple]:
        return [(5,)]


class _FakeConn:
    def __init__(self) -> None:
        self.description = [("n", None, None, None, None, None, None)]

    def execute(self, sql: str) -> _FakeResult:  # type: ignore[return-value]
        return _FakeResult()

    def __enter__(self) -> _FakeConn:
        return self

    def __exit__(self, *args: object) -> None:
        pass


def _mock_all(monkeypatch: MonkeyPatch) -> None:
    """Mocka duckdb.connect + _resolve_datasets."""
    import toolkit.domain.layer as dl

    def fake_resolve(datasets: list[str], *a, **kw) -> dict[str, str]:
        return {s: f"/tmp/{s}.parquet" for s in datasets}

    monkeypatch.setattr(dl, "_resolve_datasets", fake_resolve)
    monkeypatch.setattr(duckdb, "connect", lambda *a, **kw: _FakeConn())


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestCatalogMode:
    def test_single_slug(self, monkeypatch: MonkeyPatch) -> None:
        _mock_all(monkeypatch)
        r = layer_query(datasets=["ds_a"], layer="clean", mode="sql", sql="SELECT 1")
        assert r["mode"] == "sql"

    def test_multiple_slugs(self, monkeypatch: MonkeyPatch) -> None:
        _mock_all(monkeypatch)
        r = layer_query(
            datasets=["ds_a", "ds_b"],
            layer="clean",
            mode="sql",
            sql="SELECT COUNT(*) AS n FROM ds_a JOIN ds_b ON 1=1",
        )
        assert r["mode"] == "sql"

    def test_invalid_slug(self) -> None:
        with pytest.raises(FileNotFoundError):
            layer_query(datasets=["x"], layer="clean", mode="sql", sql="SELECT 1")

    def test_no_params_raises(self) -> None:
        with pytest.raises(ValueError, match="Specificare"):
            layer_query(mode="sql", sql="SELECT 1")

    def test_both_params_raises(self) -> None:
        with pytest.raises(ValueError, match="Specificare"):
            layer_query(config_path="/tmp/x.yml", datasets=["ds_a"], mode="sql", sql="SELECT 1")

    def test_catalog_mode_rejects_preview(self, monkeypatch: MonkeyPatch) -> None:
        _mock_all(monkeypatch)
        with pytest.raises(ValueError, match="Catalog mode"):
            layer_query(datasets=["ds_a"], mode="schema")

    def test_catalog_mode_rejects_profile(self, monkeypatch: MonkeyPatch) -> None:
        _mock_all(monkeypatch)
        with pytest.raises(ValueError, match="Catalog mode"):
            layer_query(datasets=["ds_a"], mode="profile")


class TestScopeValidation:
    def test_drop_blocked(self, monkeypatch: MonkeyPatch) -> None:
        _mock_all(monkeypatch)
        with pytest.raises(ValueError, match="SELECT o WITH"):
            layer_query(datasets=["ds_a"], mode="sql", sql="DROP TABLE ds_a")

    def test_read_parquet_blocked(self, monkeypatch: MonkeyPatch) -> None:
        _mock_all(monkeypatch)
        with pytest.raises(ValueError, match="non consentita"):
            layer_query(datasets=["ds_a"], mode="sql", sql="SELECT * FROM read_parquet('s3://f')")

    def test_unknown_table_blocked(self, monkeypatch: MonkeyPatch) -> None:
        _mock_all(monkeypatch)
        with pytest.raises(ValueError, match="non consentita"):
            layer_query(datasets=["ds_a"], mode="sql", sql="SELECT * FROM unknown_table")

    def test_insert_blocked(self, monkeypatch: MonkeyPatch) -> None:
        _mock_all(monkeypatch)
        with pytest.raises(ValueError, match="SELECT o WITH"):
            layer_query(datasets=["ds_a"], mode="sql", sql="INSERT INTO ds_a VALUES (1)")

    def test_empty_sql(self) -> None:
        with pytest.raises(ValueError, match="richiede il parametro sql"):
            layer_query(datasets=["ds"], layer="clean", mode="sql", sql="")


class TestRawSql:
    """Test SQL su layer=raw (CSV). Richiede un dataset locale con raw CSV."""

    def test_raw_sql_aggregate(self, monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
        """SQL aggregato su raw — colonne DESCRIBE allineate."""
        from toolkit.domain.layer import _layer_sql_raw

        # Crea un CSV raw finto
        raw_dir = tmp_path / "data" / "raw" / "test_ds" / "2024"
        raw_dir.mkdir(parents=True)
        csv_file = raw_dir / "data.csv"
        csv_file.write_text("regione,valore\nLombardia,100\nLazio,50\nLombardia,30\n")

        # Mocka le funzioni che servono a _resolve_raw_dir
        fake_cfg = type(
            "cfg",
            (),
            {
                "dataset": "test_ds",
                "root": str(tmp_path),
                "years": [2024],
            },
        )()

        # Mocka path hints per _resolve_raw_dir
        monkeypatch.setattr(
            "toolkit.domain.layer._resolve_raw_dir",
            lambda _cfg, _year: (
                raw_dir,
                {
                    "raw_hints": {"primary_output_file": "data.csv"},
                    "dataset": "test_ds",
                    "year": 2024,
                },
            ),
        )

        result = _layer_sql_raw(
            config_path="/fake/path.yml",
            cfg=fake_cfg,
            year=2024,
            sql="SELECT regione, SUM(valore) AS tot FROM data GROUP BY regione ORDER BY tot DESC",
            limit=10,
        )

        assert result["layer"] == "raw"
        assert result["row_count"] == 2
        for row in result["preview"]:
            assert "regione" in row
            assert "tot" in row
        # Verifica primo risultato: Lombardia=130, Lazio=50
        assert result["preview"][0]["tot"] == 130
        assert result["preview"][1]["tot"] == 50
