"""Test per core/duckdb_shape.py — parquet_preview con e senza sql=.

Il marker ``contract`` protegge l'API pubblica di ``parquet_preview``.
Il marker ``policy`` protegge regole non ovvie (es. eccezioni vs empty dict).
"""

from __future__ import annotations

import pytest

from tests.helpers import write_parquet


@pytest.mark.contract
def test_parquet_preview_default_no_sql(tmp_path):
    """parquet_preview senza sql=: SELECT * LIMIT N (backward compat)."""
    from toolkit.core.duckdb_shape import parquet_preview

    pq = tmp_path / "test.parquet"
    write_parquet(pq, "CREATE TABLE t AS SELECT 1 AS x, 'a' AS y")

    result = parquet_preview(pq, limit=5)
    assert result["column_count"] == 2
    assert result["row_count"] == 1
    assert len(result["preview"]) == 1
    assert result["preview"][0]["x"] == 1
    assert result["preview"][0]["y"] == "a"
    assert result["sql"] is None
    assert result["truncated"] is False


@pytest.mark.contract
def test_parquet_preview_with_sql_select_star(tmp_path):
    """parquet_preview con sql='SELECT * FROM data': funziona."""
    from toolkit.core.duckdb_shape import parquet_preview

    pq = tmp_path / "test.parquet"
    write_parquet(pq, "CREATE TABLE t AS SELECT 1 AS x, 'a' AS y")

    result = parquet_preview(pq, sql="SELECT * FROM data")
    assert result["column_count"] == 2
    assert result["row_count"] == 1
    assert len(result["preview"]) == 1
    assert result["sql"] == "SELECT * FROM data"


@pytest.mark.contract
def test_parquet_preview_with_where(tmp_path):
    """parquet_preview con WHERE: filtra correttamente."""
    from toolkit.core.duckdb_shape import parquet_preview

    pq = tmp_path / "test.parquet"
    write_parquet(pq, "CREATE TABLE t AS "
                       "SELECT 1 AS id, 'a' AS val UNION ALL "
                       "SELECT 2, 'b' UNION ALL "
                       "SELECT 3, 'a'")

    result = parquet_preview(pq, sql="SELECT * FROM data WHERE val = 'a'")
    assert result["row_count"] == 2
    assert len(result["preview"]) == 2
    assert all(r["val"] == "a" for r in result["preview"])


@pytest.mark.contract
def test_parquet_preview_with_group_by(tmp_path):
    """parquet_preview con GROUP BY: aggregazione corretta."""
    from toolkit.core.duckdb_shape import parquet_preview

    pq = tmp_path / "test.parquet"
    write_parquet(pq, "CREATE TABLE t AS "
                       "SELECT 'x' AS k, 10 AS v UNION ALL "
                       "SELECT 'x', 20 UNION ALL "
                       "SELECT 'y', 5")

    result = parquet_preview(pq, sql="SELECT k, SUM(v) AS total FROM data GROUP BY k ORDER BY k")
    assert result["column_count"] == 2
    assert result["row_count"] == 2
    rows = {r["k"]: r["total"] for r in result["preview"]}
    assert rows["x"] == 30
    assert rows["y"] == 5


@pytest.mark.contract
def test_parquet_preview_with_read_parquet_direct(tmp_path):
    """parquet_preview con read_parquet() esplicito: funziona comunque."""
    from toolkit.core.duckdb_shape import parquet_preview

    pq = tmp_path / "test.parquet"
    write_parquet(pq, "CREATE TABLE t AS SELECT 42 AS n")

    # Usa read_parquet direttamente invece di 'data'
    result = parquet_preview(pq, sql=f"SELECT n FROM read_parquet('{pq}') WHERE n = 42")
    assert result["row_count"] == 1
    assert result["preview"][0]["n"] == 42


@pytest.mark.policy
def test_parquet_preview_missing_file_no_sql_graceful(tmp_path):
    """Senza sql=, file mancante → empty dict graceful."""
    from toolkit.core.duckdb_shape import parquet_preview

    result = parquet_preview(tmp_path / "nonexistent.parquet")
    assert result["column_count"] == 0
    assert result["row_count"] is None
    assert result["preview"] == []


@pytest.mark.policy
def test_parquet_preview_missing_file_with_sql_raises(tmp_path):
    """Con sql=, file mancante → FileNotFoundError."""
    from toolkit.core.duckdb_shape import parquet_preview

    with pytest.raises(FileNotFoundError, match="nonexistent"):
        parquet_preview(tmp_path / "nonexistent.parquet", sql="SELECT 1")


@pytest.mark.policy
def test_parquet_preview_invalid_sql_raises(tmp_path):
    """Con sql= invalido → eccezione propagata (non ingoiata)."""
    from toolkit.core.duckdb_shape import parquet_preview

    pq = tmp_path / "test.parquet"
    write_parquet(pq, "CREATE TABLE t AS SELECT 1 AS x")

    with pytest.raises(Exception, match="nonexistent_table"):
        parquet_preview(pq, sql="SELECT * FROM nonexistent_table")



