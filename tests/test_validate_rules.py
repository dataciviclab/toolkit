from pathlib import Path

import duckdb

from toolkit.clean.validate import validate_clean


def _write_parquet(path: Path, sql_create_table_t: str) -> None:
    """
    sql_create_table_t must create a table named 't'
    """
    con = duckdb.connect(":memory:")
    con.execute(sql_create_table_t)
    con.execute(f"COPY t TO '{path.as_posix()}' (FORMAT 'parquet')")
    con.close()


def test_validate_clean_pk_duplicates_fails(tmp_path: Path):
    p = tmp_path / "clean.parquet"
    _write_parquet(
        p,
        """
        CREATE TABLE t AS
        SELECT * FROM (VALUES
          (2022, 'A', 1),
          (2022, 'A', 2)  -- duplicate key (anno, comune)
        ) v(anno, comune, valore)
        """,
    )

    res = validate_clean(
        p,
        required=["anno", "comune", "valore"],
        primary_key=["anno", "comune"],
    )
    assert res.ok is False
    assert any("Primary key duplicates found" in e for e in res.errors)


def test_validate_clean_range_fails(tmp_path: Path):
    p = tmp_path / "clean.parquet"
    _write_parquet(
        p,
        """
        CREATE TABLE t AS
        SELECT * FROM (VALUES
          (10.0),
          (120.0) -- out of range > 100
        ) v(pct_rd)
        """,
    )

    res = validate_clean(
        p,
        required=["pct_rd"],
        ranges={"pct_rd": {"min": 0, "max": 100}},
    )
    assert res.ok is False
    assert any("Range check failed for 'pct_rd'" in e for e in res.errors)


def test_validate_clean_null_pct_fails(tmp_path: Path):
    p = tmp_path / "clean.parquet"
    _write_parquet(
        p,
        """
        CREATE TABLE t AS
        SELECT * FROM (VALUES
          (NULL),
          (NULL),
          (1.0),
          (2.0)
        ) v(ru_tot_t)
        """,
    )

    # 2 null su 4 = 50% > 5%
    res = validate_clean(
        p,
        required=["ru_tot_t"],
        max_null_pct={"ru_tot_t": 0.05},
    )
    assert res.ok is False
    assert any("null_pct too high" in e for e in res.errors)