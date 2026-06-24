from pathlib import Path
from tests.helpers import write_parquet

import pytest

from toolkit.clean.validate import validate_clean
from toolkit.core.column_rules import check_column_types


@pytest.mark.policy
def test_validate_clean_pk_duplicates_fails(tmp_path: Path):
    p = tmp_path / "clean.parquet"
    write_parquet(
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


@pytest.mark.policy
def test_validate_clean_range_fails(tmp_path: Path):
    p = tmp_path / "clean.parquet"
    write_parquet(
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


@pytest.mark.policy
def test_validate_clean_null_pct_fails(tmp_path: Path):
    p = tmp_path / "clean.parquet"
    write_parquet(
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


@pytest.mark.policy
def test_check_column_types_warns_on_name_type_mismatch():
    """Una colonna chiamata 'importo' ma di tipo VARCHAR deve produrre warning."""
    cols = [("importo", "VARCHAR"), ("anno", "BIGINT"), ("regione", "VARCHAR")]
    err, warn = check_column_types(cols)
    assert len(err) == 0
    assert len(warn) == 1
    assert "importo" in warn[0]
    assert "VARCHAR" in warn[0]


@pytest.mark.policy
def test_check_column_types_ok_when_types_match():
    """Colonne con nome numerico e tipo numerico non devono produrre warning."""
    cols = [("importo", "DOUBLE"), ("anno", "INTEGER"), ("valore", "DECIMAL(10,2)")]
    err, warn = check_column_types(cols)
    assert len(err) == 0
    assert len(warn) == 0


@pytest.mark.policy
def test_check_column_types_non_numeric_name_ignored():
    """Colonne con nome non numerico non vengono controllate."""
    cols = [("regione", "VARCHAR"), ("comune", "VARCHAR")]
    err, warn = check_column_types(cols)
    assert len(err) == 0
    assert len(warn) == 0
