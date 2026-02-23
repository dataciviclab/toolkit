from pathlib import Path

import duckdb

from toolkit.raw.validate import validate_raw_output
from toolkit.clean.validate import validate_clean
from toolkit.mart.validate import validate_mart


def _write_parquet(path: Path, sql: str):
    con = duckdb.connect(":memory:")
    con.execute(sql)
    con.execute(f"COPY t TO '{path}' (FORMAT 'parquet')")
    con.close()


def test_validate_raw_detects_html_in_csv(tmp_path: Path):
    out_dir = tmp_path / "raw"
    out_dir.mkdir(parents=True, exist_ok=True)

    # finto CSV che in realtà è HTML (pagina di errore)
    bad = out_dir / "data.csv"
    bad.write_text("<html><body>error</body></html>", encoding="utf-8")

    files_written = [{"file": "data.csv", "bytes": bad.stat().st_size, "sha256": "fake"}]
    res = validate_raw_output(out_dir, files_written)

    assert res.ok is False
    assert any("contain HTML" in e for e in res.errors)


def test_validate_clean_ok_and_missing_required(tmp_path: Path):
    p = tmp_path / "clean.parquet"
    _write_parquet(p, "CREATE TABLE t AS SELECT 1 AS a, 'x' AS b")

    ok = validate_clean(p, required=["a", "b"])
    assert ok.ok is True
    assert ok.summary["row_count"] == 1

    bad = validate_clean(p, required=["missing_col"])
    assert bad.ok is False
    assert any("Missing required columns" in e for e in bad.errors)


def test_validate_mart_required_tables(tmp_path: Path):
    d = tmp_path / "mart"
    d.mkdir(parents=True, exist_ok=True)

    _write_parquet(d / "foo.parquet", "CREATE TABLE t AS SELECT 1 AS k")

    res = validate_mart(d, required_tables=["foo", "bar"])
    assert res.ok is False
    assert any("Missing required MART tables" in e for e in res.errors)