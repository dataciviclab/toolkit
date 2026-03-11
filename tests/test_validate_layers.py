import json
from pathlib import Path
import re

import duckdb

from toolkit.raw.validate import validate_raw_output
from toolkit.clean.validate import validate_clean
from toolkit.cross.validate import validate_cross_outputs
from toolkit.mart.validate import validate_mart
from toolkit.core.validation import write_validation_json


def _write_parquet(path: Path, sql: str):
    con = duckdb.connect(":memory:")
    con.execute(sql)
    con.execute(f"COPY t TO '{path}' (FORMAT 'parquet')")
    con.close()


def _assert_portable_json_report(path: Path, *, root: Path, field: str, expected: str):
    payload = json.loads(path.read_text(encoding="utf-8"))
    serialized = json.dumps(payload, ensure_ascii=False)

    assert not re.search(r"[A-Za-z]:\\\\", serialized)
    assert '": "/' not in serialized
    assert '": "\\\\' not in serialized
    assert str(root.resolve()) not in serialized
    assert payload["summary"][field] == expected


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


def test_validate_clean_report_uses_root_relative_path(tmp_path: Path):
    root = tmp_path / "root"
    parquet = root / "data" / "clean" / "demo" / "2024" / "demo_2024_clean.parquet"
    parquet.parent.mkdir(parents=True, exist_ok=True)
    _write_parquet(parquet, "CREATE TABLE t AS SELECT 1 AS a, 'x' AS b")

    result = validate_clean(parquet, required=["a", "b"], root=root)
    report = write_validation_json(parquet.parent / "_validate" / "clean_validation.json", result)

    _assert_portable_json_report(
        report,
        root=root,
        field="path",
        expected="data/clean/demo/2024/demo_2024_clean.parquet",
    )


def test_validate_mart_required_tables(tmp_path: Path):
    d = tmp_path / "mart"
    d.mkdir(parents=True, exist_ok=True)

    _write_parquet(d / "foo.parquet", "CREATE TABLE t AS SELECT 1 AS k")

    res = validate_mart(d, required_tables=["foo", "bar"])
    assert res.ok is False
    assert any("Missing required MART tables" in e for e in res.errors)


def test_validate_mart_min_rows_rule(tmp_path: Path):
    d = tmp_path / "mart"
    d.mkdir(parents=True, exist_ok=True)

    _write_parquet(d / "foo.parquet", "CREATE TABLE t AS SELECT 1 AS k")

    bad = validate_mart(d, table_rules={"foo": {"min_rows": 2}})
    assert bad.ok is False
    assert any("[foo] row_count too small: 1 < 2" in e for e in bad.errors)

    ok = validate_mart(d, table_rules={"foo": {"min_rows": 1}})
    assert ok.ok is True


def test_validate_mart_warns_on_orphan_table_rules_against_declared_tables(tmp_path: Path):
    d = tmp_path / "mart"
    d.mkdir(parents=True, exist_ok=True)

    _write_parquet(d / "foo.parquet", "CREATE TABLE t AS SELECT 1 AS k")

    result = validate_mart(
        d,
        declared_tables=["foo"],
        table_rules={"bar": {"min_rows": 1}},
    )

    assert result.ok is True
    assert any("not declared in mart.tables" in warning for warning in result.warnings)
    assert result.summary["declared_tables"] == ["foo"]
    assert result.summary["orphan_table_rules"] == ["bar"]


def test_validate_mart_report_uses_root_relative_dir(tmp_path: Path):
    root = tmp_path / "root"
    mart_dir = root / "data" / "mart" / "demo" / "2024"
    mart_dir.mkdir(parents=True, exist_ok=True)
    _write_parquet(mart_dir / "foo.parquet", "CREATE TABLE t AS SELECT 1 AS k")

    result = validate_mart(mart_dir, required_tables=["foo"], root=root)
    report = write_validation_json(mart_dir / "_validate" / "mart_validation.json", result)

    _assert_portable_json_report(
        report,
        root=root,
        field="dir",
        expected="data/mart/demo/2024",
    )


def test_validate_cross_outputs_required_tables(tmp_path: Path):
    d = tmp_path / "cross"
    d.mkdir(parents=True, exist_ok=True)

    _write_parquet(d / "foo.parquet", "CREATE TABLE t AS SELECT 1 AS k")

    res = validate_cross_outputs(d, required_tables=["foo", "bar"], years=[2022, 2023])
    assert res.ok is False
    assert any("Missing required CROSS tables" in e for e in res.errors)
    assert res.summary["years"] == [2022, 2023]
