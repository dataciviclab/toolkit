from pathlib import Path

import duckdb
import pytest

from toolkit.clean.duckdb_read import (
    _execute_normalized_csv_read,
    _load_normalized_csv_frame,
)
from toolkit.clean.run import run_clean


class _NoopLogger:
    def info(self, *_args, **_kwargs):
        return None

    def warning(self, *_args, **_kwargs):
        return None


def test_run_clean_csv_columns_reads_trailing_delimiter_csv(tmp_path: Path):
    raw_dir = tmp_path / "data" / "raw" / "demo" / "2024"
    raw_dir.mkdir(parents=True, exist_ok=True)
    csv_path = raw_dir / "ispra_like.csv"
    csv_path.write_text(
        "a;b\n\t1;2;\n\t3;4;\n",
        encoding="utf-8",
    )

    sql_path = tmp_path / "clean.sql"
    sql_path.write_text("SELECT a, b FROM raw_input", encoding="utf-8")

    run_clean(
        "demo",
        2024,
        str(tmp_path),
        {
            "sql": str(sql_path),
            "read": {
                "mode": "latest",
                "delim": ";",
                "header": True,
                "ignore_errors": True,
                "null_padding": True,
                "trim_whitespace": True,
                "columns": {
                    "a": "VARCHAR",
                    "b": "VARCHAR",
                },
            },
        },
        _NoopLogger(),
    )

    out = tmp_path / "data" / "clean" / "demo" / "2024" / "demo_2024_clean.parquet"
    assert out.exists()

    con = duckdb.connect(":memory:")
    rows = con.execute(f"SELECT a, b FROM read_parquet('{out.as_posix()}') ORDER BY a").fetchall()
    con.close()

    assert rows == [("1", "2"), ("3", "4")]


def test_run_clean_positional_csv_short_rows_are_padded(tmp_path: Path):
    """Righe piu' corte delle colonne attese vengono paddate con stringa vuota."""
    csv_path = tmp_path / "short.csv"
    # 3 colonne configurate, ma alcune righe ne hanno solo 1 o 2
    csv_path.write_text("a;b;c\n\t1\n\t2;3\n\t4;5;6\n", encoding="utf-8")

    read_cfg = {
        "columns": {"a": "VARCHAR", "b": "VARCHAR", "c": "VARCHAR"},
        "normalize_rows_to_columns": True,
        "header": True,
        "delim": ";",
        "trim_whitespace": True,
    }
    df = _load_normalized_csv_frame(csv_path, read_cfg, read_cfg["columns"])

    assert list(df.columns) == ["a", "b", "c"]
    assert len(df) == 3
    assert df.iloc[0].tolist() == ["1", "", ""]
    assert df.iloc[1].tolist() == ["2", "3", ""]
    assert df.iloc[2].tolist() == ["4", "5", "6"]


def test_run_clean_positional_csv_wide_rows_raise(tmp_path: Path):
    """Righe piu' lunghe delle colonne attese alzano ValueError."""
    csv_path = tmp_path / "wide.csv"
    csv_path.write_text("a;b\n\t1;2;EXTRA\n", encoding="utf-8")

    read_cfg = {
        "columns": {"a": "VARCHAR", "b": "VARCHAR"},
        "normalize_rows_to_columns": True,
        "header": True,
        "delim": ";",
        "trim_whitespace": True,
    }
    with pytest.raises(ValueError, match="CSV row wider than configured columns"):
        _load_normalized_csv_frame(csv_path, read_cfg, read_cfg["columns"])


def test_run_clean_positional_csv_trim_whitespace(tmp_path: Path):
    """trim_whitespace=True (default) pulisce spazi dai valori."""
    csv_path = tmp_path / "ws.csv"
    csv_path.write_text("a;b\n\tcol1 ; col2 \n", encoding="utf-8")

    read_cfg = {
        "columns": {"a": "VARCHAR", "b": "VARCHAR"},
        "normalize_rows_to_columns": True,
        "header": True,
        "delim": ";",
        "trim_whitespace": True,
    }
    df = _load_normalized_csv_frame(csv_path, read_cfg, read_cfg["columns"])
    assert df.iloc[0].tolist() == ["col1", "col2"]

    # Con trim_whitespace=False, gli spazi restano
    read_cfg["trim_whitespace"] = False
    csv_path.write_text("a;b\n col1 ; col2 \n", encoding="utf-8")
    df2 = _load_normalized_csv_frame(csv_path, read_cfg, read_cfg["columns"])
    assert df2.iloc[0].tolist() == [" col1 ", " col2 "]


def test_run_clean_positional_csv_multi_file_concat(tmp_path: Path):
    """Piu' file CSV posizionali vengono concatenati in un unico DataFrame."""
    for name, content in [
        ("file1.csv", "a;b\n1;2\n"),
        ("file2.csv", "a;b\n3;4\n5;6\n"),
    ]:
        (tmp_path / name).write_text(content, encoding="utf-8")

    read_cfg = {
        "columns": {"a": "VARCHAR", "b": "VARCHAR"},
        "normalize_rows_to_columns": True,
        "header": True,
        "delim": ";",
        "trim_whitespace": True,
    }
    con = duckdb.connect(":memory:")
    params = _execute_normalized_csv_read(
        con,
        [tmp_path / "file1.csv", tmp_path / "file2.csv"],
        read_cfg,
    )

    rows = con.execute("SELECT * FROM raw_input ORDER BY a").fetchall()
    assert rows == [("1", "2"), ("3", "4"), ("5", "6")]
    assert params["normalize_rows_to_columns"] is True
    con.close()


def test_run_clean_positional_csv_skip_and_encoding(tmp_path: Path):
    """Skip righe iniziali e encoding non-UTF funzionano con normalize_rows_to_columns."""
    csv_path = tmp_path / "encoded.csv"
    # 2 righe da saltare (skip=2) + header = 3 righe totali prima dei dati
    csv_path.write_text(
        "RIGA DA SALTARE 1\nRIGA DA SALTARE 2\na;b\ncittà;perché\n",
        encoding="latin-1",
    )

    read_cfg = {
        "columns": {"a": "VARCHAR", "b": "VARCHAR"},
        "normalize_rows_to_columns": True,
        "header": True,
        "delim": ";",
        "skip": 2,
        "encoding": "latin-1",
        "trim_whitespace": True,
    }
    df = _load_normalized_csv_frame(csv_path, read_cfg, read_cfg["columns"])
    assert len(df) == 1
    assert df.iloc[0].tolist() == ["città", "perché"]


def test_run_clean_positional_csv_empty_rows(tmp_path: Path):
    """Righe vuote o con solo delimitatore vengono gestite senza crash."""
    csv_path = tmp_path / "sparse.csv"
    csv_path.write_text("a;b\n1;2\n\n;\n3;4\n", encoding="utf-8")

    read_cfg = {
        "columns": {"a": "VARCHAR", "b": "VARCHAR"},
        "normalize_rows_to_columns": True,
        "header": True,
        "delim": ";",
        "trim_whitespace": True,
    }
    df = _load_normalized_csv_frame(csv_path, read_cfg, read_cfg["columns"])
    # Le righe vuote producono righe con colonne vuote
    assert len(df) == 4
    assert df.iloc[2].tolist() == ["", ""]  # riga con solo ';'
