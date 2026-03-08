from pathlib import Path

import duckdb

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
