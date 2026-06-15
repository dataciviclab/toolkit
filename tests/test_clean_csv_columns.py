import pytest
from lab_connectors.duckdb import safe_connect
from pathlib import Path

from tests.helpers import NoopLogger

from toolkit.core.read_csv_normalized import (
    _execute_normalized_csv_read,
    _load_normalized_csv_frame,
)
from toolkit.clean.run import run_clean


@pytest.mark.policy
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
        NoopLogger(),
    )

    out = tmp_path / "data" / "clean" / "demo" / "2024" / "demo_2024_clean.parquet"
    assert out.exists()

    with safe_connect() as con:
        rows = con.execute(
            f"SELECT a, b FROM read_parquet('{out.as_posix()}') ORDER BY a"
        ).fetchall()

    assert rows == [("1", "2"), ("3", "4")]


@pytest.mark.policy
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


@pytest.mark.policy
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


@pytest.mark.policy
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


@pytest.mark.policy
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
    with safe_connect() as con:
        params = _execute_normalized_csv_read(
            con,
            [tmp_path / "file1.csv", tmp_path / "file2.csv"],
            read_cfg,
        )

        rows = con.execute("SELECT * FROM raw_input ORDER BY a").fetchall()
        assert rows == [("1", "2"), ("3", "4"), ("5", "6")]
        assert params["normalize_rows_to_columns"] is True


@pytest.mark.policy
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


@pytest.mark.policy
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


@pytest.mark.policy
def test_run_clean_compact_columns_format_renames_and_types(tmp_path: Path):
    """Compact format 'clean_name:DUCKDB_TYPE' in columns: renames + passes only type to DuckDB.

    The DuckDB columns={} option must receive only the raw name and DuckDB type.
    The projection (csv_trim_projection) handles the rename separately.
    """
    raw_dir = tmp_path / "data" / "raw" / "demo" / "2024"
    raw_dir.mkdir(parents=True, exist_ok=True)
    csv_path = raw_dir / "data.csv"
    csv_path.write_text("a;b\n  val_a ; val_b \n", encoding="utf-8")

    sql_path = tmp_path / "clean.sql"
    sql_path.write_text("SELECT a_renamed, b_renamed FROM raw_input", encoding="utf-8")

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
                "trim_whitespace": True,
                "columns": {
                    "a": "a_renamed:VARCHAR",  # compact: raw=a, clean=a_renamed, type=VARCHAR
                    "b": "b_renamed:VARCHAR",
                },
            },
        },
        NoopLogger(),
    )

    out = tmp_path / "data" / "clean" / "demo" / "2024" / "demo_2024_clean.parquet"
    assert out.exists()

    with safe_connect() as con:
        rows = con.execute(
            f"SELECT a_renamed, b_renamed FROM read_parquet('{out.as_posix()}')"
        ).fetchall()
    # Spaces trimmed by TRIM in projection
    assert rows == [("val_a", "val_b")]


@pytest.mark.policy
def test_run_clean_compact_columns_format_with_int_type(tmp_path: Path):
    """Compact format with non-VARCHAR type (e.g. BIGINT) passes type to DuckDB columns={}."""
    raw_dir = tmp_path / "data" / "raw" / "demo" / "2024"
    raw_dir.mkdir(parents=True, exist_ok=True)
    csv_path = raw_dir / "data.csv"
    csv_path.write_text("anno;valore\n2024;42\n", encoding="utf-8")

    sql_path = tmp_path / "clean.sql"
    sql_path.write_text("SELECT anno_clean, valore_clean FROM raw_input", encoding="utf-8")

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
                "columns": {
                    "anno": "anno_clean:BIGINT",
                    "valore": "valore_clean:BIGINT",
                },
            },
        },
        NoopLogger(),
    )

    out = tmp_path / "data" / "clean" / "demo" / "2024" / "demo_2024_clean.parquet"
    assert out.exists()

    with safe_connect() as con:
        rows = con.execute(
            f"SELECT anno_clean, valore_clean FROM read_parquet('{out.as_posix()}')"
        ).fetchall()
    assert rows == [(2024, 42)]


@pytest.mark.policy
def test_run_clean_compact_columns_format_no_trim_whitespace(tmp_path: Path):
    """Compact format rename is applied even when trim_whitespace=False.

    When trim_whitespace=False the code must still produce the clean-name
    projection (raw_name AS clean_name), not just raw_name columns.
    Regression: projection fell back to raw names only.
    """
    raw_dir = tmp_path / "data" / "raw" / "demo" / "2024"
    raw_dir.mkdir(parents=True, exist_ok=True)
    csv_path = raw_dir / "data.csv"
    csv_path.write_text("anno;valore\n2024;42\n", encoding="utf-8")

    sql_path = tmp_path / "clean.sql"
    sql_path.write_text("SELECT anno_clean, valore_clean FROM raw_input", encoding="utf-8")

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
                "trim_whitespace": False,  # key: no TRIM, but rename must work
                "columns": {
                    "anno": "anno_clean:BIGINT",
                    "valore": "valore_clean:BIGINT",
                },
            },
        },
        NoopLogger(),
    )

    out = tmp_path / "data" / "clean" / "demo" / "2024" / "demo_2024_clean.parquet"
    assert out.exists()

    with safe_connect() as con:
        rows = con.execute(
            f"SELECT anno_clean, valore_clean FROM read_parquet('{out.as_posix()}')"
        ).fetchall()
    assert rows == [(2024, 42)]


@pytest.mark.policy
def test_run_clean_align_by_header_missing_middle_column(tmp_path: Path):
    """align_by_header: colonna attesa mancante in mezzo viene riempita con stringa vuota."""
    csv_path = tmp_path / "missing_mid.csv"
    csv_path.write_text("a;b;d\n1;2;4\n5;6;7\n", encoding="utf-8")

    read_cfg = {
        "columns": {"a": "VARCHAR", "b": "VARCHAR", "c": "VARCHAR", "d": "VARCHAR"},
        "normalize_rows_to_columns": True,
        "align_by_header": True,
        "header": True,
        "delim": ";",
        "trim_whitespace": True,
    }
    df = _load_normalized_csv_frame(csv_path, read_cfg, read_cfg["columns"])

    assert list(df.columns) == ["a", "b", "c", "d"]
    assert len(df) == 2
    assert df.iloc[0].tolist() == ["1", "2", "", "4"]
    assert df.iloc[1].tolist() == ["5", "6", "", "7"]


@pytest.mark.policy
def test_run_clean_align_by_header_reorder(tmp_path: Path):
    """align_by_header: colonne in ordine diverso vengono riallineate per nome."""
    csv_path = tmp_path / "reordered.csv"
    csv_path.write_text("c;a;b\nx;1;2\ny;3;4\n", encoding="utf-8")

    read_cfg = {
        "columns": {"a": "VARCHAR", "b": "VARCHAR", "c": "VARCHAR"},
        "normalize_rows_to_columns": True,
        "align_by_header": True,
        "header": True,
        "delim": ";",
        "trim_whitespace": True,
    }
    df = _load_normalized_csv_frame(csv_path, read_cfg, read_cfg["columns"])

    assert list(df.columns) == ["a", "b", "c"]
    assert len(df) == 2
    assert df.iloc[0].tolist() == ["1", "2", "x"]
    assert df.iloc[1].tolist() == ["3", "4", "y"]


@pytest.mark.policy
def test_run_clean_align_by_header_extra_columns_ignored(tmp_path: Path):
    """align_by_header: colonne CSV extra non attese vengono ignorate."""
    csv_path = tmp_path / "extra.csv"
    csv_path.write_text("a;x;b;y\n1;ignored;2;also_ignored\n", encoding="utf-8")

    read_cfg = {
        "columns": {"a": "VARCHAR", "b": "VARCHAR"},
        "normalize_rows_to_columns": True,
        "align_by_header": True,
        "header": True,
        "delim": ";",
        "trim_whitespace": True,
    }
    df = _load_normalized_csv_frame(csv_path, read_cfg, read_cfg["columns"])

    assert list(df.columns) == ["a", "b"]
    assert len(df) == 1
    assert df.iloc[0].tolist() == ["1", "2"]


@pytest.mark.policy
def test_run_clean_align_by_header_no_header_raises(tmp_path: Path):
    """align_by_header=true con header=false deve alzare ValueError."""
    csv_path = tmp_path / "noheader.csv"
    csv_path.write_text("1;2;3\n", encoding="utf-8")

    read_cfg = {
        "columns": {"a": "VARCHAR", "b": "VARCHAR", "c": "VARCHAR"},
        "normalize_rows_to_columns": True,
        "align_by_header": True,
        "header": False,
        "delim": ";",
        "trim_whitespace": True,
    }
    with pytest.raises(ValueError, match="align_by_header=true requires header=true"):
        _load_normalized_csv_frame(csv_path, read_cfg, read_cfg["columns"])


@pytest.mark.policy
def test_run_clean_align_by_header_integration(tmp_path: Path):
    """align_by_header funziona end-to-end via run_clean con colonna mancante."""
    raw_dir = tmp_path / "data" / "raw" / "demo" / "2024"
    raw_dir.mkdir(parents=True, exist_ok=True)
    csv_path = raw_dir / "data.csv"
    csv_path.write_text(
        "Anno di Riferimento;Codice Regione;Consumi sanitari\n2024;15;100.5\n2023;16;200.3\n",
        encoding="utf-8",
    )

    sql_path = tmp_path / "clean.sql"
    sql_path.write_text(
        "SELECT "
        'try_cast("Anno di Riferimento" AS INTEGER) AS anno, '
        'try_cast("Oneri Finanziari" AS DOUBLE) AS oneri, '
        'try_cast("Consumi sanitari" AS DOUBLE) AS consumi '
        "FROM raw_input",
        encoding="utf-8",
    )

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
                "columns": {
                    "Anno di Riferimento": "VARCHAR",
                    "Codice Regione": "VARCHAR",
                    "Oneri Finanziari": "VARCHAR",
                    "Consumi sanitari": "VARCHAR",
                },
                "normalize_rows_to_columns": True,
                "align_by_header": True,
                "trim_whitespace": True,
            },
        },
        NoopLogger(),
    )

    out = tmp_path / "data" / "clean" / "demo" / "2024" / "demo_2024_clean.parquet"
    assert out.exists()

    with safe_connect() as con:
        rows = con.execute(
            f"SELECT anno, oneri, consumi FROM read_parquet('{out.as_posix()}') ORDER BY anno"
        ).fetchall()
    assert rows == [(2023, None, 200.3), (2024, None, 100.5)]


@pytest.mark.policy
def test_run_clean_align_by_header_requires_normalize_config():
    """CleanReadConfig con align_by_header=true senza normalize_rows_to_columns alza ValueError."""
    from toolkit.core.config_models.clean import CleanReadConfig

    with pytest.raises(
        ValueError, match="align_by_header=true requires normalize_rows_to_columns=true"
    ):
        CleanReadConfig(
            align_by_header=True,
            normalize_rows_to_columns=False,
            columns={"a": "VARCHAR", "b": "VARCHAR"},
        )


@pytest.mark.policy
def test_run_clean_align_by_header_requires_normalize_runtime(tmp_path: Path):
    """run_clean con align_by_header=true senza normalize_rows_to_columns alza ValueError con causa."""
    raw_dir = tmp_path / "data" / "raw" / "demo" / "2024"
    raw_dir.mkdir(parents=True, exist_ok=True)
    csv_path = raw_dir / "data.csv"
    csv_path.write_text("b;a\n1;2\n", encoding="utf-8")

    sql_path = tmp_path / "clean.sql"
    sql_path.write_text("SELECT a, b FROM raw_input", encoding="utf-8")

    with pytest.raises(ValueError) as exc_info:
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
                    "columns": {"a": "VARCHAR", "b": "VARCHAR"},
                    "align_by_header": True,
                    # normalize_rows_to_columns: NOT SET — should fail
                },
            },
            NoopLogger(),
        )
    # L'errore originale è incatenato come causa diretta
    assert exc_info.value.__cause__ is not None
    assert "align_by_header=true requires normalize_rows_to_columns=true" in str(
        exc_info.value.__cause__
    )
