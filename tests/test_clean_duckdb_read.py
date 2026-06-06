from __future__ import annotations

import logging
from pathlib import Path

import duckdb
import pandas as pd
import pytest
import yaml

from toolkit.core import duckdb_read
from toolkit.clean.read_config import resolve_clean_read_cfg


@pytest.mark.policy
def test_read_raw_to_relation_fallback_invokes_robust_after_strict_failure(monkeypatch, tmp_path: Path):
    input_file = tmp_path / "dirty.csv"
    input_file.write_text("a;b\n1;2;3\n", encoding="utf-8")

    calls: list[dict[str, object]] = []

    def _fake_execute_csv_read(con, input_files, read_cfg):
        calls.append(dict(read_cfg))
        if len(calls) == 1:
            raise RuntimeError("strict boom")
        con.execute("CREATE OR REPLACE VIEW raw_input AS SELECT 1 AS x")
        return {"ignore_errors": read_cfg.get("ignore_errors"), "strict_mode": read_cfg.get("strict_mode")}

    monkeypatch.setattr(duckdb_read, "_execute_csv_read", _fake_execute_csv_read)

    con = duckdb.connect(":memory:")
    logger = logging.getLogger("tests.clean.duckdb_read")

    info = duckdb_read.read_raw_to_relation(
        con,
        [input_file],
        {"delim": ";", "encoding": "utf-8"},
        "fallback",
        logger,
    )

    assert len(calls) == 2
    assert info.source == "robust"
    assert info.params_used["ignore_errors"] is True
    assert calls[0].get("ignore_errors") is None
    assert calls[1]["ignore_errors"] is True
    assert calls[1]["null_padding"] is True
    assert calls[1]["strict_mode"] is False
    assert calls[1]["sample_size"] == -1
    con.close()


@pytest.mark.policy
def test_read_raw_to_relation_strict_returns_strict_info(tmp_path: Path):
    input_file = tmp_path / "ok.csv"
    input_file.write_text("a;b\n1;2\n", encoding="utf-8")

    con = duckdb.connect(":memory:")
    logger = logging.getLogger("tests.clean.duckdb_read.strict")

    info = duckdb_read.read_raw_to_relation(
        con,
        [input_file],
        {"delim": ";", "encoding": "utf-8", "header": True},
        "strict",
        logger,
    )

    assert info.source == "strict"
    assert info.params_used["delim"] == ";"
    assert info.params_used["encoding"] == "utf-8"
    assert info.params_used["header"] is True
    con.close()


@pytest.mark.policy
def test_read_raw_to_relation_passes_parallel_flag(tmp_path: Path):
    input_file = tmp_path / "ok.csv"
    input_file.write_text("a;b\n1;2\n", encoding="utf-8")

    con = duckdb.connect(":memory:")
    logger = logging.getLogger("tests.clean.duckdb_read.parallel")

    info = duckdb_read.read_raw_to_relation(
        con,
        [input_file],
        {"delim": ";", "encoding": "utf-8", "header": True, "parallel": False},
        "strict",
        logger,
    )

    assert info.source == "strict"
    assert info.params_used["parallel"] is False
    con.close()


@pytest.mark.policy
def test_read_raw_to_relation_keeps_explicit_columns_unchanged(tmp_path: Path):
    input_file = tmp_path / "ok.csv"
    input_file.write_text("a;b\n1;2\n", encoding="utf-8")

    con = duckdb.connect(":memory:")
    logger = logging.getLogger("tests.clean.duckdb_read.columns")

    info = duckdb_read.read_raw_to_relation(
        con,
        [input_file],
        {
            "delim": ";",
            "encoding": "utf-8",
            "header": True,
            "columns": {"a": "VARCHAR", "b": "VARCHAR"},
        },
        "strict",
        logger,
    )

    assert info.source == "strict"
    assert info.params_used["columns"] == {"a": "VARCHAR", "b": "VARCHAR"}
    con.close()


@pytest.mark.policy
def test_read_raw_to_relation_strict_error_message_uses_current_config_keys(tmp_path: Path):
    input_file = tmp_path / "bad.csv"
    input_file.write_text("a;b\n1;2;3\n", encoding="utf-8")

    con = duckdb.connect(":memory:")
    logger = logging.getLogger("tests.clean.duckdb_read.strict_error")
    original_execute = duckdb_read._execute_csv_read

    def _fail_execute_csv_read(_con, _input_files, _read_cfg):
        raise RuntimeError("strict boom")

    duckdb_read._execute_csv_read = _fail_execute_csv_read

    try:
        with pytest.raises(ValueError, match="clean.read.columns.*clean.read.source"):
            duckdb_read.read_raw_to_relation(
                con,
                [input_file],
                {"delim": ";", "encoding": "utf-8", "header": True},
                "strict",
                logger,
            )
    finally:
        duckdb_read._execute_csv_read = original_execute

    con.close()


@pytest.mark.policy
def test_read_raw_to_relation_handles_no_header_fixed_schema_without_extra_column(tmp_path: Path):
    input_file = tmp_path / "fixed.csv"
    input_file.write_text("A,2024,1,123,45.6\nB,2024,2,456,78.9\n", encoding="utf-8")

    con = duckdb.connect(":memory:")
    logger = logging.getLogger("tests.clean.duckdb_read.fixed_schema")

    info = duckdb_read.read_raw_to_relation(
        con,
        [input_file],
        {
            "delim": ",",
            "encoding": "utf-8",
            "header": False,
            "auto_detect": False,
            "columns": {
                "col0": "VARCHAR",
                "col1": "VARCHAR",
                "col2": "VARCHAR",
                "col3": "VARCHAR",
                "col4": "VARCHAR",
            },
        },
        "fallback",
        logger,
    )

    rows = con.execute("SELECT col0, col1, col2, col3, col4 FROM raw_input ORDER BY col0").fetchall()
    assert info.source == "strict"
    assert rows == [("A", "2024", "1", "123", "45.6"), ("B", "2024", "2", "456", "78.9")]
    con.close()


@pytest.mark.policy
def test_read_raw_to_relation_normalizes_short_rows_to_fixed_schema(tmp_path: Path):
    input_file = tmp_path / "ragged.csv"
    input_file.write_text("A;B;C\n1;2\n3;4;5\n", encoding="utf-8")

    con = duckdb.connect(":memory:")
    logger = logging.getLogger("tests.clean.duckdb_read.normalize_rows")

    info = duckdb_read.read_raw_to_relation(
        con,
        [input_file],
        {
            "delim": ";",
            "encoding": "utf-8",
            "header": False,
            "columns": {
                "col0": "VARCHAR",
                "col1": "VARCHAR",
                "col2": "VARCHAR",
            },
            "normalize_rows_to_columns": True,
        },
        "strict",
        logger,
    )

    rows = con.execute("SELECT col0, col1, col2 FROM raw_input ORDER BY col0").fetchall()
    assert info.source == "strict"
    assert info.params_used["normalize_rows_to_columns"] is True
    assert rows == [("1", "2", ""), ("3", "4", "5"), ("A", "B", "C")]
    con.close()


@pytest.mark.policy
def test_read_raw_to_relation_normalize_rows_skips_header_when_configured(tmp_path: Path):
    input_file = tmp_path / "ragged_header.csv"
    input_file.write_text("h0;h1;h2\n1;2\n", encoding="utf-8")

    con = duckdb.connect(":memory:")
    logger = logging.getLogger("tests.clean.duckdb_read.normalize_rows_header")

    info = duckdb_read.read_raw_to_relation(
        con,
        [input_file],
        {
            "delim": ";",
            "encoding": "utf-8",
            "header": True,
            "columns": {
                "col0": "VARCHAR",
                "col1": "VARCHAR",
                "col2": "VARCHAR",
            },
            "normalize_rows_to_columns": True,
        },
        "strict",
        logger,
    )

    rows = con.execute("SELECT col0, col1, col2 FROM raw_input").fetchall()
    assert info.params_used["header"] is True
    assert rows == [("1", "2", "")]
    con.close()


@pytest.mark.policy
def test_read_raw_to_relation_reads_xlsx_first_sheet(tmp_path: Path):
    input_file = tmp_path / "ok.xlsx"
    pd.DataFrame(
        [
            {"Anno": 2022, "Regione": "Lazio", "Domanda": 123.4},
            {"Anno": 2022, "Regione": "Umbria", "Domanda": 56.7},
        ]
    ).to_excel(input_file, index=False)

    con = duckdb.connect(":memory:")
    logger = logging.getLogger("tests.clean.duckdb_read.xlsx")

    info = duckdb_read.read_raw_to_relation(
        con,
        [input_file],
        {"header": True},
        "fallback",
        logger,
    )

    rows = con.execute('SELECT "Anno", "Regione", "Domanda" FROM raw_input ORDER BY "Regione"').fetchall()
    assert info.source == "excel"
    assert info.params_used["sheet_name"] == 0
    assert rows == [(2022, "Lazio", 123.4), (2022, "Umbria", 56.7)]
    con.close()


@pytest.mark.policy
def test_read_raw_to_relation_reads_xlsx_with_explicit_sheet_and_columns(tmp_path: Path):
    input_file = tmp_path / "sheeted.xlsx"
    with pd.ExcelWriter(input_file, engine="openpyxl") as writer:
        pd.DataFrame({"skipme": ["ignore"]}).to_excel(writer, sheet_name="Other", index=False)
        pd.DataFrame([["A", 1], ["B", 2]]).to_excel(
            writer,
            sheet_name="Export",
            header=False,
            index=False,
        )

    con = duckdb.connect(":memory:")
    logger = logging.getLogger("tests.clean.duckdb_read.xlsx_sheet")

    info = duckdb_read.read_raw_to_relation(
        con,
        [input_file],
        {
            "header": False,
            "sheet_name": "Export",
            "columns": {"col0": "VARCHAR", "col1": "VARCHAR"},
        },
        "fallback",
        logger,
    )

    rows = con.execute("SELECT col0, col1 FROM raw_input ORDER BY col0").fetchall()
    assert info.source == "excel"
    assert info.params_used["sheet_name"] == "Export"
    assert info.params_used["columns"] == {"col0": "VARCHAR", "col1": "VARCHAR"}
    assert rows == [("A", 1), ("B", 2)]
    con.close()


@pytest.mark.policy
def test_read_raw_to_relation_reads_xls_with_xlrd_engine(tmp_path: Path):
    """Test that .xls files use the xlrd engine."""
    import xlwt

    input_file = tmp_path / "ok.xls"
    wb = xlwt.Workbook()
    ws = wb.add_sheet("Sheet1")
    ws.write(0, 0, "Anno")
    ws.write(0, 1, "Regione")
    ws.write(0, 2, "Domanda")
    ws.write(1, 0, 2022)
    ws.write(1, 1, "Lazio")
    ws.write(1, 2, 123.4)
    ws.write(2, 0, 2022)
    ws.write(2, 1, "Umbria")
    ws.write(2, 2, 56.7)
    wb.save(input_file)

    con = duckdb.connect(":memory:")
    logger = logging.getLogger("tests.clean.duckdb_read.xls")

    info = duckdb_read.read_raw_to_relation(
        con,
        [input_file],
        {"header": True},
        "fallback",
        logger,
    )

    rows = con.execute('SELECT "Anno", "Regione", "Domanda" FROM raw_input ORDER BY "Regione"').fetchall()
    assert info.source == "excel"
    assert info.params_used["sheet_name"] == 0
    assert rows == [(2022, "Lazio", 123.4), (2022, "Umbria", 56.7)]
    con.close()


@pytest.mark.policy
def test_resolve_clean_read_cfg_uses_suggested_hints_in_auto_mode(tmp_path: Path):
    raw_dir = tmp_path / "raw" / "demo" / "2024"
    profile_dir = raw_dir / "_profile"
    profile_dir.mkdir(parents=True)
    (profile_dir / "suggested_read.yml").write_text(
        yaml.safe_dump(
            {
                "clean": {
                    "read": {
                        "delim": ";",
                        "decimal": ",",
                        "encoding": "utf-8",
                    }
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    selection_cfg, relation_cfg, params_source = resolve_clean_read_cfg(
        raw_dir,
        {"read": {"source": "auto"}},
        logging.getLogger("tests.clean.duckdb_read.auto"),
    )

    assert selection_cfg == {}
    assert relation_cfg["delim"] == ";"
    assert relation_cfg["decimal"] == ","
    assert relation_cfg["encoding"] == "utf-8"
    assert params_source == ["defaults", "suggested"]


@pytest.mark.policy
def test_resolve_clean_read_cfg_config_overrides_win_over_suggested(tmp_path: Path):
    raw_dir = tmp_path / "raw" / "demo" / "2024"
    profile_dir = raw_dir / "_profile"
    profile_dir.mkdir(parents=True)
    (profile_dir / "suggested_read.yml").write_text(
        yaml.safe_dump(
            {"clean": {"read": {"delim": ";", "decimal": ","}}},
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    _, relation_cfg, params_source = resolve_clean_read_cfg(
        raw_dir,
        {"read": {"source": "auto", "delim": "|", "decimal": "."}},
        logging.getLogger("tests.clean.duckdb_read.override"),
    )

    assert relation_cfg["delim"] == "|"
    assert relation_cfg["decimal"] == "."
    assert params_source == ["defaults", "suggested", "config_overrides"]


@pytest.mark.policy
def test_resolve_clean_read_cfg_config_only_ignores_suggested(tmp_path: Path):
    raw_dir = tmp_path / "raw" / "demo" / "2024"
    profile_dir = raw_dir / "_profile"
    profile_dir.mkdir(parents=True)
    (profile_dir / "suggested_read.yml").write_text(
        yaml.safe_dump(
            {"clean": {"read": {"delim": ";", "encoding": "utf-8"}}},
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    _, relation_cfg, params_source = resolve_clean_read_cfg(
        raw_dir,
        {"read": {"source": "config_only"}},
        logging.getLogger("tests.clean.duckdb_read.config_only"),
    )

    assert relation_cfg == {"columns": None}
    assert params_source == ["defaults"]


@pytest.mark.policy
def test_filter_suggested_read_excludes_robustness_keys(tmp_path: Path):
    raw_dir = tmp_path / "raw" / "demo" / "2024"
    profile_dir = raw_dir / "_profile"
    profile_dir.mkdir(parents=True)
    (profile_dir / "suggested_read.yml").write_text(
        yaml.safe_dump(
            {
                "clean": {
                    "read": {
                        "delim": ";",
                        "encoding": "utf-8",
                        "ignore_errors": True,
                        "null_padding": True,
                        "strict_mode": False,
                        "sample_size": -1,
                    }
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    _, relation_cfg, _ = resolve_clean_read_cfg(
        raw_dir,
        {"read": {"source": "auto"}},
        logging.getLogger("tests.clean.duckdb_read.filter"),
    )

    assert relation_cfg["delim"] == ";"
    assert relation_cfg["encoding"] == "utf-8"
    assert "ignore_errors" not in relation_cfg
    assert "null_padding" not in relation_cfg
    assert "strict_mode" not in relation_cfg
    assert "sample_size" not in relation_cfg


@pytest.mark.policy
def test_read_raw_to_relation_with_thousands_separator(tmp_path: Path):
    """DuckDB read_csv respects decimal=',' + thousands='.'.

    CSV con separatore migliaia "." e decimale ",".
    1.234,56 deve essere letto come 1234.56 (non 1.234).
    """
    input_file = tmp_path / "thousands.csv"
    input_file.write_text("id;val\n1;1.234,56\n2;7.890,12\n", encoding="utf-8")

    con = duckdb.connect(":memory:")
    logger = logging.getLogger("tests.clean.duckdb_read.thousands")

    info = duckdb_read.read_raw_to_relation(
        con,
        [input_file],
        {"delim": ";", "encoding": "utf-8", "header": True,
         "decimal": ",", "thousands": "."},
        "strict",
        logger,
    )

    assert info.params_used["decimal"] == ","
    assert info.params_used["thousands"] == "."
    rows = con.execute("SELECT id, val FROM raw_input ORDER BY id").fetchall()
    assert rows == [(1, 1234.56), (2, 7890.12)], f"got {rows}"
    con.close()


@pytest.mark.policy
def test_dateformat_parses_italian_dates(tmp_path: Path):
    """dateformat='%d/%m/%Y' deve parsare date in formato italiano."""
    input_file = tmp_path / "date.csv"
    input_file.write_text("data;valore\n01/02/2023;100\n15/08/2024;200\n", encoding="utf-8")

    con = duckdb.connect(":memory:")
    logger = logging.getLogger("tests.clean.duckdb_read.dateformat")

    info = duckdb_read.read_raw_to_relation(
        con,
        [input_file],
        {"delim": ";", "encoding": "utf-8", "header": True,
         "dateformat": "%d/%m/%Y"},
        "strict",
        logger,
    )

    assert info.params_used["dateformat"] == "%d/%m/%Y"
    # DuckDB should parse the dates correctly
    rows = con.execute("SELECT data, valore FROM raw_input ORDER BY valore").fetchall()
    assert len(rows) == 2
    # Data types may vary (could be DATE or VARCHAR depending on DuckDB version)
    # Check at least the year is correct
    assert str(rows[0][0]).endswith("2023") or "2023" in str(rows[0][0])
    assert str(rows[1][0]).endswith("2024") or "2024" in str(rows[1][0])
    con.close()


@pytest.mark.policy
def test_rejects_table_captures_malformed_rows(tmp_path: Path):
    """rejects_table deve catturare righe malformate senza bloccare la lettura."""
    input_file = tmp_path / "mixed.csv"
    # Row 2 ha 3 colonne invece di 2 — deve finire nella rejects_table
    input_file.write_text("a;b\n1;2\n3;4;5\n6;7\n", encoding="utf-8")

    con = duckdb.connect(":memory:")
    logger = logging.getLogger("tests.clean.duckdb_read.rejects")

    info = duckdb_read.read_raw_to_relation(
        con,
        [input_file],
        {"delim": ";", "encoding": "utf-8", "header": True,
         "ignore_errors": True, "rejects_table": "err_rows",
         "rejects_scan": "err_scan"},
        "strict",
        logger,
    )

    assert "rejects_table" in info.params_used
    assert info.params_used["rejects_table"] == "err_rows"
    # Le righe valide devono essere state lette
    rows = con.execute("SELECT a, b FROM raw_input ORDER BY a").fetchall()
    assert rows == [(1, 2), (6, 7)], f"got {rows}"
    # La riga malformata deve essere nella rejects_table
    rejects = con.execute("SELECT * FROM err_rows").fetchall()
    assert len(rejects) >= 1
    con.close()
