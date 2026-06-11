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
def test_read_raw_to_relation_fallback_invokes_robust_after_strict_failure(
    monkeypatch, tmp_path: Path
):
    input_file = tmp_path / "dirty.csv"
    input_file.write_text("a;b\n1;2;3\n", encoding="utf-8")

    calls: list[dict[str, object]] = []

    def _fake_execute_csv_read(con, input_files, read_cfg):
        calls.append(dict(read_cfg))
        if len(calls) == 1:
            raise RuntimeError("strict boom")
        con.execute("CREATE OR REPLACE VIEW raw_input AS SELECT 1 AS x")
        return {
            "ignore_errors": read_cfg.get("ignore_errors"),
            "strict_mode": read_cfg.get("strict_mode"),
        }

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

    rows = con.execute(
        "SELECT col0, col1, col2, col3, col4 FROM raw_input ORDER BY col0"
    ).fetchall()
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

    rows = con.execute(
        'SELECT "Anno", "Regione", "Domanda" FROM raw_input ORDER BY "Regione"'
    ).fetchall()
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

    rows = con.execute(
        'SELECT "Anno", "Regione", "Domanda" FROM raw_input ORDER BY "Regione"'
    ).fetchall()
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
        {"delim": ";", "encoding": "utf-8", "header": True, "decimal": ",", "thousands": "."},
        "strict",
        logger,
    )

    assert info.params_used["decimal"] == ","
    assert info.params_used["thousands"] == "."
    rows = con.execute("SELECT id, val FROM raw_input ORDER BY id").fetchall()
    assert rows == [(1, 1234.56), (2, 7890.12)], f"got {rows}"
    con.close()


@pytest.mark.contract
def test_read_raw_to_relation_injects_column_from_multiple_sources(tmp_path: Path):
    """inject_column aggiunge colonna fissa per ogni source prima dell'unione."""
    file_a = tmp_path / "reg_a.csv"
    file_a.write_text("id,nome,valore\n1,Sanità,1500\n2,Istruzione,2200\n", encoding="utf-8")
    file_b = tmp_path / "reg_b.csv"
    file_b.write_text("id,nome,valore\n1,Sanità,900\n3,Trasporti,700\n", encoding="utf-8")

    from toolkit.core.input_file import RawInputFile

    inputs = [
        RawInputFile(path=file_a, inject_column={"cod_regione": "13"}),
        RawInputFile(path=file_b, inject_column={"cod_regione": "14"}),
    ]

    logger = logging.getLogger("tests.clean.duckdb_read.inject")
    con = duckdb.connect(":memory:")
    try:
        info = duckdb_read.read_raw_to_relation(
            con, inputs, {"delim": ",", "encoding": "utf-8"}, "fallback", logger
        )
        rows = con.execute(
            "SELECT cod_regione, nome FROM raw_input ORDER BY cod_regione, nome"
        ).fetchall()
        assert rows == [
            ("13", "Istruzione"),
            ("13", "Sanità"),
            ("14", "Sanità"),
            ("14", "Trasporti"),
        ], f"Unexpected rows: {rows}"
        assert "inject_column" in info.params_used
        assert info.params_used["inject_column"]["cod_regione"]["reg_a.csv"] == "13"
        assert info.params_used["inject_column"]["cod_regione"]["reg_b.csv"] == "14"
    finally:
        con.close()


@pytest.mark.contract
def test_read_raw_to_relation_inject_column_without_columns_cfg(tmp_path: Path):
    """inject_column funziona anche senza clean.read.columns configurato."""
    file_a = tmp_path / "data_a.csv"
    file_a.write_text("x,y\n1,2\n3,4\n", encoding="utf-8")
    file_b = tmp_path / "data_b.csv"
    file_b.write_text("x,y\n5,6\n", encoding="utf-8")

    from toolkit.core.input_file import RawInputFile

    inputs = [
        RawInputFile(path=file_a, inject_column={"fonte": "A"}),
        RawInputFile(path=file_b, inject_column={"fonte": "B"}),
    ]

    logger = logging.getLogger("tests.clean.duckdb_read.inject")
    con = duckdb.connect(":memory:")
    try:
        duckdb_read.read_raw_to_relation(
            con, inputs, {"delim": ",", "encoding": "utf-8"}, "fallback", logger
        )
        rows = con.execute("SELECT fonte, x, y FROM raw_input ORDER BY fonte, x").fetchall()
        assert rows == [
            ("A", 1, 2),
            ("A", 3, 4),
            ("B", 5, 6),
        ], f"Unexpected rows: {rows}"
    finally:
        con.close()


@pytest.mark.policy
def test_read_raw_to_relation_plain_path_no_inject_still_works(tmp_path: Path):
    """Senza inject_column, list[Path] funziona ancora (backward compat)."""
    input_file = tmp_path / "simple.csv"
    input_file.write_text("a,b\n1,2\n3,4\n", encoding="utf-8")

    logger = logging.getLogger("tests.clean.duckdb_read.backward")
    con = duckdb.connect(":memory:")
    try:
        info = duckdb_read.read_raw_to_relation(
            con, [input_file], {"delim": ",", "encoding": "utf-8"}, "fallback", logger
        )
        rows = con.execute("SELECT * FROM raw_input ORDER BY a").fetchall()
        assert rows == [(1, 2), (3, 4)]
        assert "inject_column" not in info.params_used
    finally:
        con.close()


@pytest.mark.pure_unit
def test_build_raw_input_map_with_inject():
    """build_raw_input_map costruisce la mappa filename→inject_column da dict config."""
    from toolkit.core.input_file import build_raw_input_map

    sources = [
        {"name": "src_a", "args": {"filename": "data_a.csv"}, "inject_column": {"regione": "13"}},
        {
            "name": "src_b",
            "args": {"filename": "data_b.csv"},
            "inject_column": {"regione": "14", "fonte": "B"},
        },
        {"name": "src_c", "args": {"filename": "data_c.csv"}},  # senza inject
    ]
    result = build_raw_input_map(sources, 2026)
    assert result == {
        "data_a.csv": {"regione": "13"},
        "data_b.csv": {"regione": "14", "fonte": "B"},
        "data_c.csv": None,
    }


@pytest.mark.pure_unit
def test_build_raw_input_map_empty():
    """build_raw_input_map con lista vuota o None restituisce {}."""
    from toolkit.core.input_file import build_raw_input_map

    assert build_raw_input_map(None, 2026) == {}
    assert build_raw_input_map([], 2026) == {}


@pytest.mark.pure_unit
def test_build_raw_input_map_year_placeholder():
    """build_raw_input_map risolve {year} nel filename."""
    from toolkit.core.input_file import build_raw_input_map

    sources = [
        {"name": "src", "args": {"filename": "data_{year}.csv"}, "inject_column": {"regione": "01"}}
    ]
    result = build_raw_input_map(sources, 2026)
    assert "data_2026.csv" in result
    assert "data_{year}.csv" not in result


@pytest.mark.pure_unit
def test_enrich_input_files():
    """enrich_input_files abbina path a inject_column tramite filename."""
    from toolkit.core.input_file import enrich_input_files
    from pathlib import Path

    sources = [
        {"name": "src_a", "args": {"filename": "alfa.csv"}, "inject_column": {"fonte": "A"}},
        {"name": "src_b", "args": {"filename": "beta.csv"}, "inject_column": {"fonte": "B"}},
        {"name": "src_c", "args": {"filename": "gamma.csv"}},
    ]
    paths = [
        Path("/data/alfa.csv"),
        Path("/data/beta.csv"),
        Path("/data/gamma.csv"),
        Path("/data/delta.csv"),
    ]
    enriched = enrich_input_files(paths, sources, 2026)

    assert len(enriched) == 4
    assert enriched[0].inject_column == {"fonte": "A"}
    assert enriched[1].inject_column == {"fonte": "B"}
    assert enriched[2].inject_column is None  # src_c senza inject
    assert enriched[3].inject_column is None  # delta.csv non in sources
    assert enriched[0].path.name == "alfa.csv"


@pytest.mark.contract
def test_parquet_inject_column_escapes_apostrophe(tmp_path: Path):
    """inject_column su parquet: valori con apostrofo non rompono la query."""
    import pandas as pd

    file_a = tmp_path / "reg_a.parquet"
    pd.DataFrame({"id": [1, 2], "val": [10, 20]}).to_parquet(file_a)
    file_b = tmp_path / "reg_b.parquet"
    pd.DataFrame({"id": [3], "val": [30]}).to_parquet(file_b)

    from toolkit.core.input_file import RawInputFile

    inputs = [
        RawInputFile(path=file_a, inject_column={"nome": "L'Aquila", "regione": "Abruzzo"}),
        RawInputFile(
            path=file_b, inject_column={"nome": "Valle d'Aosta", "regione": "Valle d'Aosta"}
        ),
    ]

    logger = logging.getLogger("tests.clean.duckdb_read.inject_parquet")
    con = duckdb.connect(":memory:")
    try:
        info = duckdb_read.read_raw_to_relation(con, inputs, None, "fallback", logger)
        rows = con.execute("SELECT nome, regione, id FROM raw_input ORDER BY nome, id").fetchall()
        assert rows == [
            ("L'Aquila", "Abruzzo", 1),
            ("L'Aquila", "Abruzzo", 2),
            ("Valle d'Aosta", "Valle d'Aosta", 3),
        ], f"Unexpected rows: {rows}"
        assert "inject_column" in info.params_used
    finally:
        con.close()
