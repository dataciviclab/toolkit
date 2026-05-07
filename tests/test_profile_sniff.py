from pathlib import Path

import pytest
import toolkit.profile.raw as profile_raw_module
from toolkit.cli.cmd_profile import write_suggested_read_yml
from toolkit.profile._column_profile import _build_mapping_suggestions
from toolkit.profile.raw import (
    _build_read_csv_opts,
    _profile_excel,
    build_suggested_read_cfg,
    profile_raw,
    profile_with_read_cfg,
    sniff_delim,
    sniff_decimal,
    sniff_encoding,
    sniff_source_file,
    suggest_skip,
)


@pytest.mark.policy
def test_sniff_delim_tab():
    sample = "a\tb\tc\n1\t2\t3\n4\t5\t6\n"
    assert sniff_delim(sample) == "\t"


@pytest.mark.policy
def test_sniff_decimal_it_comma():
    sample = "val\n1.234,56\n7.890,12\n"
    assert sniff_decimal(sample) == ","


@pytest.mark.policy
def test_sniff_encoding_latin1(tmp_path: Path):
    p = tmp_path / "latin1.csv"
    p.write_bytes("nome\ncittà\n".encode("latin-1"))
    enc, txt = sniff_encoding(p)
    assert enc.lower() in {"latin-1", "windows-1252", "cp1252"}
    assert "citt" in txt.lower()


@pytest.mark.policy
def test_suggest_skip_for_preamble_line():
    sample = (
        "Produzione e raccolta differenziata su scala comunale anno 2022 (ISPRA)\n"
        "IstatComune;Regione;Provincia;Comune\n"
        "01001001;Piemonte;Torino;AGLIE'\n"
    )
    assert suggest_skip(sample, ";") == 1


@pytest.mark.policy
def test_profile_raw_suggests_robust_read_options_for_dirty_csv(tmp_path: Path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    (raw_dir / "dirty.csv").write_text("title row\ncol1;col2\n1;2;3\n4;5\n", encoding="utf-8")

    profile = profile_raw(raw_dir, "demo", 2024)

    assert profile.skip_suggested == 1
    assert profile.robust_read_suggested is True

    suggested_path = write_suggested_read_yml(tmp_path / "_profile", profile.__dict__)
    suggested = suggested_path.read_text(encoding="utf-8")
    assert "skip: 1" in suggested
    assert "auto_detect: false" in suggested
    assert "strict_mode: false" in suggested
    assert "null_padding: true" in suggested
    assert "ignore_errors: true" in suggested


@pytest.mark.policy
def test_profile_raw_writes_suggested_read_even_when_duckdb_sniff_fails(
    tmp_path: Path, monkeypatch
):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    (raw_dir / "dirty.csv").write_text(
        "report 2024\ncol1;col2;val\n1;2;1,23\n3;4;5,67\n",
        encoding="utf-8",
    )

    class BrokenConnection:
        def execute(self, *args, **kwargs):
            raise RuntimeError("boom")

        def close(self):
            return None

    monkeypatch.setattr(profile_raw_module.duckdb, "connect", lambda *_args, **_kwargs: BrokenConnection())

    profile = profile_raw(raw_dir, "demo", 2024)
    suggested_cfg = build_suggested_read_cfg(profile)

    assert profile.warnings
    assert profile.header_line == "col1;col2;val"
    assert suggested_cfg["delim"] == ";"
    assert suggested_cfg["decimal"] == ","
    assert suggested_cfg["encoding"] == "utf-8"
    assert suggested_cfg["header"] is True

    out_dir = tmp_path / "_profile"
    suggested_path = write_suggested_read_yml(out_dir, profile.__dict__)
    assert suggested_path.exists()
    suggested = suggested_path.read_text(encoding="utf-8")
    assert 'delim: ";"' in suggested
    assert 'decimal: ","' in suggested
    assert 'encoding: "utf-8"' in suggested
    assert "header: true" in suggested


@pytest.mark.policy
def test_build_read_csv_opts_keeps_header_and_skip_for_profiler():
    opts = _build_read_csv_opts(
        {
            "delim": ";",
            "encoding": "utf8",
            "header": False,
            "skip": 2,
            "max_line_size": 4096,
        }
    )

    assert "union_by_name=true" in opts
    assert "sep=';'" in opts
    assert "encoding='utf-8'" in opts
    assert "max_line_size=4096" in opts
    assert "header=false" in opts
    assert "skip=2" in opts


@pytest.mark.policy
def test_build_mapping_suggestions_varchar_falls_back_to_heuristics():
    """When DuckDB reports VARCHAR (generic), regex heuristics must surface.

    Italian decimal format 1.234,56 should produce type=float + parse=number_it,
    not plain str — DuckDB sees VARCHAR but the heuristic detects number_it.
    Regression test for the DuckDB-override fix.
    """
    sample = [
        {"Importo": "1.234,56"},
        {"Importo": "2.345,67"},
    ]
    duckdb_types = {"Importo": "VARCHAR"}

    result = _build_mapping_suggestions(["Importo"], sample, duckdb_types=duckdb_types)
    spec = result["Importo"]

    # Must NOT be plain str — heuristic should detect Italian number
    assert spec["type"] == "float", f"Expected float, got {spec['type']}"
    assert spec.get("parse", {}).get("kind") == "number_it", (
        f"Expected parse={{kind: number_it}}, got {spec.get('parse')}"
    )


@pytest.mark.policy
def test_profile_raw_mismatch_header_data_cols_triggers_null_padding(tmp_path: Path):
    """When DESCRIBE columns != true header token count, retry with null_padding.

    IRPEF comunale real file: header has 50 cols, data rows have 52 cols.
    suggest_skip returns 0 because 49 < 51 but 49 > 1 (first_count <= 1 fails).

    In this test: header has 2 tokens, data rows have 4 tokens.
    suggest_skip returns 1 (first_count=1 <= 1 and second_count=3 >= 3).
    The profiler must detect that the true header (line 0) has fewer tokens
    than the data columns returned by DESCRIBE, and emit a mismatch warning.

    Note: the test file structure mirrors the IRPEF column-count pattern,
    not the exact delimiter counts (skip logic differs).
    """
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()

    # Header: 2 tokens (1 comma), Data rows: 4 tokens (3 commas)
    # first_count=1 <= 1 and second_count=3 >= 3 → suggest_skip=1
    # But the mismatch is between true header (line 0 = 2 tokens) and data cols
    (raw_dir / "test.csv").write_text(
        "Codice,Descrizione\n"  # 1 comma = 2 tokens at line 0
        "A001,B001,C001,D001\n"  # 3 commas = 4 tokens at line 1+
        "A002,B002,C002,D002\n",
        encoding="utf-8",
    )

    profile = profile_raw(raw_dir, "demo", 2024)

    # Mismatch detected: true_header=2 tokens, data=4 cols
    mismatch_warnings = [w for w in profile.warnings if "mismatch" in w]
    assert mismatch_warnings, f"Expected mismatch warning, got {profile.warnings}"

    # columns_raw reflects 4 data columns from DESCRIBE
    assert len(profile.columns_raw) == 4


@pytest.mark.policy
def test_sniff_source_file_returns_all_keys(tmp_path: Path):
    """sniff_source_file must return the full set of keys used by consumers."""
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("col1;col2\n1;2\n", encoding="utf-8")

    hints = sniff_source_file(csv_path)

    expected_keys = {
        "file_used",
        "encoding_suggested",
        "delim_suggested",
        "decimal_suggested",
        "skip_suggested",
        "header_line",
        "true_header_line",
        "columns_preview",
        "warnings",
        "is_binary_file",
    }
    assert set(hints.keys()) == expected_keys
    assert hints["delim_suggested"] == ";"
    assert hints["header_line"] == "col1;col2"
    assert "col1" in hints["columns_preview"]


@pytest.mark.policy
def test_sniff_source_file_true_header_line_preserved(tmp_path: Path):
    """true_header_line is always read at line 0, independent of skip offset."""
    csv_path = tmp_path / "data.csv"
    csv_path.write_text(
        "Title row\ncol1;col2\n1;2\n", encoding="utf-8"
    )

    hints = sniff_source_file(csv_path)

    # true_header_line is the real line-0 header
    assert hints["true_header_line"] == "Title row"
    # header_line respects the skip offset
    assert hints["header_line"] == "col1;col2"
    assert hints["skip_suggested"] == 1


@pytest.mark.policy
def test_profile_with_read_cfg_overrides_sniff(tmp_path: Path):
    """When read_cfg is passed to profile_raw, it overrides sniff suggestions.

    The sniff phase still returns delim_suggested from the raw bytes,
    but the DuckDB profiling phase uses effective_read_cfg which has
    the user override.  The practical proof is in columns_raw.
    """
    csv_path = tmp_path / "raw" / "data.csv"
    csv_path.parent.mkdir()
    csv_path.write_text("a,b\n1,2\n", encoding="utf-8")

    # sniff would auto-detect delim=","
    # explicitly pass delim=";" → should override sniff for DuckDB read
    profile = profile_raw(
        csv_path.parent,
        "demo",
        2024,
        read_cfg={"delim": ";", "encoding": "utf-8"},
    )

    # sniff still reports what it saw
    assert profile.delim_suggested == ","
    # but DuckDB read used semicolon, so "a,b" is ONE column (no semicolons in data)
    assert len(profile.columns_raw) == 1
    assert "a,b" in profile.columns_raw


@pytest.mark.policy
def test_profile_with_read_cfg_reads_exactly_like_runtime(tmp_path: Path):
    """profile_with_read_cfg reads the file exactly as clean.read would."""
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("id;value\nA;1.234,56\nB;7.890,12\n", encoding="utf-8")

    effective_cfg = {
        "delim": ";",
        "decimal": ",",
        "encoding": "utf-8",
        "header": True,
        "skip": 0,
    }

    # sniff would detect decimal="," from the data values
    sniff_hints = sniff_source_file(csv_path)
    # Override decimal to prove profile_with_read_cfg uses effective_cfg directly
    result = profile_with_read_cfg(csv_path, sniff_hints, effective_cfg)

    assert result["columns_raw"] == ["id", "value"]
    assert result["robust_read_suggested"] is False
    # mapping_suggestions should be present
    assert "id" in result["mapping_suggestions"]


@pytest.mark.policy
def test_profile_with_read_cfg_retry_sets_robust_read_suggested(tmp_path: Path, monkeypatch):
    """When first DuckDB read fails but retry with robust preset succeeds, flag must be True.

    Regression test: before the fix, retry-success kept robust_read_suggested=False
    because the flag was initialised after the retry logic.
    """
    import toolkit.profile.raw as raw_mod

    csv_path = tmp_path / "data.csv"
    csv_path.write_text("col1;col2\n1;2\n", encoding="utf-8")

    sniff_hints = sniff_source_file(csv_path)
    effective_cfg = {
        "delim": ";",
        "encoding": "utf-8",
        "header": True,
        "skip": 0,
    }

    _original_profile_view = raw_mod._profile_view
    call_count = 0

    def failing_profile_view(con, file0, *, effective_read_cfg):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("first read fails")
        # retry succeeds — call the real function directly (not via module, to avoid loop)
        _original_profile_view(con, file0, effective_read_cfg=effective_read_cfg)

    monkeypatch.setattr(raw_mod, "_profile_view", failing_profile_view)

    result = profile_with_read_cfg(csv_path, sniff_hints, effective_cfg)

    assert call_count == 2, f"Expected exactly 2 calls (fail + retry), got {call_count}"
    assert result["robust_read_suggested"] is True, (
        "robust_read_suggested must be True after retry-success"
    )
    assert "profile_read_retry" in result["warnings"][-1]


@pytest.mark.policy
def test_profile_raw_respects_primary_file_override(tmp_path: Path):
    """When primary_file is passed, profile_raw uses it instead of glob alphabetical.

    Regression test: before the fix, multi-source datasets with different
    source files per year would get the wrong file profiled because
    _pick_data_file just returns the first alphabetical match.
    """
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()

    # Two source files: alphabetically bonus.csv comes first
    bonus = raw_dir / "bonus.csv"
    bonus.write_text("col_bonus\nval_bonus\n", encoding="utf-8")
    tipo_reddito = raw_dir / "tipo_reddito.csv"
    tipo_reddito.write_text("col_tipo_reddito\nval_tr\n", encoding="utf-8")

    # Profile with primary_file pointing to tipo_reddito
    profile = profile_raw(
        raw_dir,
        "demo",
        2024,
        primary_file=tipo_reddito,
    )

    # Must profile tipo_reddito.csv, not bonus.csv
    assert profile.file_used == "tipo_reddito.csv"
    assert profile.columns_raw == ["col_tipo_reddito"]


@pytest.mark.policy
def test_profile_raw_xlsx_produces_real_columns(tmp_path: Path):
    """XLSX files are profiled via pandas — columns_raw must contain real column names."""
    import pandas as pd

    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    xlsx_path = raw_dir / "input_2023.xlsx"
    df = pd.DataFrame({"anno": [2020, 2021], "comune": ["Roma", "Milano"], "importo": [100.0, 200.5]})
    df.to_excel(xlsx_path, index=False)

    profile = profile_raw(raw_dir, "test_ds", 2023)

    assert profile.columns_raw == ["anno", "comune", "importo"]
    assert profile.columns_norm == ["anno", "comune", "importo"]
    assert profile.warnings == []


@pytest.mark.policy
def test_profile_excel_parity_header_false_columns(tmp_path: Path):
    """_profile_excel with header=false + columns must surface the same cols as clean runtime.

    Parity with test_clean_duckdb_read.test_read_raw_to_relation_reads_xlsx_with_explicit_sheet_and_columns.
    """
    import pandas as pd

    xlsx_path = tmp_path / "sheeted.xlsx"
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        pd.DataFrame({"skipme": ["ignore"]}).to_excel(writer, sheet_name="Other", index=False)
        pd.DataFrame([["A", 1], ["B", 2]]).to_excel(
            writer, sheet_name="Export", header=False, index=False
        )

    read_cfg = {
        "header": False,
        "sheet_name": "Export",
        "columns": {"col0": "VARCHAR", "col1": "VARCHAR"},
    }

    result = _profile_excel(xlsx_path, read_cfg)

    # columns_raw must reflect what the clean runtime will actually read after applying columns map
    assert result["columns_raw"] == ["col0", "col1"]
    # No warnings — valid sheet + config
    assert result["warnings"] == []
    # Sample rows contain the data from "Export" sheet (as dicts, not raw lists)
    assert result["sample_rows"] == [{"col0": "A", "col1": 1}, {"col0": "B", "col1": 2}]


@pytest.mark.policy
def test_sniff_source_file_detects_xlsx_as_binary(tmp_path: Path):
    """sniff_source_file must detect XLSX via magic bytes and return is_binary_file."""
    import pandas as pd

    xlsx_path = tmp_path / "input.xlsx"
    df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    df.to_excel(xlsx_path, index=False)

    hints = sniff_source_file(xlsx_path)

    assert hints["is_binary_file"] == "xlsx"
    assert "binary_file_detected: xlsx" in hints["warnings"]


@pytest.mark.policy
def test_sniff_source_file_detects_xls_as_binary(tmp_path: Path):
    """sniff_source_file must detect XLS (OLE2) via magic bytes and return is_binary_file."""
    # Minimal OLE2 file header (D0 CF 11 E0) — just enough to trigger magic detection.
    xls_path = tmp_path / "input.xls"
    xls_path.write_bytes(b"\xd0\xcf\x11\xe0\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")

    hints = sniff_source_file(xls_path)

    assert hints["is_binary_file"] == "xls"
    assert "binary_file_detected: xls" in hints["warnings"]
