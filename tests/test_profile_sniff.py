from pathlib import Path

import toolkit.profile.raw as profile_raw_module
from toolkit.cli.cmd_profile import write_suggested_read_yml
from toolkit.profile._column_profile import _build_mapping_suggestions
from toolkit.profile.raw import (
    _build_read_csv_opts,
    build_suggested_read_cfg,
    profile_raw,
    sniff_delim,
    sniff_decimal,
    sniff_encoding,
    suggest_skip,
)


def test_sniff_delim_tab():
    sample = "a\tb\tc\n1\t2\t3\n4\t5\t6\n"
    assert sniff_delim(sample) == "\t"


def test_sniff_decimal_it_comma():
    sample = "val\n1.234,56\n7.890,12\n"
    assert sniff_decimal(sample) == ","


def test_sniff_encoding_latin1(tmp_path: Path):
    p = tmp_path / "latin1.csv"
    p.write_bytes("nome\ncittà\n".encode("latin-1"))
    enc, txt = sniff_encoding(p)
    assert enc.lower() in {"latin-1", "windows-1252", "cp1252"}
    assert "citt" in txt.lower()


def test_suggest_skip_for_preamble_line():
    sample = (
        "Produzione e raccolta differenziata su scala comunale anno 2022 (ISPRA)\n"
        "IstatComune;Regione;Provincia;Comune\n"
        "01001001;Piemonte;Torino;AGLIE'\n"
    )
    assert suggest_skip(sample, ";") == 1


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
