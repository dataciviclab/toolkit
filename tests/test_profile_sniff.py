from pathlib import Path

import toolkit.profile.raw as profile_raw_module
from toolkit.cli.cmd_profile import write_suggested_read_yml
from toolkit.profile.raw import (
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
