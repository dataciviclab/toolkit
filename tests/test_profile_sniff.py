from pathlib import Path

from toolkit.profile.raw import sniff_delim, sniff_decimal, sniff_encoding


def test_sniff_delim_tab():
    sample = "a\tb\tc\n1\t2\t3\n4\t5\t6\n"
    assert sniff_delim(sample) == "\t"


def test_sniff_decimal_it_comma():
    sample = "val\n1.234,56\n7.890,12\n"
    assert sniff_decimal(sample) == ","


def test_sniff_encoding_latin1(tmp_path: Path):
    p = tmp_path / "latin1.csv"
    # "città" in latin-1 contiene byte non utf-8 validi
    p.write_bytes("nome\ncittà\n".encode("latin-1"))
    enc, txt = sniff_encoding(p)
    assert enc.lower() in {"latin-1", "windows-1252", "cp1252"}
    assert "citt" in txt.lower()