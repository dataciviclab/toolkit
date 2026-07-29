"""Test: parser macro SQL (toolkit/core/macro_reader.py).

Verifica che il parser estragga correttamente nome, parametri, descrizione,
e annotazioni @contract da macros.sql.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.contract


def test_read_macros_returns_all() -> None:
    """read_macros restituisce tutte le 8 macro standard."""
    from toolkit.core.macro_reader import read_macros

    macros = read_macros()
    names = {m["name"] for m in macros}
    expected = {
        "normalize_italian_number",
        "normalize_italian_integer",
        "normalize_string",
        "cast_int",
        "cast_bigint",
        "cast_double",
        "decode_flag",
        "remove_dot_thousands",
    }
    assert names == expected, (
        f"Macro lette: {names - expected} in piu', {expected - names} mancanti"
    )


def test_read_macros_normalize_italian_contract() -> None:
    """normalize_italian_number ha warning e returns da annotazione."""
    from toolkit.core.macro_reader import read_macros

    macros = read_macros()
    m = next(x for x in macros if x["name"] == "normalize_italian_number")

    assert m["returns"] == "DOUBLE"
    assert "warning" in m
    assert "read.decimal" in m["warning"]
    assert "see" in m
    assert "remove_dot_thousands" in m["see"]


def test_read_macros_signature_and_params() -> None:
    """Firma e parametri sono estratti correttamente."""
    from toolkit.core.macro_reader import read_macros

    macros = read_macros()
    by_name = {m["name"]: m for m in macros}

    # Senza parametri
    # (tutte le macro attuali hanno almeno un parametro)

    # Con un parametro
    assert by_name["cast_int"]["params"] == ["val"]
    assert by_name["cast_int"]["signature"] == "cast_int(val)"

    # Con due parametri
    assert by_name["decode_flag"]["params"] == ["val", "yes_value"]
    assert by_name["decode_flag"]["signature"] == "decode_flag(val, yes_value)"


def test_read_macros_description_non_empty() -> None:
    """Ogni macro ha una descrizione leggibile."""
    from toolkit.core.macro_reader import read_macros

    for m in read_macros():
        assert len(m["description"]) > 10, f"Macro '{m['name']}' ha descrizione troppo corta"


def test_read_macros_example_present() -> None:
    """Ogni macro ha un example (da @contract o generato)."""
    from toolkit.core.macro_reader import read_macros

    for m in read_macros():
        assert m["example"], f"Macro '{m['name']}' non ha example"


def test_read_macros_return_types() -> None:
    """Tutte le macro hanno returns type esplicito (non default VARCHAR).

    normalize_string e' l'unica eccezione: restituisce VARCHAR per natura.
    """
    from toolkit.core.macro_reader import read_macros

    for m in read_macros():
        # VARCHAR e' il default del parser - deve essere esplicito solo per normalize_string
        if m["name"] == "normalize_string":
            assert m["returns"] == "VARCHAR"
        else:
            assert m["returns"] != "VARCHAR", (
                f"Macro '{m['name']}' manca @contract returns: in macros.sql "
                f"(ha '{m['returns']}' di default)"
            )


def test_macro_names_matches() -> None:
    """macro_names e read_macros sono coerenti."""
    from toolkit.core.macro_reader import read_macros, macro_names

    assert macro_names() == {m["name"] for m in read_macros()}


def test_read_macros_file_not_found(tmp_path: Path) -> None:
    """File mancante -> lista vuota (non blocca il contratto all'import)."""
    from toolkit.core.macro_reader import read_macros

    result = read_macros(tmp_path / "nonexistent.sql")
    assert result == []
