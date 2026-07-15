"""Tests for toolkit/core/normalize.py.

Copre:
- normalize_number (numeri formato italiano, valori speciali, strip_dot_zero)
- normalize_columns_map (mappatura colonne con regex)
- decode_bytes (encoding tentativi, fallback)
- decode_csv_bytes (wrapping)
"""

from __future__ import annotations

import re

import pytest

from toolkit.core.normalize import (
    decode_bytes,
    decode_csv_bytes,
    normalize_columns_map,
    normalize_number,
)

pytestmark = pytest.mark.pure_unit


# ===========================================================================
# normalize_number
# ===========================================================================


class TestNormalizeNumber:
    """Contratto: normalize_number normalizza numeri in formato italiano."""

    # ── Casi base ──────────────────────────────────────────────────────

    def test_simple_integer(self) -> None:
        assert normalize_number("1234") == "1234"

    def test_thousands_separator(self) -> None:
        assert normalize_number("1.234") == "1234"

    def test_comma_decimal(self) -> None:
        assert normalize_number("1234,56") == "1234.56"

    def test_thousands_and_comma(self) -> None:
        assert normalize_number("1.234,56") == "1234.56"

    def test_large_number(self) -> None:
        assert normalize_number("12.345.678") == "12345678"

    def test_decimal_with_dot_zero_stripped(self) -> None:
        """5849,00 → 5849 (strip_dot_zero=True di default)."""
        assert normalize_number("5849,00") == "5849"

    def test_decimal_with_nonzero_preserved(self) -> None:
        assert normalize_number("1234,50") == "1234.50"

    # ── strip_dot_zero=False ──────────────────────────────────────────

    def test_strip_dot_zero_false_with_comma(self) -> None:
        assert normalize_number("5849,00", strip_dot_zero=False) == "5849.00"

    def test_strip_dot_zero_false_integer(self) -> None:
        assert normalize_number("1234", strip_dot_zero=False) == "1234"

    # ── Valori speciali → None ────────────────────────────────────────

    @pytest.mark.parametrize(
        "val",
        [
            "***",
            "-",
            "N.D.",
            "",
            "  ",
        ],
    )
    def test_empty_values_return_none(self, val: str) -> None:
        assert normalize_number(val) is None

    def test_quoted_value_stripped(self) -> None:
        assert normalize_number('"1.234,56"') == "1234.56"

    def test_quoted_empty_returns_none(self) -> None:
        assert normalize_number('""') is None

    # ── custom empty_values ───────────────────────────────────────────

    def test_custom_empty_values(self) -> None:
        assert normalize_number("X", empty_values=frozenset({"X"})) is None

    def test_custom_empty_values_no_match(self) -> None:
        """Valore speciale non negli empty_values -> None perche' non e' un numero."""
        assert normalize_number("N.D.", empty_values=frozenset({"X"})) is None

    def test_custom_empty_values_with_real_number(self) -> None:
        """Valore numerico con empty_values custom funziona comunque."""
        assert normalize_number("1234", empty_values=frozenset({"X"})) == "1234"

    # ── Non-numeric after cleanup ─────────────────────────────────────

    def test_non_numeric_string_returns_none(self) -> None:
        """Stringa che dopo rimozione punti contiene lettere -> None."""
        assert normalize_number("N.D.") is None
        assert normalize_number("TEST") is None
        assert normalize_number("1a234") is None

    def test_mixed_string_returns_none(self) -> None:
        assert normalize_number("1.234a") is None

    # ── Edge cases ────────────────────────────────────────────────────

    def test_non_string_returns_none(self) -> None:
        assert normalize_number(1234) is None  # type: ignore[arg-type]

    def test_negative_number(self) -> None:
        """Numeri negativi non sono gestiti esplicitamente ma passano."""
        assert normalize_number("-1234") == "-1234"

    def test_negative_with_thousands(self) -> None:
        assert normalize_number("-1.234,56") == "-1234.56"


# ===========================================================================
# normalize_columns_map
# ===========================================================================


class TestNormalizeColumnsMap:
    """Contratto: normalize_columns_map applica regex map e restituisce
    nomi normalizzati + indici originali."""

    COL_MAP: list[tuple[re.Pattern, str]] = [
        (re.compile(r"^REG(IONE)?$", re.I), "regione"),
        (re.compile(r"^PROV(INCIA)?$", re.I), "provincia"),
        (re.compile(r"^COMUNE$", re.I), "comune"),
        (re.compile(r"^(ELETTORI(TOT)?)$", re.I), "elettori"),
        (re.compile(r"^(VOTI_LISTA|VOTILISTA)$", re.I), "voti_lista"),
    ]

    def test_basic_mapping(self) -> None:
        header = ["REGIONE", "PROV", "COMUNE", "SKIP"]
        names, indices = normalize_columns_map(header, self.COL_MAP)
        assert names == ["regione", "provincia", "comune"]
        assert indices == [0, 1, 2]

    def test_regex_variants(self) -> None:
        header = ["REG", "PROVINCIA", "COMUNE"]
        names, indices = normalize_columns_map(header, self.COL_MAP)
        assert names == ["regione", "provincia", "comune"]
        assert indices == [0, 1, 2]

    def test_voti_variants(self) -> None:
        header = ["VOTI_LISTA", "VOTILISTA"]
        names, _ = normalize_columns_map(header, self.COL_MAP)
        assert names == ["voti_lista", "voti_lista"]

    def test_no_match_returns_empty(self) -> None:
        names, indices = normalize_columns_map(["SKIP1", "SKIP2"], self.COL_MAP)
        assert names == []
        assert indices == []

    def test_empty_header(self) -> None:
        names, indices = normalize_columns_map([], self.COL_MAP)
        assert names == []
        assert indices == []

    def test_empty_col_map(self) -> None:
        header = ["A", "B"]
        names, indices = normalize_columns_map(header, [])
        assert names == []
        assert indices == []

    def test_strips_whitespace_and_quotes(self) -> None:
        header = ['"REGIONE"', "  PROV  "]
        names, indices = normalize_columns_map(header, self.COL_MAP)
        assert names == ["regione", "provincia"]
        assert indices == [0, 1]

    def test_first_pattern_wins(self) -> None:
        """L'ordine di col_map determina la priorita'."""
        dup_map = [
            (re.compile(r"^A$"), "prima"),
            (re.compile(r"^A$"), "seconda"),  # non matcha mai
        ]
        names, indices = normalize_columns_map(["A"], dup_map)
        assert names == ["prima"]
        assert indices == [0]

    def test_case_insensitive(self) -> None:
        names, indices = normalize_columns_map(["comune", "Comune", "COMUNE"], self.COL_MAP)
        assert names == ["comune", "comune", "comune"]
        assert indices == [0, 1, 2]

    def test_real_elezioni_pattern(self) -> None:
        """Test con COL_MAP reale da elezioni-regionali."""
        real_map = [
            (re.compile(r"^REG(IONE)?$", re.I), "regione"),
            (re.compile(r"^CIRC(OSCR(IZIONE)?)?$", re.I), "circoscrizione"),
            (re.compile(r"^PROV(INCIA)?$", re.I), "provincia"),
            (re.compile(r"^COMUNE$", re.I), "comune"),
            (re.compile(r"^(ELETTORI(TOT)?)$", re.I), "elettori"),
            (re.compile(r"^(VOTANTI(TOT)?)$", re.I), "votanti"),
            (re.compile(r"^(SCHEDE_BIANCHE|SKBIANCHE)$", re.I), "schede_bianche"),
            (re.compile(r"^(COGNOME|COGNOME_CANDIDATO)$", re.I), "cognome"),
            (re.compile(r"^(NOME|NOME_CANDIDATO)$", re.I), "nome"),
            (re.compile(r"^(LISTA|DESCRLISTA)$", re.I), "lista"),
            (re.compile(r"^(VOTI_LISTA|VOTILISTA)$", re.I), "voti_lista"),
            (re.compile(r"^(VOTI_CAND(IDATO)?|VOTICAND(IDATO)?)$", re.I), "voti_candidato"),
        ]
        header = [
            "REGIONE",
            "PROVINCIA",
            "COMUNE",
            "ELETTORI",
            "VOTANTI",
            "SCHEDE_BIANCHE",
            "COGNOME_CANDIDATO",
            "NOME_CANDIDATO",
            "LISTA",
            "VOTI_LISTA",
            "DATA",
        ]
        names, indices = normalize_columns_map(header, real_map)
        assert len(names) == 10
        assert names[0] == "regione"
        assert names[-1] == "voti_lista"
        assert "DATA" not in names
        assert indices == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


# ===========================================================================
# decode_bytes
# ===========================================================================


class TestDecodeBytes:
    """Contratto: decode_bytes tenta encoding in ordine, fallback utf-8 replace."""

    def test_utf8(self) -> None:
        assert decode_bytes(b"hello") == "hello"

    def test_utf8_sig(self) -> None:
        """BOM UTF-8 viene rimosso da utf-8-sig."""
        data = "\ufeffCIAO".encode("utf-8")
        assert decode_bytes(data) == "CIAO"

    def test_latin1_fallback(self) -> None:
        data = "café".encode("latin-1")
        # utf-8 decodifica latin-1 senza errori (latin-1 e' un subset di utf-8
        # per i primi 128, ma café ha byte > 127 che utf-8 non accetta)
        result = decode_bytes(data)
        assert result == "café"

    def test_cp1252(self) -> None:
        """Caratteri speciali cp1252 (0x80-0x9F) non validi in utf-8 né latin-1."""
        # EURO SIGN € in cp1252 = 0x80
        data = bytes([0x80])
        result = decode_bytes(data)
        # cp1252 e' l'ultimo tentativo prima del fallback
        assert result == "€"

    def test_iso_8859_1_sequence(self) -> None:
        data = "café".encode("iso-8859-1")
        result = decode_bytes(data)
        assert result == "café"

    def test_fallback_replace(self) -> None:
        """Bytes che falliscono tutti gli encoding → utf-8 replace."""
        data = b"\xff\xfe\x00\x01"
        result = decode_bytes(data)
        assert isinstance(result, str)
        assert len(result) > 0  # replace produce caratteri

    def test_custom_encodings(self) -> None:
        data = "hello".encode("ascii")
        result = decode_bytes(data, encodings=("ascii",))
        assert result == "hello"

    def test_custom_encodings_fallback(self) -> None:
        data = b"\xff"
        result = decode_bytes(data, encodings=("ascii",))
        # ascii fallisce → fallback utf-8 replace
        assert isinstance(result, str)


# ===========================================================================
# decode_csv_bytes
# ===========================================================================


class TestDecodeCsvBytes:
    """Contratto: decode_csv_bytes e' un wrapper di decode_bytes."""

    def test_delegates_to_decode_bytes(self) -> None:
        data = "a,b,c\n1,2,3".encode("utf-8")
        assert decode_csv_bytes(data) == decode_bytes(data)

    def test_latin1_csv(self) -> None:
        data = "nome,valore\ncaffè,123".encode("latin-1")
        result = decode_csv_bytes(data)
        assert "caffè" in result

    def test_utf8_sig_csv(self) -> None:
        data = "\ufeffnome,valore\n1,2".encode("utf-8")
        result = decode_csv_bytes(data)
        assert result == "nome,valore\n1,2"  # BOM stripped
