"""Tests for the standard DuckDB macros in ``macros.sql``.

Le macro sono caricate automaticamente dal layer CLEAN. Questi test
verificano il comportamento di ogni macro direttamente via DuckDB,
usando lo stesso meccanismo di caricamento del runtime.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lab_connectors.duckdb import safe_connect

_MACROS_PATH = Path(__file__).parent.parent / "toolkit" / "sql" / "macros.sql"
_MACROS_SQL = _MACROS_PATH.read_text(encoding="utf-8")

pytestmark = pytest.mark.pure_unit


# ===========================================================================
# normalize_italian_number
# ===========================================================================


class TestNormalizeItalianNumber:
    """Contratto: converte numero formato italiano (1.234,56 → 1234.56)."""

    def test_simple_integer(self) -> None:
        with safe_connect() as con:
            con.execute(_MACROS_SQL)
            result = con.execute("SELECT normalize_italian_number('1234')").fetchone()
        assert result[0] == 1234.0

    def test_thousands_and_comma(self) -> None:
        with safe_connect() as con:
            con.execute(_MACROS_SQL)
            result = con.execute("SELECT normalize_italian_number('1.234,56')").fetchone()
        assert result[0] == 1234.56

    def test_comma_decimal_only(self) -> None:
        with safe_connect() as con:
            con.execute(_MACROS_SQL)
            result = con.execute("SELECT normalize_italian_number('1234,56')").fetchone()
        assert result[0] == 1234.56

    def test_large_thousands(self) -> None:
        with safe_connect() as con:
            con.execute(_MACROS_SQL)
            result = con.execute("SELECT normalize_italian_number('12.345.678')").fetchone()
        assert result[0] == 12345678.0

    def test_null_returns_null(self) -> None:
        with safe_connect() as con:
            con.execute(_MACROS_SQL)
            result = con.execute("SELECT normalize_italian_number(NULL)").fetchone()
        assert result[0] is None

    def test_non_numeric_returns_null(self) -> None:
        with safe_connect() as con:
            con.execute(_MACROS_SQL)
            result = con.execute("SELECT normalize_italian_number('abc')").fetchone()
        assert result[0] is None

    def test_empty_string_returns_null(self) -> None:
        with safe_connect() as con:
            con.execute(_MACROS_SQL)
            result = con.execute("SELECT normalize_italian_number('')").fetchone()
        assert result[0] is None


# ===========================================================================
# normalize_italian_integer
# ===========================================================================


class TestNormalizeItalianInteger:
    """Contratto: come normalize_italian_number ma restituisce INTEGER."""

    def test_simple_integer(self) -> None:
        with safe_connect() as con:
            con.execute(_MACROS_SQL)
            result = con.execute("SELECT normalize_italian_integer('1234')").fetchone()
        assert result[0] == 1234

    def test_with_thousands(self) -> None:
        with safe_connect() as con:
            con.execute(_MACROS_SQL)
            result = con.execute("SELECT normalize_italian_integer('1.234')").fetchone()
        assert result[0] == 1234

    def test_comma_truncates(self) -> None:
        with safe_connect() as con:
            con.execute(_MACROS_SQL)
            result = con.execute("SELECT normalize_italian_integer('5.432,10')").fetchone()
        assert result[0] == 5432

    def test_null_returns_null(self) -> None:
        with safe_connect() as con:
            con.execute(_MACROS_SQL)
            result = con.execute("SELECT normalize_italian_integer(NULL)").fetchone()
        assert result[0] is None


# ===========================================================================
# decode_flag
# ===========================================================================


class TestDecodeFlag:
    """Contratto: decode_flag(val, yes_value) → BOOLEAN."""

    def test_yes_matches(self) -> None:
        with safe_connect() as con:
            con.execute(_MACROS_SQL)
            result = con.execute("SELECT decode_flag('X', 'X')").fetchone()
        assert result[0] is True

    def test_no_mismatch(self) -> None:
        with safe_connect() as con:
            con.execute(_MACROS_SQL)
            result = con.execute("SELECT decode_flag('', 'X')").fetchone()
        assert result[0] is False

    def test_different_yes_value(self) -> None:
        with safe_connect() as con:
            con.execute(_MACROS_SQL)
            result = con.execute("SELECT decode_flag('S', 'S')").fetchone()
        assert result[0] is True

    def test_case_sensitive_default(self) -> None:
        with safe_connect() as con:
            con.execute(_MACROS_SQL)
            result = con.execute("SELECT decode_flag('x', 'X')").fetchone()
        assert result[0] is False

    def test_trim_applied(self) -> None:
        with safe_connect() as con:
            con.execute(_MACROS_SQL)
            result = con.execute("SELECT decode_flag(' X ', 'X')").fetchone()
        assert result[0] is True

    def test_null_val_returns_false(self) -> None:
        with safe_connect() as con:
            con.execute(_MACROS_SQL)
            result = con.execute("SELECT decode_flag(NULL, 'X')").fetchone()
        assert result[0] is False


# ===========================================================================
# normalize_string
# ===========================================================================


class TestNormalizeString:
    """Contratto: TRIM + stringa vuota → NULL."""

    def test_trim(self) -> None:
        with safe_connect() as con:
            con.execute(_MACROS_SQL)
            result = con.execute("SELECT normalize_string('  hello  ')").fetchone()
        assert result[0] == "hello"

    def test_empty_returns_null(self) -> None:
        with safe_connect() as con:
            con.execute(_MACROS_SQL)
            result = con.execute("SELECT normalize_string('')").fetchone()
        assert result[0] is None

    def test_whitespace_only_returns_null(self) -> None:
        with safe_connect() as con:
            con.execute(_MACROS_SQL)
            result = con.execute("SELECT normalize_string('   ')").fetchone()
        assert result[0] is None

    def test_null_returns_null(self) -> None:
        with safe_connect() as con:
            con.execute(_MACROS_SQL)
            result = con.execute("SELECT normalize_string(NULL)").fetchone()
        assert result[0] is None


# ===========================================================================
# cast_int
# ===========================================================================


class TestCastInt:
    """Contratto: TRY_CAST(val AS INTEGER)."""

    def test_integer_string(self) -> None:
        with safe_connect() as con:
            con.execute(_MACROS_SQL)
            result = con.execute("SELECT cast_int('123')").fetchone()
        assert result[0] == 123

    def test_integer_value(self) -> None:
        with safe_connect() as con:
            con.execute(_MACROS_SQL)
            result = con.execute("SELECT cast_int(456)").fetchone()
        assert result[0] == 456

    def test_non_numeric_returns_null(self) -> None:
        with safe_connect() as con:
            con.execute(_MACROS_SQL)
            result = con.execute("SELECT cast_int('abc')").fetchone()
        assert result[0] is None

    def test_null_returns_null(self) -> None:
        with safe_connect() as con:
            con.execute(_MACROS_SQL)
            result = con.execute("SELECT cast_int(NULL)").fetchone()
        assert result[0] is None


# ===========================================================================
# cast_double
# ===========================================================================


class TestCastDouble:
    """Contratto: TRY_CAST(val AS DOUBLE)."""

    def test_integer_string(self) -> None:
        with safe_connect() as con:
            con.execute(_MACROS_SQL)
            result = con.execute("SELECT cast_double('123')").fetchone()
        assert result[0] == 123.0

    def test_decimal_string(self) -> None:
        with safe_connect() as con:
            con.execute(_MACROS_SQL)
            result = con.execute("SELECT cast_double('123.45')").fetchone()
        assert result[0] == 123.45

    def test_non_numeric_returns_null(self) -> None:
        with safe_connect() as con:
            con.execute(_MACROS_SQL)
            result = con.execute("SELECT cast_double('abc')").fetchone()
        assert result[0] is None

    def test_null_returns_null(self) -> None:
        with safe_connect() as con:
            con.execute(_MACROS_SQL)
            result = con.execute("SELECT cast_double(NULL)").fetchone()
        assert result[0] is None


# ===========================================================================
# remove_dot_thousands
# ===========================================================================


class TestRemoveDotThousands:
    """Contratto: rimuove punti migliaia senza toccare decimali standard."""

    def test_simple_thousands(self) -> None:
        with safe_connect() as con:
            con.execute(_MACROS_SQL)
            result = con.execute("SELECT remove_dot_thousands('1.234')").fetchone()
        assert result[0] == 1234.0

    def test_multiple_thousands(self) -> None:
        with safe_connect() as con:
            con.execute(_MACROS_SQL)
            result = con.execute("SELECT remove_dot_thousands('1.234.567')").fetchone()
        assert result[0] == 1234567.0

    def test_with_italian_decimal_returns_wrong(self) -> None:
        """Numeri con virgola decimale vanno normalizzati con
        normalize_italian_number, non remove_dot_thousands."""
        with safe_connect() as con:
            con.execute(_MACROS_SQL)
            result = con.execute("SELECT normalize_italian_number('1.234,56')").fetchone()
        assert result[0] == 1234.56

    def test_plain_integer(self) -> None:
        with safe_connect() as con:
            con.execute(_MACROS_SQL)
            result = con.execute("SELECT remove_dot_thousands('1234')").fetchone()
        assert result[0] == 1234.0

    def test_null_returns_null(self) -> None:
        with safe_connect() as con:
            con.execute(_MACROS_SQL)
            result = con.execute("SELECT remove_dot_thousands(NULL)").fetchone()
        assert result[0] is None


# ===========================================================================
# Integration: macro usate in una query realistica
# ===========================================================================


class TestMacroIntegration:
    """Contratto: le macro funzionano insieme in una query realistica."""

    def test_clean_sql_pattern(self) -> None:
        """Simula un clean.sql che usa più macro insieme."""
        with safe_connect() as con:
            con.execute(_MACROS_SQL)
            # Simula la vista raw_input con dati di esempio
            con.execute("""
                CREATE TABLE raw_input AS SELECT * FROM (VALUES
                    ('1', '  Mario  ', 'X', '1.234,56'),
                    ('2', '  Luisa  ', '', '9.876,50'),
                    ('3', '  Carla  ', NULL, NULL)
                ) AS t(prog, nome, flag, importo)
            """)
            result = con.execute("""
                SELECT
                    cast_int(prog) AS progressivo,
                    normalize_string(nome) AS nome,
                    decode_flag(flag, 'X') AS flag_attivo,
                    normalize_italian_number(importo) AS importo
                FROM raw_input
                ORDER BY progressivo
            """).fetchall()
        assert len(result) == 3
        assert result[0] == (1, "Mario", True, 1234.56)
        assert result[1] == (2, "Luisa", False, 9876.50)
        assert result[2] == (3, "Carla", False, None)
