"""Tests for toolkit/core/sql_utils.py."""

from __future__ import annotations

from pathlib import Path

import pytest

from toolkit.core.sql_utils import q_ident, quote_list, sql_literal, sql_path, sql_str

pytestmark = pytest.mark.pure_unit


class TestQIdent:
    def test_simple_name(self) -> None:
        assert q_ident("foo") == '"foo"'

    def test_name_with_space(self) -> None:
        assert q_ident("foo bar") == '"foo bar"'

    def test_name_with_double_quote(self) -> None:
        assert q_ident('foo"bar') == '"foo""bar"'

    def test_empty_string(self) -> None:
        assert q_ident("") == '""'

    def test_reserved_word(self) -> None:
        assert q_ident("select") == '"select"'

    def test_unicode(self) -> None:
        assert q_ident("café") == '"café"'

    def test_name_with_backslash(self) -> None:
        # Backslash is not special for DuckDB identifiers — no escaping needed
        assert q_ident("foo\bar") == '"foo\bar"'

    def test_multiple_double_quotes(self) -> None:
        assert q_ident('a"b"c') == '"a""b""c"'

    def test_returns_string(self) -> None:
        result = q_ident("test")
        assert isinstance(result, str)

    def test_always_starts_and_ends_with_double_quote(self) -> None:
        result = q_ident("anything")
        assert result.startswith('"')
        assert result.endswith('"')


class TestSqlPath:
    def test_absolute_path_to_sql_literal(self) -> None:
        p = Path("/tmp/test/file.csv")
        result = sql_path(p)
        expected = p.resolve().as_posix()
        assert result == expected

    def test_quotes_single_quote_in_path(self) -> None:
        p = Path("/tmp/test/it's.csv")
        result = sql_path(p)
        assert "'" not in result.replace("''", "")
        assert "''" in result


class TestSqlLiteral:
    """Contract: sql_literal is the single canonical SQL string escaper."""

    def test_plain_string(self) -> None:
        assert sql_literal("hello") == "hello"

    def test_single_quote(self) -> None:
        assert sql_literal("it's") == "it''s"

    def test_multiple_quotes(self) -> None:
        assert sql_literal("a'b'c") == "a''b''c"

    def test_double_quotes_untouched(self) -> None:
        assert sql_literal('a"b"c') == 'a"b"c'

    def test_empty_string(self) -> None:
        assert sql_literal("") == ""

    def test_backslash_not_escaped(self) -> None:
        assert sql_literal("a\\b") == "a\\b"

    def test_only_quotes(self) -> None:
        assert sql_literal("'''") == "''''''"

    def test_already_escaped(self) -> None:
        # Single quotes are always escaped — no "already escaped" concept in SQL
        assert sql_literal("a''b") == "a''''b"

    def test_returns_string(self) -> None:
        assert isinstance(sql_literal("x"), str)


class TestSqlStr:
    """Contract: sql_str converte oggetti in string SQL-safe (delega a sql_literal)."""

    def test_string_value(self) -> None:
        assert sql_str("hello") == "hello"

    def test_string_with_quote(self) -> None:
        assert sql_str("it's") == "it''s"

    def test_int_value(self) -> None:
        assert sql_str(42) == "42"

    def test_float_value(self) -> None:
        assert sql_str(3.14) == "3.14"

    def test_none_becomes_string_none(self) -> None:
        assert sql_str(None) == "None"

    def test_delegates_to_literal(self) -> None:
        assert sql_str("a'b") == sql_literal("a'b")

    def test_backward_compat_via_csv_read(self) -> None:
        from toolkit.core.csv_read import sql_str as csv_sql_str
        assert csv_sql_str("test") == sql_str("test")

    def test_returns_string(self) -> None:
        assert isinstance(sql_str(42), str)


class TestQuoteList:
    def test_single_path(self) -> None:
        p = Path("/tmp/a.csv")
        result = quote_list([p])
        expected = f"'{p.resolve().as_posix()}'"
        assert result == expected

    def test_multiple_paths(self) -> None:
        p1 = Path("/tmp/a.csv")
        p2 = Path("/tmp/b.csv")
        result = quote_list([p1, p2])
        assert result.startswith("'")
        assert result.endswith("'")
        assert p1.resolve().as_posix() in result
        assert p2.resolve().as_posix() in result

    def test_empty_list(self) -> None:
        assert quote_list([]) == ""
