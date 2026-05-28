"""Tests for toolkit/core/sql_utils.py."""

from pathlib import Path

from toolkit.core.sql_utils import q_ident, quote_list, sql_path


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
