"""Tests for toolkit/core/sql_utils.py."""

from toolkit.core.sql_utils import q_ident


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
