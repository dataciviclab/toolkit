"""Tests for toolkit/mcp/raw_ops.py — describe, query, path resolution, SQL validation."""

from __future__ import annotations

import pytest

from toolkit.mcp.raw_ops import _normalize_path, _validate_select_sql


# ---------------------------------------------------------------------------
# _normalize_path
# ---------------------------------------------------------------------------

class TestNormalizePath:
    def test_gs_path(self) -> None:
        """gs://bucket/key → HTTPS URL."""
        url, is_local = _normalize_path("gs://dataciviclab-clean/terna/2024/data.parquet")
        assert url.startswith("https://storage.googleapis.com/")
        assert "dataciviclab-clean" in url
        assert "terna/2024/data.parquet" in url
        assert is_local is False

    def test_gs_path_no_key_raises(self) -> None:
        """gs://bucket senza key → errore."""
        with pytest.raises(ValueError, match="path GCS non valido"):
            _normalize_path("gs://bucket")

    def test_https_path(self) -> None:
        """URL HTTPS viene passato cosi' com'e'."""
        url, is_local = _normalize_path("https://example.test/data.csv")
        assert url == "https://example.test/data.csv"
        assert is_local is False

    def test_http_path(self) -> None:
        """URL HTTP viene passato cosi' com'e'."""
        url, is_local = _normalize_path("http://example.test/data.csv")
        assert url == "http://example.test/data.csv"
        assert is_local is False

    def test_local_absolute_path(self) -> None:
        """Path assoluto passato com'e', non risolto contro CWD."""
        url, is_local = _normalize_path("/dev/null")
        assert url == "/dev/null"
        assert is_local is True

    def test_local_path_not_found_raises(self) -> None:
        """Path inesistente → errore."""
        with pytest.raises(ValueError, match="File non trovato"):
            _normalize_path("/tmp/non-esiste-12345.csv")

    def test_empty_path_raises(self) -> None:
        with pytest.raises(ValueError, match="file_path vuoto"):
            _normalize_path("")


# ---------------------------------------------------------------------------
# _validate_select_sql
# ---------------------------------------------------------------------------

class TestValidateSelectSql:
    def test_select_allowed(self) -> None:
        """SELECT semplice passa."""
        result = _validate_select_sql("SELECT * FROM read_parquet('file.parquet')")
        assert "SELECT" in result

    def test_with_allowed(self) -> None:
        """WITH passa."""
        result = _validate_select_sql("WITH t AS (SELECT 1 AS x) SELECT * FROM t")
        assert "WITH" in result

    def test_drop_blocked(self) -> None:
        """DROP bloccato dal gate SELECT (prima del keyword check)."""
        with pytest.raises(ValueError, match="SELECT o WITH"):
            _validate_select_sql("DROP TABLE x")

    def test_insert_blocked(self) -> None:
        """INSERT bloccato dal gate SELECT."""
        with pytest.raises(ValueError, match="SELECT o WITH"):
            _validate_select_sql("INSERT INTO t VALUES (1)")

    def test_delete_blocked(self) -> None:
        """DELETE bloccato dal gate SELECT."""
        with pytest.raises(ValueError, match="SELECT o WITH"):
            _validate_select_sql("DELETE FROM t")

    def test_create_blocked(self) -> None:
        """CREATE bloccato dal gate SELECT."""
        with pytest.raises(ValueError, match="SELECT o WITH"):
            _validate_select_sql("CREATE TABLE t (x INT)")

    def test_update_blocked(self) -> None:
        """UPDATE bloccato dal gate SELECT."""
        with pytest.raises(ValueError, match="SELECT o WITH"):
            _validate_select_sql("UPDATE t SET x = 1")

    def test_alter_blocked(self) -> None:
        """ALTER bloccato dal gate SELECT."""
        with pytest.raises(ValueError, match="SELECT o WITH"):
            _validate_select_sql("ALTER TABLE t ADD COLUMN x INT")

    def test_multiple_statements_blocked(self) -> None:
        """Piu' query separate da ; bloccate."""
        with pytest.raises(ValueError, match="non consentiti"):
            _validate_select_sql("SELECT 1; SELECT 2")

    def test_empty_sql_raises(self) -> None:
        with pytest.raises(ValueError, match="sql vuoto"):
            _validate_select_sql("")

    def test_keyword_in_string_literal_not_blocked(self) -> None:
        """Keyword dentro un letterale non viene bloccata."""
        result = _validate_select_sql("SELECT * FROM t WHERE name = 'drop table'")
        assert "drop table" in result

    def test_keyword_in_comment_not_blocked(self) -> None:
        """Keyword dentro un commento -- non viene bloccata."""
        result = _validate_select_sql("SELECT 1 -- drop table")
        assert result
