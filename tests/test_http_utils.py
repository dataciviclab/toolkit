"""Tests for toolkit.plugins._http_utils — contratto condiviso di troncamento.

Protegge il contratto pubblico del modulo: NON_TRUNCABLE_EXTS,
is_non_truncable_url, truncate_at_line. Se questo modulo viene modificato,
questi test garantiscono che i consumer (http_file, http_post_file, ckan)
continuino a funzionare.
"""

from __future__ import annotations

import pytest

from toolkit.plugins._http_utils import (
    NON_TRUNCABLE_EXTS,
    is_non_truncable_url,
    truncate_at_line,
)

pytestmark = pytest.mark.pure_unit


class TestNonTruncableExts:
    """Contract: le estensioni non troncabili sono quelle note."""

    def test_parquet_is_non_truncable(self) -> None:
        assert ".parquet" in NON_TRUNCABLE_EXTS

    def test_zip_is_non_truncable(self) -> None:
        assert ".zip" in NON_TRUNCABLE_EXTS

    def test_xlsx_is_non_truncable(self) -> None:
        assert ".xlsx" in NON_TRUNCABLE_EXTS

    def test_xls_is_non_truncable(self) -> None:
        assert ".xls" in NON_TRUNCABLE_EXTS

    def test_gz_is_non_truncable(self) -> None:
        assert ".gz" in NON_TRUNCABLE_EXTS

    def test_bz2_is_non_truncable(self) -> None:
        assert ".bz2" in NON_TRUNCABLE_EXTS

    def test_csv_is_truncable(self) -> None:
        assert ".csv" not in NON_TRUNCABLE_EXTS

    def test_json_is_truncable(self) -> None:
        assert ".json" not in NON_TRUNCABLE_EXTS


class TestIsNonTruncableUrl:
    """Contract: is_non_truncable_url riconosce URL non troncabili per estensione."""

    def test_parquet_url(self) -> None:
        assert is_non_truncable_url("https://example.test/data.parquet") is True

    def test_csv_url(self) -> None:
        assert is_non_truncable_url("https://example.test/data.csv") is False

    def test_no_extension(self) -> None:
        assert is_non_truncable_url("https://example.test/api/v1/download") is False

    def test_query_params(self) -> None:
        assert is_non_truncable_url("https://example.test/download?format=csv") is False

    def test_uppercase_extension(self) -> None:
        assert is_non_truncable_url("https://example.test/data.ZIP") is True


class TestTruncateAtLine:
    """Contract: truncate_at_line taglia bytes all'ultima riga completa."""

    def test_shorter_than_limit(self) -> None:
        content = b"hello\nworld\n"
        assert truncate_at_line(content, 100) == content

    def test_exact_limit(self) -> None:
        content = b"hello\nworld\n"
        assert truncate_at_line(content, len(content)) == content

    def test_truncate_at_newline(self) -> None:
        content = b"line1\nline2\nline3\nline4\n"
        result = truncate_at_line(content, 15)
        # 15 bytes = "line1\nline2\nlin" → tronca a "line1\nline2\n"
        assert result == b"line1\nline2\n"
        assert result.endswith(b"\n")

    def test_no_newline_before_limit(self) -> None:
        content = b"this is a long line with no newline until the end\n"
        result = truncate_at_line(content, 10)
        # 10 bytes = "this is a " — no \n found in first 10 bytes, but rfind after truncation
        # After truncation to 10 bytes, rfind(b"\n") returns -1, so we keep the 10 bytes
        assert result == b"this is a "

    def test_empty_content(self) -> None:
        assert truncate_at_line(b"", 100) == b""

    def test_newline_at_start_of_truncated(self) -> None:
        content = b"\nabcdefghijklmnopqrstuvwxyz"
        result = truncate_at_line(content, 5)
        # 5 bytes = "\nabcd" — rfind(b"\n") = 0, skip (last_newline > 0 è falso)
        assert result == b"\nabcd"
