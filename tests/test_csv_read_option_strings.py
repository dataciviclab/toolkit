from __future__ import annotations

import pytest

from toolkit.core.csv_read import csv_read_option_strings


class TestCsvReadOptionStrings:
    """Tests for the shared csv_read_option_strings function."""

    def test_empty_cfg_returns_empty_list(self):
        assert csv_read_option_strings({}) == []

    def test_delim(self):
        assert csv_read_option_strings({"delim": ";"}) == ["sep=';'"]

    def test_sep_alias(self):
        assert csv_read_option_strings({"sep": ","}) == ["sep=','"]

    def test_delim_over_sep(self):
        """sep takes precedence when both are present (matches raw.py behavior)."""
        result = csv_read_option_strings({"delim": ";", "sep": ","})
        assert result == ["sep=','"]

    def test_encoding(self):
        assert csv_read_option_strings({"encoding": "utf-8"}) == ["encoding='utf-8'"]

    def test_encoding_normalization(self):
        """normalize_encoding is applied."""
        result = csv_read_option_strings({"encoding": "utf8"})
        assert result == ["encoding='utf-8'"]

    def test_decimal(self):
        assert csv_read_option_strings({"decimal": ","}) == ["decimal_separator=','"]

    def test_nullstr_scalar(self):
        assert csv_read_option_strings({"nullstr": ""}) == ["nullstr=''"]

    def test_nullstr_list(self):
        result = csv_read_option_strings({"nullstr": ["", "-", "NA"]})
        assert result == ["nullstr=['', '-', 'NA']"]

    def test_boolean_flags(self):
        cfg = {
            "auto_detect": False,
            "strict_mode": True,
            "ignore_errors": True,
            "null_padding": True,
            "parallel": False,
        }
        result = csv_read_option_strings(cfg)
        assert "auto_detect=false" in result
        assert "strict_mode=true" in result
        assert "ignore_errors=true" in result
        assert "null_padding=true" in result
        assert "parallel=false" in result

    def test_quote_escape_comment(self):
        cfg = {"quote": '"', "escape": "\\", "comment": "#"}
        result = csv_read_option_strings(cfg)
        assert "quote='\"'" in result
        assert "escape='\\'" in result
        assert "comment='#'" in result

    def test_max_line_size(self):
        assert csv_read_option_strings({"max_line_size": 1000000}) == ["max_line_size=1000000"]

    def test_columns_dict(self):
        cfg = {"columns": {"id": "VARCHAR", "val": "INTEGER"}}
        result = csv_read_option_strings(cfg)
        assert len(result) == 1
        assert "columns={" in result[0]
        assert "'id': 'VARCHAR'" in result[0]
        assert "'val': 'INTEGER'" in result[0]

    def test_columns_list_ignored(self):
        """csv_read_option_strings only handles dict columns; list form is a config concern."""
        cfg = {"columns": [{"name": "id", "type": "VARCHAR"}]}
        # List columns aren't converted to SQL here (handled at config level)
        assert csv_read_option_strings(cfg) == []

    def test_sql_injection_escaping(self):
        """Single quotes in values are escaped."""
        cfg = {"delim": "'; DROP TABLE--", "encoding": "' OR 1=1 --"}
        result = csv_read_option_strings(cfg)
        assert "'';" in result[0]
        assert "'' OR 1=1" in result[1]

    def test_order_is_deterministic(self):
        """Option order follows the function implementation, not dict insertion."""
        cfg = {
            "columns": {"a": "VARCHAR"},
            "delim": ";",
            "auto_detect": False,
            "encoding": "utf-8",
        }
        result = csv_read_option_strings(cfg)
        # Order: delim, encoding, ..., auto_detect, ..., columns
        assert result[0].startswith("sep=")
        assert result[1].startswith("encoding=")
        assert result[-1].startswith("columns=")

    def test_full_cfg(self):
        """All supported keys together."""
        cfg = {
            "delim": ";",
            "encoding": "utf-8",
            "decimal": ",",
            "nullstr": ["", "NA"],
            "auto_detect": False,
            "strict_mode": False,
            "ignore_errors": True,
            "null_padding": True,
            "parallel": True,
            "quote": '"',
            "escape": "\\",
            "comment": "#",
            "max_line_size": 500000,
            "columns": {"id": "VARCHAR", "val": "DOUBLE"},
        }
        result = csv_read_option_strings(cfg)
        # sep, encoding, decimal, nullstr, auto_detect, strict_mode,
        # ignore_errors, null_padding, parallel, quote, escape, comment,
        # max_line_size, columns = 14
        assert len(result) == 14
