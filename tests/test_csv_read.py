"""Tests for toolkit/core/csv_read.py — normalize_columns_spec and error messages."""

import pytest

from toolkit.core.csv_read import normalize_columns_spec


class TestNormalizeColumnsSpecErrors:
    """Error messages must include actual values that caused the failure."""

    def test_dict_with_non_string_name_includes_value(self):
        """When name is not a string, error shows the actual value."""
        with pytest.raises(ValueError) as exc_info:
            normalize_columns_spec({123: "VARCHAR", "b": "VARCHAR"})
        msg = str(exc_info.value)
        assert "123" in msg
        assert "int" in msg

    def test_dict_with_non_string_dtype_includes_value(self):
        """When dtype is not a string, error shows the actual value and type."""
        with pytest.raises(ValueError) as exc_info:
            normalize_columns_spec({"col1": 123, "col2": "VARCHAR"})
        msg = str(exc_info.value)
        assert "123" in msg
        assert "int" in msg

    def test_dict_with_non_string_name_and_dtype(self):
        """Both non-string name and dtype are reported."""
        with pytest.raises(ValueError) as exc_info:
            normalize_columns_spec({123: 456})
        msg = str(exc_info.value)
        assert "123" in msg
        assert "456" in msg

    def test_list_with_non_string_name_includes_value(self):
        """List entry with non-string name shows actual value."""
        with pytest.raises(ValueError) as exc_info:
            normalize_columns_spec([{"name": 123, "type": "VARCHAR"}])
        msg = str(exc_info.value)
        assert "123" in msg
        assert "name" in msg

    def test_list_with_non_string_dtype_includes_value(self):
        """List entry with non-string dtype shows actual value."""
        with pytest.raises(ValueError) as exc_info:
            normalize_columns_spec([{"name": "col1", "type": 999}])
        msg = str(exc_info.value)
        assert "999" in msg
        assert "type" in msg

    def test_list_with_both_non_string_includes_both_values(self):
        """Both non-string name and dtype are reported."""
        with pytest.raises(ValueError) as exc_info:
            normalize_columns_spec([{"name": 111, "type": 222}])
        msg = str(exc_info.value)
        assert "111" in msg
        assert "222" in msg

    def test_list_with_missing_name_key(self):
        """List entry without 'name' key reports None."""
        with pytest.raises(ValueError) as exc_info:
            normalize_columns_spec([{"type": "VARCHAR"}])
        msg = str(exc_info.value)
        assert "None" in msg

    def test_list_with_missing_type_key(self):
        """List entry without 'type' key reports None."""
        with pytest.raises(ValueError) as exc_info:
            normalize_columns_spec([{"name": "col1"}])
        msg = str(exc_info.value)
        assert "None" in msg

    def test_valid_dict_passes(self):
        """Valid dict mapping returns normalized dict."""
        result = normalize_columns_spec({"a": "VARCHAR", "b": "INTEGER"})
        assert result == {"a": "VARCHAR", "b": "INTEGER"}

    def test_valid_list_passes(self):
        """Valid list of mappings returns normalized dict."""
        result = normalize_columns_spec([{"name": "a", "type": "VARCHAR"}])
        assert result == {"a": "VARCHAR"}

    def test_none_returns_none(self):
        """None input returns None."""
        assert normalize_columns_spec(None) is None


class TestNormalizeEncoding:
    """Encoding normalization maps common aliases to DuckDB-expected forms."""

    def test_existing_aliases(self):
        """Already supported aliases remain supported."""
        from toolkit.core.csv_read import normalize_encoding

        assert normalize_encoding("latin1") == "latin-1"
        assert normalize_encoding("LATIN1") == "latin-1"
        assert normalize_encoding("utf8") == "utf-8"
        assert normalize_encoding("UTF8") == "utf-8"
        assert normalize_encoding("win1252") == "CP1252"
        assert normalize_encoding("Windows1252") == "CP1252"

    def test_iso_8859_1_aliases(self):
        """ISO-8859-1 variants normalize to latin-1."""
        from toolkit.core.csv_read import normalize_encoding

        assert normalize_encoding("iso-8859-1") == "latin-1"
        assert normalize_encoding("ISO-8859-1") == "latin-1"
        assert normalize_encoding("iso8859-1") == "latin-1"

    def test_ascii_alias(self):
        """ASCII normalizes to us-ascii."""
        from toolkit.core.csv_read import normalize_encoding

        assert normalize_encoding("ascii") == "us-ascii"
        assert normalize_encoding("ASCII") == "us-ascii"

    def test_already_normalized(self):
        """Already canonical forms pass through unchanged."""
        from toolkit.core.csv_read import normalize_encoding

        assert normalize_encoding("utf-8") == "utf-8"
        assert normalize_encoding("latin-1") == "latin-1"
        assert normalize_encoding("CP1252") == "CP1252"
        assert normalize_encoding("us-ascii") == "us-ascii"

    def test_none_returns_none(self):
        """None input returns None."""
        from toolkit.core.csv_read import normalize_encoding

        assert normalize_encoding(None) is None

    def test_whitespace_stripped(self):
        """Leading/trailing whitespace is stripped before normalization."""
        from toolkit.core.csv_read import normalize_encoding

        assert normalize_encoding("  latin1  ") == "latin-1"
        assert normalize_encoding("\tutf8\t") == "utf-8"


class TestValidateNullstr:
    """nullstr must be string or list of strings — numbers become wrong strings."""

    def test_nullstr_scalar_string_ok(self):
        """Scalar string is valid."""
        from toolkit.core.csv_read import _validate_nullstr

        _validate_nullstr("NA")
        _validate_nullstr("")
        _validate_nullstr("-")
        _validate_nullstr("NULL")

    def test_nullstr_none_ok(self):
        """None means no null marker configured."""
        from toolkit.core.csv_read import _validate_nullstr

        _validate_nullstr(None)

    def test_nullstr_list_of_strings_ok(self):
        """List of strings is valid."""
        from toolkit.core.csv_read import _validate_nullstr

        _validate_nullstr(["NA", "-", ""])
        _validate_nullstr(["NULL", "N/A", "n/a"])

    def test_nullstr_list_with_number_rejects(self):
        """List containing a number is rejected with the actual value."""
        from toolkit.core.csv_read import _validate_nullstr

        with pytest.raises(ValueError) as exc_info:
            _validate_nullstr([1, 2, "NA"])
        msg = str(exc_info.value)
        assert "1" in msg
        assert "int" in msg

    def test_nullstr_list_with_none_rejects(self):
        """List containing None is rejected (None becomes string 'None')."""
        from toolkit.core.csv_read import _validate_nullstr

        with pytest.raises(ValueError) as exc_info:
            _validate_nullstr([None, "NA"])
        msg = str(exc_info.value)
        assert "None" in msg
        assert "NoneType" in msg

    def test_nullstr_dict_rejects(self):
        """Dict is not a valid nullstr type."""
        from toolkit.core.csv_read import _validate_nullstr

        with pytest.raises(ValueError) as exc_info:
            _validate_nullstr({"NA": True})
        msg = str(exc_info.value)
        assert "dict" in msg

    def test_nullstr_int_rejects(self):
        """Integer nullstr is rejected."""
        from toolkit.core.csv_read import _validate_nullstr

        with pytest.raises(ValueError) as exc_info:
            _validate_nullstr(123)
        msg = str(exc_info.value)
        assert "123" in msg
        assert "int" in msg

    def test_normalize_read_cfg_rejects_bad_nullstr(self):
        """normalize_read_cfg raises on invalid nullstr."""
        from toolkit.core.csv_read import normalize_read_cfg

        with pytest.raises(ValueError) as exc_info:
            normalize_read_cfg({"nullstr": [1, 2, 3]})
        msg = str(exc_info.value)
        assert "nullstr" in msg
        assert "1" in msg
