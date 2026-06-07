"""Tests for toolkit/core/csv_read.py — normalize_columns_spec, normalize_encoding, validate_nullstr."""

import pytest

from toolkit.core.csv_read import (
    normalize_columns_spec,
    normalize_read_cfg,
    _validate_nullstr,
)
from toolkit.core.io import normalize_encoding


# ---------------------------------------------------------------------------
# normalize_columns_spec — error cases
# ---------------------------------------------------------------------------


NORMALIZE_COLUMNS_SPEC_ERROR_CASES = [
    # (input, must_contain_all)
    # dict: non-string name
    ({123: "VARCHAR", "b": "VARCHAR"}, ["123", "int"]),
    # dict: non-string dtype
    ({"col1": 123, "col2": "VARCHAR"}, ["123", "int"]),
    # dict: both non-string
    ({123: 456}, ["123", "456"]),
    # list: non-string name
    ([{"name": 123, "type": "VARCHAR"}], ["123", "name"]),
    # list: non-string dtype
    ([{"name": "col1", "type": 999}], ["999", "type"]),
    # list: both non-string
    ([{"name": 111, "type": 222}], ["111", "222"]),
    # list: missing name key
    ([{"type": "VARCHAR"}], ["None"]),
    # list: missing type key
    ([{"name": "col1"}], ["None"]),
]


@pytest.mark.policy
@pytest.mark.parametrize("invalid_input, must_contain_all", NORMALIZE_COLUMNS_SPEC_ERROR_CASES)
def test_normalize_columns_spec_errors(invalid_input, must_contain_all):
    """Invalid inputs to normalize_columns_spec must surface the bad value in the error."""
    with pytest.raises(ValueError) as exc_info:
        normalize_columns_spec(invalid_input)
    msg = str(exc_info.value)
    for fragment in must_contain_all:
        assert fragment in msg, f"Expected '{fragment}' in error message: {msg}"


@pytest.mark.policy
def test_normalize_columns_spec_valid_cases():
    """Valid dict and list inputs pass through unchanged. None returns None."""
    assert normalize_columns_spec({"a": "VARCHAR", "b": "INTEGER"}) == {
        "a": "VARCHAR",
        "b": "INTEGER",
    }
    assert normalize_columns_spec([{"name": "a", "type": "VARCHAR"}]) == {"a": "VARCHAR"}
    assert normalize_columns_spec(None) is None


# ---------------------------------------------------------------------------
# normalize_encoding — alias normalization
# ---------------------------------------------------------------------------


ENCODING_CASES = [
    # latin1 family → CP1252 (superset, DuckDB-friendly)
    ("latin1", "CP1252"),
    ("LATIN1", "CP1252"),
    # utf-8 family
    ("utf8", "utf-8"),
    ("UTF8", "utf-8"),
    # windows cp variants
    ("win1252", "CP1252"),
    ("Windows1252", "CP1252"),
    # iso-8859-1 variants → CP1252
    ("iso-8859-1", "CP1252"),
    ("ISO-8859-1", "CP1252"),
    ("iso8859-1", "CP1252"),
    # ascii
    ("ascii", "us-ascii"),
    ("ASCII", "us-ascii"),
    # already normalized
    ("utf-8", "utf-8"),
    ("latin-1", "CP1252"),
    ("CP1252", "CP1252"),
    ("us-ascii", "us-ascii"),
    # whitespace stripping
    ("  latin1  ", "CP1252"),
    ("\tutf8\t", "utf-8"),
    # None
    (None, None),
]


@pytest.mark.policy
@pytest.mark.parametrize("input_enc, expected", ENCODING_CASES)
def test_normalize_encoding(input_enc, expected):
    """normalize_encoding maps common encoding aliases to their canonical form."""
    assert normalize_encoding(input_enc) == expected


# ---------------------------------------------------------------------------
# _validate_nullstr — null marker validation
# ---------------------------------------------------------------------------


VALID_NULLSTR_CASES = [
    "NA",
    "",
    "-",
    "NULL",
    None,
    ["NA", "-", ""],
    ["NULL", "N/A", "n/a"],
]


@pytest.mark.policy
@pytest.mark.parametrize("valid_input", VALID_NULLSTR_CASES)
def test_validate_nullstr_accepts_valid(valid_input):
    """Valid nullstr values (strings, None, list of strings) are accepted."""
    _validate_nullstr(valid_input)  # must not raise


INVALID_NULLSTR_ERROR_CASES = [
    # (invalid_input, must_contain_all)
    ([1, 2, "NA"], ["1", "int"]),
    ([None, "NA"], ["None", "NoneType"]),
    ({"NA": True}, ["dict"]),
    (123, ["123", "int"]),
]


@pytest.mark.policy
@pytest.mark.parametrize("invalid_input, must_contain_all", INVALID_NULLSTR_ERROR_CASES)
def test_validate_nullstr_rejects_invalid(invalid_input, must_contain_all):
    """Invalid nullstr types (numbers, dict, list with non-strings) are rejected with the bad value."""
    with pytest.raises(ValueError) as exc_info:
        _validate_nullstr(invalid_input)
    msg = str(exc_info.value)
    for fragment in must_contain_all:
        assert fragment in msg, f"Expected '{fragment}' in error message: {msg}"


@pytest.mark.policy
def test_normalize_read_cfg_rejects_bad_nullstr():
    """normalize_read_cfg propagates nullstr validation errors."""
    with pytest.raises(ValueError) as exc_info:
        normalize_read_cfg({"nullstr": [1, 2, 3]})
    msg = str(exc_info.value)
    assert "nullstr" in msg
    assert "1" in msg
