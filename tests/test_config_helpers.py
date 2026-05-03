"""Tests for config helper functions: parse_bool, ensure_str_list."""

import pytest

from toolkit.core.config import ensure_str_list, parse_bool


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (True, True),
        (False, False),
        ("true", True),
        ("false", False),
        ("1", True),
        ("0", False),
        (1, True),
        (0, False),
        ("yes", True),
        ("no", False),
    ],
)
def test_parse_bool_accepts_supported_boolean_like_values(value, expected):
    assert parse_bool(value, "field") is expected


def test_parse_bool_rejects_unsupported_value():
    with pytest.raises(ValueError):
        parse_bool("maybe", "field")


def test_ensure_str_list_accepts_single_string_and_list():
    assert ensure_str_list("col_a", "field") == ["col_a"]
    assert ensure_str_list(["col_a", "col_b"], "field") == ["col_a", "col_b"]


def test_ensure_str_list_rejects_non_string_items():
    with pytest.raises(ValueError):
        ensure_str_list(["col_a", 2], "field")
