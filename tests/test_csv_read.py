"""Tests for toolkit/core/csv_read.py — normalize_columns_spec, normalize_encoding, validate_nullstr."""

import contextlib
import logging

import pytest

from toolkit.core.csv_read import (
    normalize_columns_spec,
    normalize_read_cfg,
    _validate_nullstr,
)
from toolkit.core.io import normalize_encoding, normalize_duckdb_encoding


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
    # latin1 family
    ("latin1", "latin-1"),
    ("LATIN1", "latin-1"),
    # utf-8 family
    ("utf8", "utf-8"),
    ("UTF8", "utf-8"),
    # windows cp variants
    ("win1252", "CP1252"),
    ("Windows1252", "CP1252"),
    # iso-8859-1 variants
    ("iso-8859-1", "latin-1"),
    ("ISO-8859-1", "latin-1"),
    ("iso8859-1", "latin-1"),
    # ascii
    ("ascii", "us-ascii"),
    ("ASCII", "us-ascii"),
    # already normalized
    ("utf-8", "utf-8"),
    ("latin-1", "latin-1"),
    ("CP1252", "CP1252"),
    ("us-ascii", "us-ascii"),
    # whitespace stripping
    ("  latin1  ", "latin-1"),
    ("\tutf8\t", "utf-8"),
    # None
    (None, None),
]


@pytest.mark.policy
@pytest.mark.parametrize("input_enc, expected", ENCODING_CASES)
def test_normalize_encoding(input_enc, expected):
    """normalize_encoding maps common encoding aliases to their canonical form."""
    assert normalize_encoding(input_enc) == expected


DUCKDB_ENCODING_CASES = [
    ("latin1", "CP1252"),
    ("latin-1", "CP1252"),
    ("iso-8859-1", "CP1252"),
    ("utf-8", "utf-8"),
    ("CP1252", "CP1252"),
    (None, None),
]


@pytest.mark.policy
@pytest.mark.parametrize("input_enc, expected", DUCKDB_ENCODING_CASES)
def test_normalize_duckdb_encoding(input_enc, expected):
    """normalize_duckdb_encoding mappa latin-1 → CP1252 per DuckDB."""
    assert normalize_duckdb_encoding(input_enc) == expected


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


@pytest.mark.pure_unit
def test_robust_preset_disables_parallel_when_null_padding():
    """DuckDB: parallel scanner + null_padding non supporta quoted newlines."""
    from toolkit.core.csv_read import robust_preset

    cfg = robust_preset({})
    assert cfg["null_padding"] is True
    assert cfg["parallel"] is False


@pytest.mark.pure_unit
def test_robust_preset_forces_strict_mode_with_null_padding():
    """null_padding + strict_mode=false misparsa i campi quotati con "" e newline.

    Osservato su camera_sindisp leg 16: 44.304 righe corrette -> 44.655
    frammenti. ignore_errors resta attivo per le righe malformate.
    """
    from toolkit.core.csv_read import robust_preset

    # Anche se il dataset dichiara strict_mode: false, con null_padding va forzato
    cfg = robust_preset({"strict_mode": False})
    assert cfg["null_padding"] is True
    assert cfg["strict_mode"] is True
    assert cfg["ignore_errors"] is True


@pytest.mark.pure_unit
def test_robust_preset_keeps_parallel_when_null_padding_off():
    from toolkit.core.csv_read import robust_preset

    cfg = robust_preset({"null_padding": False, "parallel": True})
    assert cfg["null_padding"] is False
    assert cfg["parallel"] is True


@contextlib.contextmanager
def _capture_logger(name: str, level: int = logging.WARNING):
    """Cattura in modo deterministico i record di un logger specifico.

    Non usa il fixture `caplog`: con xdist (`-n auto`) la cattura via root
    handler non e' affidabile — il test passava in isolamento ma falliva in
    suite (CI come sorteggio). Un handler dedicato sul logger e' deterministico.
    """
    records: list[logging.LogRecord] = []

    class _Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    logger = logging.getLogger(name)
    handler = _Capture()
    prev_level = logger.level
    logger.addHandler(handler)
    logger.setLevel(level)
    try:
        yield records
    finally:
        logger.removeHandler(handler)
        logger.setLevel(prev_level)


@pytest.mark.policy
def test_robust_preset_warns_when_overriding_explicit_strict_mode():
    """La sovrascrittura non deve essere silenziosa per i dataset che la dichiarano."""
    from toolkit.core.csv_read import robust_preset

    with _capture_logger("toolkit.core.csv_read") as records:
        robust_preset({"strict_mode": False})
    assert any("strict_mode=false" in r.getMessage() for r in records)


@pytest.mark.policy
def test_robust_preset_no_warning_without_explicit_strict_mode():
    from toolkit.core.csv_read import robust_preset

    with _capture_logger("toolkit.core.csv_read") as records:
        robust_preset({})
    assert not records
