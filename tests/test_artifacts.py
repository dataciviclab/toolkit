"""Tests for toolkit/core/artifacts.py."""

import pytest

from toolkit.core.artifacts import profile_required, should_write


# profile_required


@pytest.mark.policy
@pytest.mark.parametrize(
    "cfg, expected",
    [
        (None, True),
        ({}, True),
        ({"clean": {}}, True),
        ({"clean": {"read": {"source": "auto"}}}, True),
        ({"clean": {"read": {"source": "duckdb"}}}, False),
        ({"clean": {"read": "duckdb"}}, False),
        ({"clean": {"read_source": "duckdb"}}, False),
    ],
)
def test_profile_required(cfg, expected) -> None:
    assert profile_required(cfg) is expected


@pytest.mark.policy
def test_profile_required_object_attribute_access() -> None:
    class Cfg:
        pass

    obj = Cfg()
    obj.clean = {"read": {"source": "duckdb"}}
    assert profile_required(obj) is False


# should_write


@pytest.mark.policy
@pytest.mark.parametrize(
    "layer, artifact, cfg, expected",
    [
        ("profile", "suggested_read", {"clean": {"read": {"source": "auto"}}}, True),
        ("profile", "suggested_read", {"clean": {"read": {"source": "duckdb"}}}, False),
    ],
)
def test_should_write(layer, artifact, cfg, expected) -> None:
    assert should_write(layer, artifact, cfg) is expected


@pytest.mark.policy
@pytest.mark.parametrize(
    "layer, artifact",
    [
        ("profile", "raw_profile"),
        ("clean", "rendered_sql"),
        ("mart", "rendered_sql"),
        ("clean", "data_parquet"),
        ("mart", "aggregated_parquet"),
    ],
)
def test_should_write_always_true(layer, artifact) -> None:
    """All artifacts always returned True — no policy gating."""
    assert should_write(layer, artifact, {}) is True
