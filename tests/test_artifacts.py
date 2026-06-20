"""Tests for toolkit/core/artifacts.py."""

import pytest

from toolkit.core.artifacts import should_write


@pytest.mark.policy
@pytest.mark.parametrize(
    "layer, artifact",
    [
        ("profile", "raw_profile"),
        ("profile", "suggested_read"),
        ("clean", "rendered_sql"),
        ("clean", "data_parquet"),
        ("mart", "rendered_sql"),
        ("mart", "aggregated_parquet"),
    ],
)
def test_should_write_always_true(layer: str, artifact: str) -> None:
    """All artifacts are always written — no policy gating."""
    assert should_write(layer, artifact) is True
