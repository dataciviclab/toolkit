"""Tests for toolkit/core/artifacts.py."""

import pytest

from toolkit.core.artifacts import (
    ARTIFACT_POLICIES,
    ARTIFACT_POLICY_MINIMAL,
    ARTIFACT_POLICY_STANDARD,
    legacy_aliases_enabled,
    profile_required,
    resolve_artifact_policy,
    should_write,
)


# resolve_artifact_policy

@pytest.mark.policy
@pytest.mark.parametrize("cfg, expected", [
    (None, ARTIFACT_POLICY_STANDARD),
    ({}, ARTIFACT_POLICY_STANDARD),
    ({"artifacts": "standard"}, ARTIFACT_POLICY_STANDARD),
    ({"artifacts": "  standard  "}, ARTIFACT_POLICY_STANDARD),
    ({"artifacts": "STANDARD"}, ARTIFACT_POLICY_STANDARD),
    ({"artifacts": "minimal"}, ARTIFACT_POLICY_MINIMAL),
])
def test_resolve_artifact_policy_valid(cfg, expected) -> None:
    assert resolve_artifact_policy(cfg) == expected


@pytest.mark.policy
@pytest.mark.parametrize("policy", sorted(ARTIFACT_POLICIES))
def test_resolve_artifact_policy_all_policies_valid(policy) -> None:
    assert resolve_artifact_policy({"artifacts": policy}) == policy


@pytest.mark.policy
def test_resolve_artifact_policy_invalid_raises() -> None:
    with pytest.raises(ValueError) as exc_info:
        resolve_artifact_policy({"artifacts": "unknown"})
    assert "output.artifacts must be one of" in str(exc_info.value)


# legacy_aliases_enabled

@pytest.mark.policy
@pytest.mark.parametrize("cfg, expected", [
    (None, False),
    ({}, False),
    ({"legacy_aliases": True}, True),
    ({"legacy_aliases": False}, False),
])
def test_legacy_aliases_enabled(cfg, expected) -> None:
    assert legacy_aliases_enabled(cfg) is expected


@pytest.mark.policy
def test_legacy_aliases_enabled_returns_bool() -> None:
    assert isinstance(legacy_aliases_enabled({"legacy_aliases": "yes"}), bool)


# profile_required

@pytest.mark.policy
@pytest.mark.parametrize("cfg, expected", [
    (None, True),
    ({}, True),
    ({"clean": {}}, True),
    ({"clean": {"read": {"source": "auto"}}}, True),
    ({"clean": {"read": {"source": "duckdb"}}}, False),
    ({"clean": {"read": "duckdb"}}, False),
    ({"clean": {"read_source": "duckdb"}}, False),
])
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
@pytest.mark.parametrize("layer, artifact, policy, cfg, expected", [
    ("profile", "suggested_read", ARTIFACT_POLICY_STANDARD, {"clean": {"read": {"source": "auto"}}}, True),
    ("profile", "suggested_read", ARTIFACT_POLICY_STANDARD, {"clean": {"read": {"source": "duckdb"}}}, False),
    ("profile", "profile_alias", ARTIFACT_POLICY_STANDARD, {"output": {"artifacts": "standard", "legacy_aliases": True}}, True),
    ("profile", "profile_alias", ARTIFACT_POLICY_MINIMAL, {"output": {"artifacts": "minimal", "legacy_aliases": True}}, False),
    ("profile", "profile_alias", ARTIFACT_POLICY_STANDARD, {"output": {"artifacts": "standard", "legacy_aliases": False}}, False),
])
def test_should_write(layer, artifact, policy, cfg, expected) -> None:
    assert should_write(layer, artifact, policy, cfg) is expected


@pytest.mark.policy
@pytest.mark.parametrize("layer, artifact", [
    ("clean", "rendered_sql"),
    ("mart", "rendered_sql"),
])
def test_should_write_minimal_policy_suppresses_optional(layer, artifact) -> None:
    assert should_write(layer, artifact, ARTIFACT_POLICY_MINIMAL, {}) is False


@pytest.mark.policy
@pytest.mark.parametrize("layer, artifact", [
    ("clean", "data_parquet"),
    ("mart", "aggregated_parquet"),
])
def test_should_write_minimal_policy_keeps_required(layer, artifact) -> None:
    assert should_write(layer, artifact, ARTIFACT_POLICY_MINIMAL, {}) is True
