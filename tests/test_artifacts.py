"""Tests for toolkit/core/artifacts.py."""

import pytest

from toolkit.core.artifacts import (
    ARTIFACT_POLICIES,
    ARTIFACT_POLICY_DEBUG,
    ARTIFACT_POLICY_MINIMAL,
    ARTIFACT_POLICY_STANDARD,
    legacy_aliases_enabled,
    profile_required,
    resolve_artifact_policy,
    should_write,
)


class TestResolveArtifactPolicy:
    def test_standard_default(self) -> None:
        assert resolve_artifact_policy(None) == ARTIFACT_POLICY_STANDARD

    def test_explicit_standard(self) -> None:
        assert resolve_artifact_policy({"artifacts": "standard"}) == ARTIFACT_POLICY_STANDARD

    def test_explicit_minimal(self) -> None:
        assert resolve_artifact_policy({"artifacts": "minimal"}) == ARTIFACT_POLICY_MINIMAL

    def test_explicit_debug(self) -> None:
        assert resolve_artifact_policy({"artifacts": "debug"}) == ARTIFACT_POLICY_DEBUG

    def test_whitespace_stripped(self) -> None:
        assert resolve_artifact_policy({"artifacts": "  standard  "}) == ARTIFACT_POLICY_STANDARD

    def test_case_insensitive(self) -> None:
        assert resolve_artifact_policy({"artifacts": "DEBUG"}) == ARTIFACT_POLICY_DEBUG

    def test_invalid_raises_value_error(self) -> None:
        with pytest.raises(ValueError) as exc_info:
            resolve_artifact_policy({"artifacts": "unknown"})
        assert "output.artifacts must be one of" in str(exc_info.value)

    def test_empty_dict_defaults_to_standard(self) -> None:
        assert resolve_artifact_policy({}) == ARTIFACT_POLICY_STANDARD

    def test_all_policies_are_valid(self) -> None:
        for policy in ARTIFACT_POLICIES:
            assert resolve_artifact_policy({"artifacts": policy}) == policy


class TestLegacyAliasesEnabled:
    def test_none_returns_true(self) -> None:
        assert legacy_aliases_enabled(None) is True

    def test_empty_dict_returns_true(self) -> None:
        assert legacy_aliases_enabled({}) is True

    def test_explicit_true(self) -> None:
        assert legacy_aliases_enabled({"legacy_aliases": True}) is True

    def test_explicit_false(self) -> None:
        assert legacy_aliases_enabled({"legacy_aliases": False}) is False

    def test_returns_bool(self) -> None:
        assert isinstance(legacy_aliases_enabled({"legacy_aliases": "yes"}), bool)


class TestProfileRequired:
    def test_dict_cfg_auto_source(self) -> None:
        cfg = {"clean": {"read": {"source": "auto"}}}
        assert profile_required(cfg) is True

    def test_dict_cfg_explicit_source(self) -> None:
        cfg = {"clean": {"read": {"source": "duckdb"}}}
        assert profile_required(cfg) is False

    def test_dict_cfg_string_read(self) -> None:
        cfg = {"clean": {"read": "duckdb"}}
        assert profile_required(cfg) is False

    def test_dict_cfg_read_source_fallback(self) -> None:
        cfg = {"clean": {"read_source": "duckdb"}}
        assert profile_required(cfg) is False

    def test_dict_cfg_no_read(self) -> None:
        cfg = {"clean": {}}
        assert profile_required(cfg) is True

    def test_empty_dict(self) -> None:
        assert profile_required({}) is True

    def test_none(self) -> None:
        assert profile_required(None) is True

    def test_attribute_access_dict(self) -> None:
        # simulate object with attributes
        class Cfg:
            pass

        obj = Cfg()
        obj.clean = {"read": {"source": "duckdb"}}
        assert profile_required(obj) is False


class TestShouldWrite:
    def test_debug_policy_always_true(self) -> None:
        assert should_write("clean", "any_artifact", ARTIFACT_POLICY_DEBUG, {}) is True
        assert should_write("mart", "any_artifact", ARTIFACT_POLICY_DEBUG, {}) is True
        assert should_write("profile", "raw_profile", ARTIFACT_POLICY_DEBUG, {}) is True

    def test_minimal_policy_suppresses_optional_clean_artifacts(self) -> None:
        assert should_write("clean", "rendered_sql", ARTIFACT_POLICY_MINIMAL, {}) is False
        assert should_write("mart", "rendered_sql", ARTIFACT_POLICY_MINIMAL, {}) is False

    def test_minimal_policy_keeps_required_artifacts(self) -> None:
        assert should_write("clean", "data_parquet", ARTIFACT_POLICY_MINIMAL, {}) is True
        assert should_write("mart", "aggregated_parquet", ARTIFACT_POLICY_MINIMAL, {}) is True

    def test_profile_raw_profile_minimal_suppressed(self) -> None:
        assert (
            should_write("profile", "raw_profile", ARTIFACT_POLICY_MINIMAL, {}) is False
        )

    def test_profile_suggested_read_auto(self) -> None:
        cfg = {"clean": {"read": {"source": "auto"}}}
        assert should_write("profile", "suggested_read", ARTIFACT_POLICY_STANDARD, cfg) is True

    def test_profile_suggested_read_explicit_source(self) -> None:
        cfg = {"clean": {"read": {"source": "duckdb"}}}
        assert should_write("profile", "suggested_read", ARTIFACT_POLICY_STANDARD, cfg) is False

    def test_profile_md_never_written_non_debug(self) -> None:
        # DEBUG short-circuits to True at line 49; non-DEBUG policies suppress profile_md
        assert should_write("profile", "profile_md", ARTIFACT_POLICY_STANDARD, {}) is False
        assert should_write("profile", "profile_md", ARTIFACT_POLICY_MINIMAL, {}) is False

    def test_profile_suggested_mapping_never_written_non_debug(self) -> None:
        assert should_write("profile", "suggested_mapping", ARTIFACT_POLICY_STANDARD, {}) is False
        assert should_write("profile", "suggested_mapping", ARTIFACT_POLICY_DEBUG, {}) is True  # DEBUG short-circuits

    def test_profile_md_debug_short_circuits(self) -> None:
        # DEBUG policy returns True immediately, before the layer-specific checks
        assert should_write("profile", "profile_md", ARTIFACT_POLICY_DEBUG, {}) is True

    def test_standard_policy_keeps_optional_artifacts(self) -> None:
        cfg = {"output": {"artifacts": "standard", "legacy_aliases": True}}
        assert should_write("profile", "profile_alias", ARTIFACT_POLICY_STANDARD, cfg) is True

    def test_minimal_policy_suppresses_profile_alias(self) -> None:
        cfg = {"output": {"artifacts": "minimal", "legacy_aliases": True}}
        assert should_write("profile", "profile_alias", ARTIFACT_POLICY_MINIMAL, cfg) is False

    def test_profile_alias_without_legacy(self) -> None:
        cfg = {"output": {"artifacts": "standard", "legacy_aliases": False}}
        assert should_write("profile", "profile_alias", ARTIFACT_POLICY_STANDARD, cfg) is False
