"""Shared utilities and models used across all config layers.

This module is a thin re-export facade. All responsibilities have been
migrated to dedicated sub-modules:
- path_normalization: path resolution and section normalization
- shared_models: Pydantic models, ConfigDeprecation, coercion helpers
- policy: unknown-key detection, deprecation notices, legacy normalization
"""

from __future__ import annotations

# ruff: noqa: F401
# Re-exported for backward compat via __init__.py
# (consumers import from common, not from submodules directly)

# --- path_normalization ---
from toolkit.core.config_models.path_normalization import (
    _err,
    _MANAGED_OUTPUT_ROOTS,
    _require_map,
    _ensure_root_within_repo,
    _is_managed_output_root,
    _iter_matching_tokens,
    _normalize_section_paths,
    _path_tokens_to_str,
    _resolve_root,
    _set_nested_value,
    _get_nested_value,
    _resolve_path_value,
)

# --- shared_models ---
from toolkit.core.config_models.shared_models import (
    ConfigDeprecation,
    ConfigPolicy,
    DatasetBlock,
    GlobalValidationConfig,
    OutputConfig,
    RangeRuleConfig,
    SupportDatasetConfig,
    TimeCoverage,
    _CONFIG_DEPRECATIONS,
    _SAFE_SQL_IDENTIFIER_RE,
    ensure_str_list,
    parse_bool,
)

# --- policy ---
from toolkit.core.config_models.policy import (
    _TOP_LEVEL_ALLOWED_KEYS,
    _declared_model_keys,
    _emit_deprecation_notice,
    _emit_unknown_keys_notice,
    _normalize_legacy_payload,
    _warn_or_reject_unknown_keys,
)
