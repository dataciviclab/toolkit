"""Configuration models for the DataCivicLab toolkit.

This package groups config models by layer for maintainability.
All public symbols are re-exported here for backward compatibility.
"""

from __future__ import annotations

# --- path_normalization ---
from toolkit.core.config_models.path_normalization import (
    _err,
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
    _MANAGED_OUTPUT_ROOTS,
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

# --- Layer models ---
from toolkit.core.config_models.raw import (
    ClientConfig,
    ExtractorConfig,
    RawConfig,
    RawSourceConfig,
)
from toolkit.core.config_models.clean import (
    CleanConfig,
    CleanReadConfig,
    CleanValidateConfig,
    CleanValidationSpec,
)
from toolkit.core.config_models.mart import (
    MartConfig,
    MartTableConfig,
    MartTableRuleConfig,
    MartValidateConfig,
    MartValidationSpec,
    TransitionConfig,
)
from toolkit.core.config_models.cross_year import (
    CrossYearConfig,
    CrossYearTableConfig,
)

# --- Loader (ToolkitConfigModel and load_config_model) ---
from toolkit.core.config_models._loader import (
    ToolkitConfigModel,
    load_config_model,
)

__all__ = [
    # path_normalization
    "_err",
    "_MANAGED_OUTPUT_ROOTS",
    "_require_map",
    "_ensure_root_within_repo",
    "_is_managed_output_root",
    "_iter_matching_tokens",
    "_normalize_section_paths",
    "_path_tokens_to_str",
    "_resolve_root",
    "_set_nested_value",
    "_get_nested_value",
    "_resolve_path_value",
    # shared_models
    "ConfigDeprecation",
    "ConfigPolicy",
    "DatasetBlock",
    "GlobalValidationConfig",
    "OutputConfig",
    "RangeRuleConfig",
    "SupportDatasetConfig",
    "TimeCoverage",
    "_CONFIG_DEPRECATIONS",
    "_SAFE_SQL_IDENTIFIER_RE",
    "ensure_str_list",
    "parse_bool",
    # policy
    "_TOP_LEVEL_ALLOWED_KEYS",
    "_declared_model_keys",
    "_emit_deprecation_notice",
    "_emit_unknown_keys_notice",
    "_normalize_legacy_payload",
    "_warn_or_reject_unknown_keys",
    # Raw
    "ClientConfig",
    "ExtractorConfig",
    "RawConfig",
    "RawSourceConfig",
    # Clean
    "CleanConfig",
    "CleanReadConfig",
    "CleanValidateConfig",
    "CleanValidationSpec",
    # Mart
    "MartConfig",
    "MartTableConfig",
    "MartTableRuleConfig",
    "MartValidateConfig",
    "MartValidationSpec",
    "TransitionConfig",
    # Cross-year
    "CrossYearConfig",
    "CrossYearTableConfig",
    # Loader
    "ToolkitConfigModel",
    "load_config_model",
]
