"""Configuration models for the DataCivicLab toolkit.

This package groups config models by layer for maintainability.
All public symbols are re-exported here for backward compatibility.
"""

from __future__ import annotations

# --- Common (shared utilities and models) ---
from toolkit.core.config_models.common import (
    ConfigDeprecation,
    ConfigPolicy,
    DatasetBlock,
    GlobalValidationConfig,
    OutputConfig,
    RangeRuleConfig,
    SupportDatasetConfig,
    TimeCoverage,
    _CONFIG_DEPRECATIONS,
    _MANAGED_OUTPUT_ROOTS,
    _SAFE_SQL_IDENTIFIER_RE,
    _declared_model_keys,
    _ensure_root_within_repo,
    _err,
    _get_nested_value,
    _is_managed_output_root,
    _iter_matching_tokens,
    _normalize_legacy_payload,
    _normalize_section_paths,
    _path_tokens_to_str,
    _require_map,
    _resolve_path_value,
    _resolve_root,
    _set_nested_value,
    _warn_or_reject_unknown_keys,
    ensure_str_list,
    parse_bool,
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
    # Common
    "ConfigDeprecation",
    "ConfigPolicy",
    "DatasetBlock",
    "GlobalValidationConfig",
    "OutputConfig",
    "RangeRuleConfig",
    "SupportDatasetConfig",
    "TimeCoverage",
    "_CONFIG_DEPRECATIONS",
    "_MANAGED_OUTPUT_ROOTS",
    "_SAFE_SQL_IDENTIFIER_RE",
    "ensure_str_list",
    "parse_bool",
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
    # Internal utilities (used by config.py and tests)
    "_declared_model_keys",
    "_ensure_root_within_repo",
    "_err",
    "_get_nested_value",
    "_is_managed_output_root",
    "_iter_matching_tokens",
    "_normalize_legacy_payload",
    "_normalize_section_paths",
    "_path_tokens_to_str",
    "_require_map",
    "_resolve_path_value",
    "_resolve_root",
    "_set_nested_value",
    "_warn_or_reject_unknown_keys",
]
