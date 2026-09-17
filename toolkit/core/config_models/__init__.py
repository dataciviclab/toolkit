"""DEPRECATED: questo modulo e' un shim per backward compat.

Usa direttamente ``toolkit.core.config`` per tutti i simboli.
"""

from __future__ import annotations

import warnings

from toolkit.core.config import (
    CleanConfig,
    CleanReadConfig,
    CleanValidateConfig,
    CleanValidationSpec,
    HierarchyConfig,
    HierarchyLevel,
    MartConfig,
    MartTableConfig,
    MartTableRuleConfig,
    MartValidateConfig,
    MartValidationSpec,
    PipelineConfig,
    RangeRuleConfig,
    RawConfig,
    RawSourceConfig,
    ToolkitConfig,
    TransitionConfig,
    ensure_dict,
    ensure_str_list,
    load_config,
    parse_bool,
)

warnings.warn(
    "toolkit.core.config_models is deprecated. Import directly from toolkit.core.config instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Legacy aliases (kept for backward compat, will be removed in a future version)
load_config_model = load_config
ToolkitConfigModel = PipelineConfig

__all__ = [
    "CleanConfig",
    "CleanReadConfig",
    "CleanValidateConfig",
    "CleanValidationSpec",
    "HierarchyConfig",
    "HierarchyLevel",
    "MartConfig",
    "MartTableConfig",
    "MartTableRuleConfig",
    "MartValidateConfig",
    "MartValidationSpec",
    "PipelineConfig",
    "RangeRuleConfig",
    "RawConfig",
    "RawSourceConfig",
    "ToolkitConfig",
    "ToolkitConfigModel",
    "TransitionConfig",
    "ensure_dict",
    "ensure_str_list",
    "load_config",
    "load_config_model",
    "parse_bool",
]
