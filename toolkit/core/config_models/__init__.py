"""
Shim per backward compat: re-esporta tutti i simboli dal nuovo config.py.

In precedenza questo package conteneva 24 modelli Pydantic in 9 file.
Ora tutto e' centralizzato in toolkit.core.config con semplici dataclass.
"""

from __future__ import annotations

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

# Old name used by some consumers
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
