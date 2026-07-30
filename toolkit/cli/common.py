from __future__ import annotations

from typing import Any

from toolkit.core.config import load_config
from toolkit.core.logging import get_logger


def dump_cfg_section(cfg_section: Any) -> Any:
    """Convert config section to dict for functions expecting dict.

    Ordine: model_dump → to_dict (dataclass) → Mapping → altra iterabile → valore nudo.
    Un dict non deve passare per il caso lista, altrimenti ``dump_cfg_section({"a": 1})``
    restituirebbe ``["a"]`` invece di ``{"a": 1}``.
    """
    if hasattr(cfg_section, "model_dump"):
        return cfg_section.model_dump(
            mode="python", by_alias=True, exclude_none=True, exclude_unset=True
        )
    if hasattr(cfg_section, "to_dict"):
        return cfg_section.to_dict()
    from dataclasses import asdict

    if hasattr(cfg_section, "__dataclass_fields__"):
        return {k: v for k, v in asdict(cfg_section).items() if v is not None}
    if isinstance(cfg_section, dict):
        return cfg_section
    if hasattr(cfg_section, "__iter__") and not isinstance(cfg_section, str):
        return [dump_cfg_section(item) for item in cfg_section]
    return cfg_section


def load_cfg_and_logger(
    config_path: str,
    *,
    verbose: bool = False,
    quiet: bool = False,
    strict_config: bool = False,
    root_override: str | None = None,
):
    cfg = load_config(config_path, strict_config=strict_config, root_override=root_override)
    if verbose and quiet:
        raise ValueError("verbose and quiet cannot both be true")

    level: str | int = "INFO"
    if verbose:
        level = "DEBUG"
    elif quiet:
        level = "WARNING"

    logger = get_logger(level=level)
    return cfg, logger
