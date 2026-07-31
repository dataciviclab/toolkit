from __future__ import annotations


from toolkit.core.config import ensure_dict, load_config
from toolkit.core.logging import get_logger

# Re-export per backward compat dei consumer CLI
__all__ = ["dump_cfg_section", "load_cfg_and_logger"]

dump_cfg_section = ensure_dict


def load_cfg_and_logger(
    config_path: str | None = None,
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
