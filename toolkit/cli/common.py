from __future__ import annotations

from toolkit.core.config import load_config
from toolkit.core.logging import get_logger


def load_cfg_and_logger(
    config_path: str,
    *,
    verbose: bool = False,
    quiet: bool = False,
    strict_config: bool = False,
):
    cfg = load_config(config_path, strict_config=strict_config)
    if verbose and quiet:
        raise ValueError("verbose and quiet cannot both be true")

    level: str | int = "INFO"
    if verbose:
        level = "DEBUG"
    elif quiet:
        level = "WARNING"

    logger = get_logger(level=level)
    return cfg, logger


def iter_years(cfg, year_arg: int | None = None) -> list[int]:
    if year_arg is None:
        return list(cfg.years)
    if year_arg not in cfg.years:
        raise ValueError(f"Year {year_arg} is not configured in dataset.yml")
    return [year_arg]
