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


def iter_selected_years(
    cfg,
    *,
    year_arg: int | None = None,
    years_arg: str | None = None,
) -> list[int]:
    if year_arg is not None and years_arg is not None:
        raise ValueError("Use either --year or --years, not both")

    if years_arg is None:
        return iter_years(cfg, year_arg)

    requested: list[int] = []
    for raw_part in years_arg.split(","):
        part = raw_part.strip()
        if not part:
            raise ValueError("Invalid --years value: empty year entry")
        try:
            year = int(part)
        except ValueError as exc:
            raise ValueError(f"Invalid --years value: '{part}' is not an integer year") from exc
        if year not in requested:
            requested.append(year)

    if not requested:
        raise ValueError("Invalid --years value: no years provided")

    invalid = [year for year in requested if year not in cfg.years]
    if invalid:
        listed = ", ".join(str(year) for year in invalid)
        raise ValueError(f"Year(s) not configured in dataset.yml: {listed}")

    return requested
