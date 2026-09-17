from __future__ import annotations

from typing import Any, NamedTuple

from toolkit.core.config import ensure_dict, load_config
from toolkit.core.logging import get_logger

__all__ = ["ensure_dict", "load_cfg_and_logger", "resolve_run_context"]


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


class RunContextParams(NamedTuple):
    """Resolved parameters for a pipeline run."""

    cfg: Any
    logger: Any
    sample_rows: int | None
    sample_bytes: int | None
    sampling_active: bool
    dry_run: bool


def resolve_run_context(
    config: str | None,
    *,
    smoke: bool = False,
    sample_rows: int | None = None,
    sample_bytes: int | None = None,
    root: str | None = None,
    dry_run: bool = False,
) -> RunContextParams:
    """Resolve effective sampling params, root override, and load config + logger.

    Shared by CLI subcommands (``_make_step_cmd``) and pipeline orchestrator
    (``_run_pipeline``) to avoid duplicating smoke→defaults and root→smoke logic.
    """
    dry_flag = dry_run if isinstance(dry_run, bool) else False
    sample_rows_final = 1000 if smoke else sample_rows
    sample_bytes_final = 1048576 if smoke else sample_bytes
    sampling_active = sample_rows_final is not None or sample_bytes_final is not None

    root_override_final = root
    if sampling_active and not root and config is not None:
        _cfg0, _ = load_cfg_and_logger(config)
        root_override_final = str(_cfg0.root / "smoke")

    cfg, logger = load_cfg_and_logger(
        config,
        root_override=root_override_final,
    )

    return RunContextParams(
        cfg=cfg,
        logger=logger,
        sample_rows=sample_rows_final,
        sample_bytes=sample_bytes_final,
        sampling_active=sampling_active,
        dry_run=dry_flag,
    )
