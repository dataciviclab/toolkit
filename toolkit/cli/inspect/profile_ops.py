"""inspect profile — profilo diagnostico del RAW (encoding, delim, colonne).

La funzione run_profile() è pubblica cosicché cmd_profile.py (deprecato)
possa riusarla senza duplicare logica.
"""

from __future__ import annotations

from logging import Logger

import typer

from toolkit.cli.common import iter_selected_years, load_cfg_and_logger
from toolkit.core.artifacts import resolve_artifact_policy, should_write
from toolkit.core.paths import layer_year_dir
from toolkit.core.config import ToolkitConfig
from toolkit.profile.raw import profile_raw, write_raw_profile, write_suggested_read_yml


def run_profile(cfg: ToolkitConfig, years: list[int], logger: Logger) -> None:
    """Core logic: profiling RAW per ogni anno e scrittura su _profile/.

    Chiamabile sia da inspect/profile che da cmd_profile (deprecato).
    """
    for y in years:
        raw_dir = layer_year_dir(cfg.root, "raw", cfg.dataset, y)
        out_dir = raw_dir / "_profile"
        out_dir.mkdir(parents=True, exist_ok=True)
        policy = resolve_artifact_policy(cfg.output)

        prof = profile_raw(raw_dir, cfg.dataset, y, read_cfg=(cfg.clean or {}).get("read"))
        paths = write_raw_profile(out_dir, prof)
        written_paths = list(paths.values())

        if should_write("profile", "suggested_read", policy, cfg):
            written_paths.append(write_suggested_read_yml(out_dir, prof.__dict__))

        if written_paths:
            logger.info("PROFILE RAW -> %s", " | ".join(str(path) for path in written_paths))
        else:
            logger.info("PROFILE RAW -> no optional artifacts written for current policy")


def profile(
    config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
    year: int | None = typer.Option(None, "--year", "-y", help="Single dataset year"),
    years: str | None = typer.Option(None, "--years", help="Comma-separated dataset years"),
    strict_config: bool = typer.Option(
        False, "--strict-config", help="Treat deprecated config forms as errors"
    ),
):
    """
    Profilo diagnostico del RAW: encoding, delimitatore, colonne.

    Scrive raw_profile.json e (opzionalmente) suggested_read.yml
    nella directory _profile/ del raw layer.
    """
    strict_flag = strict_config if isinstance(strict_config, bool) else False
    year_val = year if isinstance(year, int) else None
    years_val = years if isinstance(years, str) else None
    cfg, logger = load_cfg_and_logger(config, strict_config=strict_flag)
    selected_years = iter_selected_years(cfg, year_arg=year_val, years_arg=years_val)
    run_profile(cfg, selected_years, logger)
