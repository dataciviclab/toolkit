"""CLI command: toolkit profile (DEPRECATED, usa toolkit inspect profile).

Wrapper deprecato che delega a inspect/profile_ops.run_profile().
"""

from __future__ import annotations

import warnings

import typer

from toolkit.cli.common import iter_selected_years, load_cfg_and_logger


def profile(
    step: str = typer.Argument(..., help="raw"),
    config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
    year: int | None = typer.Option(None, "--year", "-y", help="Single dataset year"),
    years: str | None = typer.Option(None, "--years", help="Comma-separated dataset years"),
    strict_config: bool = typer.Option(
        False, "--strict-config", help="Treat deprecated config forms as errors"
    ),
):
    """
    Profiling (assist) per i layer. Per ora: raw.

    ⚠️  DEPRECATO: usa 'toolkit inspect profile --config <path>' invece.
    """
    warnings.warn(
        "'toolkit profile' e' deprecato. Usa 'toolkit inspect profile'.",
        DeprecationWarning, stacklevel=2,
    )

    if step != "raw":
        raise typer.BadParameter("step must be: raw")

    strict_flag = strict_config if isinstance(strict_config, bool) else False
    year_val = year if isinstance(year, int) else None
    years_val = years if isinstance(years, str) else None
    cfg, logger = load_cfg_and_logger(config, strict_config=strict_flag)
    selected_years = iter_selected_years(cfg, year_arg=year_val, years_arg=years_val)

    from toolkit.cli.inspect.profile_ops import run_profile

    run_profile(cfg, selected_years, logger)


def register(app: typer.Typer) -> None:
    app.command("profile")(profile)
