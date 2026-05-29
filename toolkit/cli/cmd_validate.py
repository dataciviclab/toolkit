from __future__ import annotations

import typer

from toolkit.cli.common import iter_selected_years, load_cfg_and_logger
from toolkit.clean.validate import run_clean_validation
from toolkit.mart.validate import run_mart_validation
from toolkit.raw.validate import run_raw_validation


def _raise_on_failed_summary(summary: dict[str, object]) -> None:
    if not bool(summary.get("passed")):
        raise typer.Exit(code=1)


def validate(
    step: str = typer.Argument(..., help="raw | clean | mart | all"),
    config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
    year: int | None = typer.Option(None, "--year", "-y", help="Single dataset year"),
    years: str | None = typer.Option(None, "--years", help="Comma-separated dataset years"),
):
    """
    Quality gate per RAW, CLEAN (include cross-layer raw→clean) e MART.
    Usa regole opzionali in dataset.yml:
      clean.validate.*
      mart.validate.table_rules.*
    """
    cfg, logger = load_cfg_and_logger(config)
    years_arg = years if isinstance(years, str) else None
    year_arg = year if isinstance(year, int) else None
    selected_years = iter_selected_years(cfg, year_arg=year_arg, years_arg=years_arg)

    for year in selected_years:
        if step == "all":
            _raise_on_failed_summary(run_raw_validation(cfg.root, cfg.dataset, year, logger))
            _raise_on_failed_summary(run_clean_validation(cfg, year, logger))
            _raise_on_failed_summary(run_mart_validation(cfg, year, logger))

        elif step == "raw":
            _raise_on_failed_summary(run_raw_validation(cfg.root, cfg.dataset, year, logger))

        elif step == "clean":
            _raise_on_failed_summary(run_clean_validation(cfg, year, logger))

        elif step == "mart":
            _raise_on_failed_summary(run_mart_validation(cfg, year, logger))

        else:
            raise typer.BadParameter("step must be one of: raw, clean, mart, all")


def register(app: typer.Typer) -> None:
    app.command("validate")(validate)
