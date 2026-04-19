from __future__ import annotations

import typer

from toolkit.cli.common import iter_selected_years, load_cfg_and_logger
from toolkit.clean.validate import run_clean_validation, run_promotion_validation
from toolkit.mart.validate import run_mart_validation


def _raise_on_failed_summary(summary: dict[str, object]) -> None:
    if not bool(summary.get("passed")):
        raise typer.Exit(code=1)


def validate(
    step: str = typer.Argument(..., help="clean | mart | promotion | all"),
    config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
    years: str | None = typer.Option(None, "--years", help="Comma-separated dataset years"),
    strict_config: bool = typer.Option(False, "--strict-config", help="Treat deprecated config forms as errors"),
):
    """
    Quality gate per CLEAN, MART e promotion QA raw -> clean.
    Usa regole opzionali in dataset.yml:
      clean.validate.*
      mart.validate.table_rules.*
    """
    strict_config_flag = strict_config if isinstance(strict_config, bool) else False
    cfg, logger = load_cfg_and_logger(config, strict_config=strict_config_flag)
    years_arg = years if isinstance(years, str) else None
    selected_years = iter_selected_years(cfg, years_arg=years_arg)

    for year in selected_years:
        if step == "all":
            _raise_on_failed_summary(run_clean_validation(cfg, year, logger))
            _raise_on_failed_summary(run_mart_validation(cfg, year, logger))

        elif step == "clean":
            _raise_on_failed_summary(run_clean_validation(cfg, year, logger))

        elif step == "mart":
            _raise_on_failed_summary(run_mart_validation(cfg, year, logger))

        elif step == "promotion":
            _raise_on_failed_summary(run_promotion_validation(cfg, year, logger))

        else:
            raise typer.BadParameter("step must be one of: clean, mart, promotion, all")


def register(app: typer.Typer) -> None:
    app.command("validate")(validate)
