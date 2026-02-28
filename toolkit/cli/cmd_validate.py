from __future__ import annotations

import typer

from toolkit.cli.common import iter_years, load_cfg_and_logger
from toolkit.clean.validate import run_clean_validation
from toolkit.mart.validate import run_mart_validation


def _raise_on_failed_summary(summary: dict[str, object]) -> None:
    if not bool(summary.get("passed")):
        raise typer.Exit(code=1)


def validate(
    step: str = typer.Argument(..., help="clean | mart | all"),
    config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
):
    """
    Quality gate per CLEAN e MART.
    Usa regole opzionali in dataset.yml:
      clean.validate.*
      mart.validate.table_rules.*
    """
    cfg, logger = load_cfg_and_logger(config)

    for year in iter_years(cfg, None):
        if step == "all":
            _raise_on_failed_summary(run_clean_validation(cfg, year, logger))
            _raise_on_failed_summary(run_mart_validation(cfg, year, logger))

        elif step == "clean":
            _raise_on_failed_summary(run_clean_validation(cfg, year, logger))

        elif step == "mart":
            _raise_on_failed_summary(run_mart_validation(cfg, year, logger))

        else:
            raise typer.BadParameter("step must be one of: clean, mart, all")


def register(app: typer.Typer) -> None:
    app.command("validate")(validate)
