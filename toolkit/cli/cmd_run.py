from __future__ import annotations

import typer

from toolkit.core.config import load_config
from toolkit.core.logging import get_logger

from toolkit.raw.run import run_raw
from toolkit.clean.run import run_clean
from toolkit.mart.run import run_mart


def run(
    step: str = typer.Argument(..., help="raw | clean | mart"),
    config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
):
    """
    Esegue un singolo step della pipeline.
    """
    cfg = load_config(config)
    logger = get_logger()

    for year in cfg.years:
        logger.info(f"RUN → step={step} dataset={cfg.dataset} year={year}")

        if step == "raw":
            run_raw(cfg.dataset, year, cfg.root, cfg.raw, logger)

        elif step == "clean":
            run_clean(cfg.dataset, year, cfg.root, cfg.clean, logger, base_dir=cfg.base_dir)

        elif step == "mart":
            run_mart(cfg.dataset, year, cfg.root, cfg.mart, logger, base_dir=cfg.base_dir)

        else:
            raise typer.BadParameter("step must be one of: raw, clean, mart")


def register(app: typer.Typer) -> None:
    app.command("run")(run)