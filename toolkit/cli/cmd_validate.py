from __future__ import annotations

from typing import Any

import typer

from toolkit.core.config import load_config
from toolkit.core.logging import get_logger
from toolkit.core.paths import layer_year_dir

from toolkit.clean.validate import validate_clean
from toolkit.mart.validate import validate_mart

from toolkit.clean.report import write_clean_validation
from toolkit.mart.report import write_mart_validation


def validate(
    step: str = typer.Argument(..., help="clean | mart"),
    config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
):
    """
    Quality gate per CLEAN e MART.
    Usa regole opzionali in dataset.yml:
      clean.validate.*
      mart.validate.table_rules.*
    """
    cfg = load_config(config)
    logger = get_logger()

    for year in cfg.years:
        if step == "clean":
            out_dir = layer_year_dir(cfg.root, "clean", cfg.dataset, year)
            parquet = out_dir / f"{cfg.dataset}_{year}_clean.parquet"

            clean_cfg: dict[str, Any] = cfg.clean or {}
            required_cols = clean_cfg.get("required_columns", [])
            v: dict[str, Any] = (clean_cfg.get("validate") or {})

            res = validate_clean(
                parquet,
                required=required_cols,
                primary_key=v.get("primary_key"),
                not_null=v.get("not_null"),
                ranges=v.get("ranges"),
                max_null_pct=v.get("max_null_pct"),
                min_rows=v.get("min_rows"),
            )

            report = write_clean_validation(out_dir, res)
            logger.info(f"VALIDATE CLEAN → {report} (ok={res.ok})")
            if not res.ok:
                raise typer.Exit(code=1)

        elif step == "mart":
            mart_dir = layer_year_dir(cfg.root, "mart", cfg.dataset, year)

            mart_cfg: dict[str, Any] = cfg.mart or {}
            required_tables = mart_cfg.get("required_tables", [])

            mv: dict[str, Any] = (mart_cfg.get("validate") or {})
            table_rules: dict[str, Any] = (mv.get("table_rules") or {})

            res = validate_mart(
                mart_dir,
                required_tables=required_tables,
                table_rules=table_rules,
            )

            report = write_mart_validation(mart_dir, res)
            logger.info(f"VALIDATE MART → {report} (ok={res.ok})")
            if not res.ok:
                raise typer.Exit(code=1)

        else:
            raise typer.BadParameter("step must be one of: clean, mart")


def register(app: typer.Typer) -> None:
    app.command("validate")(validate)