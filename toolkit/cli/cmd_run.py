from __future__ import annotations

from pathlib import Path

import typer

from toolkit.cli.common import iter_years, load_cfg_and_logger
from toolkit.clean.run import run_clean
from toolkit.clean.validate import run_clean_validation
from toolkit.core.logging import bind_logger, get_logger
from toolkit.core.paths import layer_year_dir
from toolkit.core.run_context import RunContext
from toolkit.mart.run import run_mart
from toolkit.mart.validate import run_mart_validation
from toolkit.raw.run import run_raw
from toolkit.raw.validate import run_raw_validation


class ValidationGateError(RuntimeError):
    pass


def _validation_runner(layer_name: str):
    if layer_name == "raw":
        return lambda cfg, year, logger: run_raw_validation(cfg.root, cfg.dataset, year, logger)
    if layer_name == "clean":
        return run_clean_validation
    if layer_name == "mart":
        return run_mart_validation
    raise ValueError(f"Unsupported validation layer: {layer_name}")


def _planned_layers(step: str) -> list[str]:
    if step == "all":
        return ["raw", "clean", "mart"]
    return [step]


def _resolve_sql_path(cfg, rel_path: str | None) -> Path:
    if not rel_path:
        raise ValueError("Missing SQL path in dataset.yml")
    return Path(rel_path)


def _validate_execution_plan(cfg, step: str) -> list[str]:
    layers = _planned_layers(step)

    if "clean" in layers:
        clean_sql = _resolve_sql_path(cfg, cfg.clean.get("sql"))
        if not clean_sql.exists():
            raise FileNotFoundError(f"CLEAN SQL file not found: {clean_sql}")

    if "mart" in layers:
        tables = cfg.mart.get("tables") or []
        if not isinstance(tables, list) or not tables:
            raise ValueError("mart.tables missing or empty in dataset.yml")
        for table in tables:
            if not isinstance(table, dict):
                raise ValueError("Each entry in mart.tables must be a mapping (dict).")
            sql_path = _resolve_sql_path(cfg, table.get("sql"))
            if not sql_path.exists():
                raise FileNotFoundError(f"MART SQL file not found: {sql_path}")

    return layers


def _layers_from_start(layers: list[str], start_from_layer: str | None) -> list[str]:
    if start_from_layer is None:
        return layers
    if start_from_layer not in layers:
        raise ValueError(f"Cannot start from layer '{start_from_layer}' for planned steps {layers}")
    start_index = layers.index(start_from_layer)
    return layers[start_index:]


def _print_execution_plan(cfg, year: int, layers: list[str], context: RunContext, fail_on_error: bool) -> None:
    typer.echo("Execution Plan")
    typer.echo(f"dataset: {cfg.dataset}")
    typer.echo(f"year: {year}")
    typer.echo("status: DRY_RUN")
    typer.echo(f"run_id: {context.run_id}")
    if context.resumed_from:
        typer.echo(f"resumed_from: {context.resumed_from}")
    typer.echo(f"steps: {', '.join(layers)}")
    typer.echo(f"validation.fail_on_error: {fail_on_error}")
    typer.echo(f"run_record: {context.path}")
    typer.echo("output_dirs:")
    for layer in layers:
        typer.echo(f"  - {layer}: {layer_year_dir(cfg.root, layer, cfg.dataset, year)}")
    typer.echo("")


def run_year(
    cfg,
    year: int,
    *,
    step: str,
    start_from_layer: str | None = None,
    dry_run: bool = False,
    logger=None,
    resumed_from: str | None = None,
) -> RunContext:
    if logger is None:
        logger = get_logger()

    fail_on_error = bool((cfg.validation or {}).get("fail_on_error", True))
    planned_layers = _validate_execution_plan(cfg, step)
    layers_to_run = _layers_from_start(planned_layers, start_from_layer)

    context = RunContext(cfg.dataset, year, root=cfg.root, resumed_from=resumed_from)
    base_logger = bind_logger(
        logger,
        dataset=cfg.dataset,
        year=year,
        run_id=context.run_id,
    )
    base_logger.info(
        "RUN context | dataset=%s year=%s base_dir=%s effective_root=%s root_source=%s",
        cfg.dataset,
        year,
        cfg.base_dir,
        cfg.root,
        cfg.root_source,
    )

    if dry_run:
        context.mark_dry_run()
        _print_execution_plan(cfg, year, layers_to_run, context, fail_on_error)
        return context

    base_logger.info(f"RUN -> step={step} dataset={cfg.dataset} year={year}")
    run_has_validation_warnings = False

    def _execute_layer(layer_name: str, target, *args, **kwargs) -> None:
        nonlocal run_has_validation_warnings

        layer_logger = bind_logger(base_logger, layer=layer_name)
        context.start_layer(layer_name)
        try:
            target(*args, logger=layer_logger, **kwargs)
            context.complete_layer(layer_name)

            summary = _validation_runner(layer_name)(cfg, year, layer_logger)
            context.set_validation(layer_name, summary)
            if not summary.get("passed", False):
                message = f"{layer_name.upper()} validation failed"
                if fail_on_error:
                    raise ValidationGateError(message)
                run_has_validation_warnings = True
        except Exception as exc:
            context.fail_layer(layer_name, str(exc))
            context.fail_run(str(exc))
            raise

    if "raw" in layers_to_run:
        _execute_layer(
            "raw",
            run_raw,
            cfg.dataset,
            year,
            cfg.root,
            cfg.raw,
            base_dir=cfg.base_dir,
            run_id=context.run_id,
            strict_plugins=bool((getattr(cfg, "config", {}) or {}).get("strict", False)),
        )

    if "clean" in layers_to_run:
        _execute_layer(
            "clean",
            run_clean,
            cfg.dataset,
            year,
            cfg.root,
            cfg.clean,
            base_dir=cfg.base_dir,
            output_cfg=cfg.output,
        )

    if "mart" in layers_to_run:
        _execute_layer(
            "mart",
            run_mart,
            cfg.dataset,
            year,
            cfg.root,
            cfg.mart,
            base_dir=cfg.base_dir,
            output_cfg=cfg.output,
        )

    context.complete_run(success_with_warnings=run_has_validation_warnings)
    return context


def run(
    step: str = typer.Argument(..., help="raw | clean | mart | all"),
    config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print execution plan without executing"),
    strict_config: bool = typer.Option(False, "--strict-config", help="Treat deprecated config forms as errors"),
):
    """
    Esegue un singolo step della pipeline.
    """
    strict_config_flag = strict_config if isinstance(strict_config, bool) else False
    cfg, logger = load_cfg_and_logger(config, strict_config=strict_config_flag)
    dry_run_flag = dry_run if isinstance(dry_run, bool) else False

    if step not in {"raw", "clean", "mart", "all"}:
        raise typer.BadParameter("step must be one of: raw, clean, mart, all")

    for year in iter_years(cfg, None):
        run_year(cfg, year, step=step, dry_run=dry_run_flag, logger=logger)


def register(app: typer.Typer) -> None:
    app.command("run")(run)
