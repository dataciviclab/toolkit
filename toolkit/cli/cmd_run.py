from __future__ import annotations

from pathlib import Path

import typer

from toolkit.cli.common import iter_selected_years, load_cfg_and_logger
from toolkit.cli.sql_dry_run import validate_sql_dry_run
from toolkit.clean.run import run_clean
from toolkit.clean.validate import run_clean_validation
from toolkit.cross.run import run_cross_year
from toolkit.cross.validate import run_cross_validation
from toolkit.core.logging import bind_logger, get_logger
from toolkit.core.paths import layer_dataset_dir, layer_year_dir
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
    if step == "cross_year":
        return ["cross_year"]
    return [step]


def _resolve_sql_path(cfg, rel_path: str | None) -> Path:
    if not rel_path:
        raise ValueError("Missing SQL path in dataset.yml")
    path = Path(rel_path)
    if path.is_absolute():
        return path
    return Path(cfg.base_dir) / path


def _is_mart_only_cfg(cfg) -> bool:
    return not bool(cfg.clean.get("sql"))


def _validate_execution_plan(cfg, step: str) -> list[str]:
    layers = _planned_layers(step)

    if step == "all" and _is_mart_only_cfg(cfg):
        raise ValueError(
            "run all is not supported for mart-only / compose-only configs; "
            "use: toolkit run mart --config ...",
        )

    if "clean" in layers:
        if _is_mart_only_cfg(cfg):
            raise ValueError(
                "run clean is not supported for mart-only / compose-only configs; "
                "use: toolkit run mart --config ...",
            )
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

    if "cross_year" in layers:
        tables = cfg.cross_year.get("tables") or []
        if not isinstance(tables, list) or not tables:
            raise ValueError("cross_year.tables missing or empty in dataset.yml")
        for table in tables:
            if not isinstance(table, dict):
                raise ValueError("Each entry in cross_year.tables must be a mapping (dict).")
            sql_path = _resolve_sql_path(cfg, table.get("sql"))
            if not sql_path.exists():
                raise FileNotFoundError(f"CROSS_YEAR SQL file not found: {sql_path}")
            if table.get("source_layer", "clean") == "mart" and not table.get("source_table"):
                raise ValueError("cross_year.tables[].source_table is required when source_layer = mart")

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
        if layer == "cross_year":
            typer.echo(f"  - {layer}: {layer_dataset_dir(cfg.root, 'cross', cfg.dataset)}")
        else:
            typer.echo(f"  - {layer}: {layer_year_dir(cfg.root, layer, cfg.dataset, year)}")
    typer.echo("")


def run_cross_year_step(
    cfg,
    *,
    years: list[int] | None = None,
    dry_run: bool = False,
    logger=None,
) -> None:
    if logger is None:
        logger = get_logger()

    _validate_execution_plan(cfg, "cross_year")
    output_dir = layer_dataset_dir(cfg.root, "cross", cfg.dataset)
    selected_years = list(years) if years is not None else list(cfg.years)

    if dry_run:
        typer.echo("Execution Plan")
        typer.echo(f"dataset: {cfg.dataset}")
        typer.echo("scope: cross_year")
        typer.echo("status: DRY_RUN")
        typer.echo(f"years: {', '.join(str(year) for year in selected_years)}")
        typer.echo("steps: cross_year")
        typer.echo(f"output_dir: {output_dir}")
        typer.echo("")
        return

    logger.info(
        "RUN cross_year | dataset=%s years=%s base_dir=%s effective_root=%s root_source=%s",
        cfg.dataset,
        ",".join(str(year) for year in selected_years),
        cfg.base_dir,
        cfg.root,
        cfg.root_source,
    )
    run_cross_year(
        cfg.dataset,
        selected_years,
        cfg.root,
        cfg.cross_year,
        logger,
        base_dir=cfg.base_dir,
        output_cfg=cfg.output,
    )
    summary = run_cross_validation(cfg, selected_years, logger)
    fail_on_error = bool((cfg.validation or {}).get("fail_on_error", True))
    if not summary.get("passed", False) and fail_on_error:
        raise ValidationGateError("CROSS_YEAR validation failed")


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
        try:
            validate_sql_dry_run(cfg, year=year, layers=layers_to_run)
        except Exception as exc:
            context.fail_run(str(exc))
            raise
        if any(layer in {"clean", "mart"} for layer in layers_to_run):
            typer.echo("sql_validation: OK")
            typer.echo("")
        return context

    base_logger.info(f"RUN -> step={step} dataset={cfg.dataset} year={year}")
    run_has_validation_warnings = False

    def _execute_layer(layer_name: str, target, *args, **kwargs) -> None:
        nonlocal run_has_validation_warnings

        layer_logger = bind_logger(base_logger, layer=layer_name)
        context.start_layer(layer_name)
        try:
            metrics = target(*args, logger=layer_logger, **kwargs)
            context.complete_layer(layer_name)
            if isinstance(metrics, dict):
                context.set_layer_metrics(layer_name, **metrics)

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
            output_cfg=cfg.output,
            clean_cfg=cfg.clean,
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
            clean_cfg=cfg.clean,
            output_cfg=cfg.output,
            support_cfg=cfg.support,
        )

    context.complete_run(success_with_warnings=run_has_validation_warnings)
    return context


# ---- run step wrappers for subcommand registration ----


def run(
    step: str,
    config: str,
    years: str | None = None,
    dry_run: bool = False,
    strict_config: bool = False,
):
    """Backward-compatible Python entrypoint used by tests and internal callers."""
    strict_flag = strict_config if isinstance(strict_config, bool) else False
    cfg, logger = load_cfg_and_logger(config, strict_config=strict_flag)
    dry_flag = dry_run if isinstance(dry_run, bool) else False
    years_arg = years if isinstance(years, str) else None
    selected_years = iter_selected_years(cfg, years_arg=years_arg)

    if step == "cross_year":
        run_cross_year_step(cfg, years=selected_years, dry_run=dry_flag, logger=logger)
        return

    for year in selected_years:
        run_year(cfg, year, step=step, dry_run=dry_flag, logger=logger)


def _make_step_cmd(step: str):
    """Factory: returns a Typer command wrapping run_year for the given step."""
    _step = step

    def cmd(
        config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
        years: str | None = typer.Option(None, "--years", help="Comma-separated dataset years"),
        dry_run: bool = typer.Option(False, "--dry-run", help="Print execution plan without executing"),
        strict_config: bool = typer.Option(False, "--strict-config", help="Treat deprecated config forms as errors"),
    ):
        strict_flag = strict_config if isinstance(strict_config, bool) else False
        cfg, logger = load_cfg_and_logger(config, strict_config=strict_flag)
        dry_flag = dry_run if isinstance(dry_run, bool) else False
        years_arg = years if isinstance(years, str) else None
        selected_years = iter_selected_years(cfg, years_arg=years_arg)

        if _step == "cross_year":
            run_cross_year_step(cfg, years=selected_years, dry_run=dry_flag, logger=logger)
            return

        for year in selected_years:
            run_year(cfg, year, step=_step, dry_run=dry_flag, logger=logger)

    cmd.__name__ = f"run_{_step}_cmd"
    cmd.__doc__ = f"Esegue lo step {_step} della pipeline."
    return cmd


run_raw_cmd = _make_step_cmd("raw")
run_clean_cmd = _make_step_cmd("clean")
run_mart_cmd = _make_step_cmd("mart")
run_all_cmd = _make_step_cmd("all")
run_cross_year_cmd = _make_step_cmd("cross_year")


def run_init(
    config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
    years: str | None = typer.Option(None, "--years", help="Comma-separated dataset years"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print plan without executing"),
    strict_config: bool = typer.Option(False, "--strict-config", help="Treat deprecated config forms as errors"),
):
    """
    Bootstrap candidate: esegue run raw e scaffold clean.sql se assente.

    Non esegue clean ne mart. Output: raw scaricato, profilo disponibile,
    sql/clean.sql scaffoldato oppure skip esplicito se gia esistente.
    """
    strict_config_flag = strict_config if isinstance(strict_config, bool) else False
    cfg, logger = load_cfg_and_logger(config, strict_config=strict_config_flag)
    dry_run_flag = dry_run if isinstance(dry_run, bool) else False
    years_arg = years if isinstance(years, str) else None
    selected_years = iter_selected_years(cfg, years_arg=years_arg)

    if dry_run_flag:
        # Validate the execution plan before showing the dry-run plan.
        # This catches missing sources, invalid paths, etc. early.
        try:
            _validate_execution_plan(cfg, "raw")
        except (ValueError, FileNotFoundError) as exc:
            raise typer.BadParameter(str(exc))

        # Also validate raw.sources specifically (not covered by _validate_execution_plan)
        raw_sources = cfg.raw.get("sources") if cfg.raw else None
        if not raw_sources:
            raise typer.BadParameter("raw.sources missing or empty in dataset.yml")

        typer.echo("Init bootstrap plan")
        typer.echo(f"dataset: {cfg.dataset}")
        typer.echo(f"years: {', '.join(str(y) for y in selected_years)}")
        typer.echo("steps: raw (+ scaffold clean.sql if missing)")
        typer.echo("status: DRY_RUN")
        typer.echo("")
        typer.echo("Nota: clean.sql sara scaffoldato solo se non esiste gia.")
        return

    for year in selected_years:
        logger.info("INIT | dataset=%s year=%s", cfg.dataset, year)

        # Track scaffold state BEFORE run_raw, so we can tell if it was
        # scaffolded by this run vs. pre-existing.
        clean_cfg = cfg.clean or {}
        clean_sql_rel = clean_cfg.get("sql", "sql/clean.sql")
        clean_sql_path = Path(cfg.base_dir) / clean_sql_rel
        scaffold_existed_before = clean_sql_path.exists()

        run_year(cfg, year, step="raw", dry_run=False, logger=logger)

        typer.echo(f"[init] Bootstrap completato per {cfg.dataset}/{year}")
        typer.echo("  - raw scaricato")
        typer.echo("  - profiling disponibile")

        scaffolded_now = not scaffold_existed_before and clean_sql_path.exists()
        if scaffold_existed_before:
            typer.echo(f"  - clean.sql gia esistente ({clean_sql_rel}), skip scaffold")
        elif scaffolded_now:
            typer.echo(f"  - clean.sql scaffoldato ({clean_sql_rel})")
        else:
            # clean.sql was not scaffolded: raw output is not a CSV that triggers
            # profiling, so no profile -> no scaffold.  Fail if bootstrap cannot proceed.
            profile_dir = layer_year_dir(cfg.root, "raw", cfg.dataset, year) / "_profile"
            profile_exists = (profile_dir / "profile.json").exists() or (
                profile_dir / "raw_profile.json"
            ).exists()
            if not profile_exists:
                raise typer.BadParameter(
                    f"Profilo raw non disponibile per {cfg.dataset}/{year}. "
                    f"Esegui prima: toolkit run raw -c <config> oppure crea clean.sql manualmente."
                )
            typer.echo("  - clean.sql non scaffoldato (nessun profilo raw disponibile)")
            typer.echo("    Esegui: toolkit scaffold clean -c <config> dopo aver verificato il profilo")

    typer.echo("")
    typer.echo("Prossimo passo: toolkit run clean -c <config>")


def register(app: typer.Typer) -> None:
    run_sub = typer.Typer(no_args_is_help=True, add_completion=False)
    run_sub.command("raw")(run_raw_cmd)
    run_sub.command("clean")(run_clean_cmd)
    run_sub.command("mart")(run_mart_cmd)
    run_sub.command("all")(run_all_cmd)
    run_sub.command("cross_year")(run_cross_year_cmd)
    run_sub.command("init")(run_init)
    app.add_typer(run_sub, name="run")
