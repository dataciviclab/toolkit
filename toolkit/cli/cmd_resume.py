from __future__ import annotations

import typer

from toolkit.cli.cmd_run import run_year
from toolkit.core.config import load_config
from toolkit.core.logging import get_logger
from toolkit.core.run_context import get_run_dir, latest_run, read_run_record


def _resume_layer(record: dict[str, object]) -> str | None:
    layers = record.get("layers") or {}
    for layer in ("raw", "clean", "mart"):
        status = (layers.get(layer) or {}).get("status")
        if status != "SUCCESS":
            return layer
    return None


def resume(
    dataset: str = typer.Option(..., "--dataset", help="Dataset name"),
    year: int = typer.Option(..., "--year", help="Dataset year"),
    run_id: str | None = typer.Option(None, "--run-id", help="Specific run id"),
    latest: bool = typer.Option(False, "--latest", help="Resume latest run"),
    compat: bool = typer.Option(False, "--compat", help="Allow resume from non-portable legacy run records"),
    config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
    strict_config: bool = typer.Option(False, "--strict-config", help="Treat deprecated config forms as errors"),
):
    """
    Riprende un run dal primo layer non SUCCESS.
    """
    if run_id and latest:
        raise typer.BadParameter("Use either --run-id or --latest, not both")

    strict_config_flag = strict_config if isinstance(strict_config, bool) else False
    cfg = load_config(config, strict_config=strict_config_flag)
    if cfg.dataset != dataset:
        raise typer.BadParameter(f"Config dataset mismatch: expected {dataset}, found {cfg.dataset}")
    if year not in cfg.years:
        raise typer.BadParameter(f"Year {year} is not configured in {config}")

    run_dir = get_run_dir(cfg.root, dataset, year)
    try:
        record = read_run_record(run_dir, run_id) if run_id else latest_run(run_dir)
    except FileNotFoundError as exc:
        raise typer.BadParameter(str(exc)) from exc

    portability = record.get("_portability") or {}
    if not portability.get("portable", True):
        warning = (
            "Run record contains absolute paths outside the current root and is non-portable. "
            "Use --compat to resume anyway."
        )
        if not compat:
            typer.echo(warning, err=True)
            raise typer.Exit(code=2)
        typer.echo(f"warning: {warning}")

    start_from_layer = _resume_layer(record)
    if start_from_layer is None:
        typer.echo("Nothing to resume")
        return

    logger = get_logger()
    new_context = run_year(
        cfg,
        year,
        step="all",
        start_from_layer=start_from_layer,
        logger=logger,
        resumed_from=str(record.get("run_id")),
    )
    typer.echo(
        f"Resumed from {record.get('run_id')} starting at {start_from_layer}. New run_id: {new_context.run_id}"
    )


def register(app: typer.Typer) -> None:
    app.command("resume")(resume)
