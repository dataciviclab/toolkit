from __future__ import annotations

import typer

from toolkit.core.config import load_config
from toolkit.core.run_context import get_run_dir, latest_run, read_run_record


def _layer_row(record: dict[str, object], layer: str) -> str:
    layer_info = (record.get("layers") or {}).get(layer, {})
    validation = (record.get("validations") or {}).get(layer, {})
    validation_passed = validation.get("passed")
    return (
        f"{layer:<5} "
        f"{str(layer_info.get('status', 'PENDING')):<20} "
        f"{str(validation_passed):<17} "
        f"{str(validation.get('errors_count', 0)):<12} "
        f"{str(validation.get('warnings_count', 0)):<14}"
    )


def status(
    dataset: str = typer.Option(..., "--dataset", help="Dataset name"),
    year: int = typer.Option(..., "--year", help="Dataset year"),
    run_id: str | None = typer.Option(None, "--run-id", help="Specific run id"),
    latest: bool = typer.Option(False, "--latest", help="Show latest run"),
    config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
):
    """
    Mostra lo stato dell'ultimo run o di uno specifico run_id.
    """
    if run_id and latest:
        raise typer.BadParameter("Use either --run-id or --latest, not both")

    cfg = load_config(config)
    run_dir = get_run_dir(cfg.root, dataset, year)
    record = read_run_record(run_dir, run_id) if run_id else latest_run(run_dir)

    typer.echo(f"dataset: {record.get('dataset')}")
    typer.echo(f"year: {record.get('year')}")
    typer.echo(f"run_id: {record.get('run_id')}")
    typer.echo(f"started_at: {record.get('started_at')}")
    typer.echo(f"status: {record.get('status')}")
    portability = record.get("_portability") or {}
    if not portability.get("portable", True):
        typer.echo("portable: False")
    typer.echo("")
    typer.echo("layer layer_status         validation_passed errors_count warnings_count")
    for layer in ("raw", "clean", "mart"):
        typer.echo(_layer_row(record, layer))

    if record.get("status") == "FAILED" and record.get("error"):
        typer.echo("")
        typer.echo(f"error: {record.get('error')}")


def register(app: typer.Typer) -> None:
    app.command("status")(status)
