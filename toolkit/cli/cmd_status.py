"""toolkit status alias — hidden, delega a inspect summary."""

from __future__ import annotations

import typer

from toolkit.cli.inspect.summary_ops import summary as _summary


def status(
    config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
    year: int | None = typer.Option(None, "--year", "-y", help="Dataset year (default: first)"),
    dataset: str | None = typer.Option(None, "--dataset", help="Dataset name (auto-da-config)"),
    run_id: str | None = typer.Option(None, "--run-id", help="Specific run id"),
    latest: bool = typer.Option(False, "--latest", help="Show latest run"),
    as_json: bool = typer.Option(False, "--json", help="Emit JSON output"),
):
    """Alias nascosto — usa ``toolkit inspect summary``."""
    _summary(
        config=config,
        year=year,
        dataset=dataset,
        run_id=run_id,
        latest=latest,
        as_json=as_json,
    )


def register(app: typer.Typer) -> None:
    app.command("status", hidden=True)(status)
