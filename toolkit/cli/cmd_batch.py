"""Batch execution — deprecato, usa ``toolkit run --batch <file>``."""

from __future__ import annotations

import warnings

import typer

from toolkit.cli.cmd_run import _run_batch


def batch(
    configs: str = typer.Option(
        ..., "--configs", help="Path to a text file with one dataset.yml path per line"
    ),
    step: str = typer.Option("all", "--step", help="probe | raw | clean | mart | all"),
    smoke: bool = typer.Option(
        False, "--smoke", help="Alias per --sample-rows 1000 --sample-bytes 1048576"
    ),
    sample_rows: int | None = typer.Option(
        None, "--sample-rows", help="Leggi solo N righe in CLEAN (LIMIT N sul output SQL)"
    ),
    sample_bytes: int | None = typer.Option(
        None,
        "--sample-bytes",
        help="Scarica solo N bytes in RAW (HTTP Range header + troncamento locale)",
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print execution plan without executing"),
    json_output: bool = typer.Option(
        False, "--json", help="Output in formato JSON (machine-readable)"
    ),
):
    """
    ⚠️  DEPRECATO: usa ``toolkit run --batch <file>``.

    Esegue piu' config in sequenza e stampa un report aggregato finale.
    """
    warnings.warn(
        "'toolkit batch' è deprecato, usa 'toolkit run --batch <file>'",
        DeprecationWarning,
        stacklevel=2,
    )
    typer.echo(
        "⚠️  'toolkit batch' è deprecato, usa 'toolkit run --batch <file>'",
        err=True,
    )
    _run_batch(
        batch_file=configs,
        step=step,
        years=None,
        smoke=smoke,
        sample_rows=sample_rows,
        sample_bytes=sample_bytes,
        root=None,
        json_output=json_output,
        dry_run=dry_run,
    )


def register(app: typer.Typer) -> None:
    app.command("batch", hidden=True)(batch)
