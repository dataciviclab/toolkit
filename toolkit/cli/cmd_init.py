"""Top-level `init` command — bootstrap a dataset from URL or config.

Usage:
    toolkit init --url <URL>                     # scout + generate dataset.yml
    toolkit init --url <URL> --run               # scout + run raw + scaffold
    toolkit init --config <dataset.yml>           # run raw + scaffold (existing)
"""

from __future__ import annotations

import typer

from toolkit.cli.cmd_run import run_init as _run_init
from toolkit.cli.cmd_scout import scout_url

_DEFAULT_TIMEOUT = 15


def init(
    config: str | None = typer.Option(None, "--config", "-c", help="Path to dataset.yml"),
    url: str | None = typer.Option(None, "--url", "-u", help="Download, profile and scaffold from URL"),
    year: int | None = typer.Option(None, "--year", "-y", help="Single dataset year (for --config)"),
    years: str | None = typer.Option(None, "--years", help="Comma-separated dataset years (for --config)"),
    run: bool = typer.Option(False, "--run", "-r", help="Also execute raw run after scaffold (only with --url)"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print plan without executing"),
    strict_config: bool = typer.Option(False, "--strict-config", help="Treat deprecated config forms as errors"),
):
    """
    Bootstrap candidate: prepara dataset.yml e scaffold SQL.

    Con --url: probe arricchito + scaffold candidate completo.
    Usa routing automatico per file, CKAN, SDMX e HTML.
    Usa --run per eseguire anche il run raw dopo lo scaffold.
    Alternativa: toolkit scout <URL> per sola ispezione.

    Con --config: run raw + scaffold clean.sql se assente.
    """
    if url and config:
        typer.echo("error: specificare --url o --config, non entrambi", err=True)
        raise typer.Exit(code=1)

    if url:
        scout_url(url, scaffold=True, run_raw=run, timeout=_DEFAULT_TIMEOUT)
        return

    if not config:
        typer.echo("error: specificare --url o --config", err=True)
        raise typer.Exit(code=1)

    _run_init(
        config=config,
        year=year,
        years=years,
        dry_run=dry_run,
        strict_config=strict_config,
    )


def register(app: typer.Typer) -> None:
    app.command("init")(init)
