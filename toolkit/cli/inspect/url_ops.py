"""inspect url — DEPRECATO. Usa 'toolkit scout'."""

from __future__ import annotations

import warnings

import typer

from toolkit.cli.cmd_scout import scout_url
from toolkit.scout.http import DEFAULT_TIMEOUT


def url(
    ctx: typer.Context,
    url_arg: str = typer.Argument(..., help="URL da esplorare"),
    scaffold: bool = typer.Option(False, "--scaffold", "-s", help="Genera scaffold candidate dataset"),
    run: bool = typer.Option(False, "--run", "-r", help="Scaffold + raw run (implies --scaffold)"),
    timeout: int = typer.Option(DEFAULT_TIMEOUT, "--timeout", min=1, help="Timeout HTTP in secondi"),
) -> None:
    """Esplora un URL esterno. DEPRECATO: usa 'toolkit scout'."""
    warnings.warn(
        "'toolkit inspect url' e' deprecato. Usa 'toolkit scout'.",
        DeprecationWarning,
        stacklevel=2,
    )
    typer.echo("⚠️  'toolkit inspect url' e' deprecato. Usa 'toolkit scout'.", err=True)
    if run:
        scaffold = True
    scout_url(url_arg, timeout=timeout, scaffold=scaffold, run_raw=run)
