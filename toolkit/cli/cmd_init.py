"""Top-level `init` command — bootstrap a dataset from URL or config.

Unlike `run init` (which lives under `run`), `toolkit init` is a first-class
entry point discoverable via `--help`. It does the same thing: raw + clean.sql scaffold.

Usage:
    toolkit init --config <dataset.yml> [--years Y] [--dry-run] [--strict-config]
    toolkit run init --config <dataset.yml>  # same thing, backward-compatible alias
"""

from __future__ import annotations

import typer

from toolkit.cli.cmd_run import run_init as _run_init

# Re-export run_init as the init command implementation
# Both `toolkit init` and `toolkit run init` call the same function.


def init(
    config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
    years: str | None = typer.Option(None, "--years", help="Comma-separated dataset years"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print plan without executing"),
    strict_config: bool = typer.Option(False, "--strict-config", help="Treat deprecated config forms as errors"),
):
    """
    Bootstrap candidate: esegue run raw e scaffold clean.sql se assente.

    Non esegue clean ne mart. Output: raw scaricato, profilo disponibile,
    sql/clean.sql scaffoldato oppure skip esplicito se gia esistente.

    Questo comando e' anche disponibile come `toolkit run init`.
    """
    _run_init(
        config=config,
        years=years,
        dry_run=dry_run,
        strict_config=strict_config,
    )


def register(app: typer.Typer) -> None:
    app.command("init")(init)
