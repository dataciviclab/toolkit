"""CLI command: toolkit blocker-hints

Esporta blocker_hints come interfaccia CLI pubblica, invece di chiamare
il modulo interno toolkit.mcp.toolkit_client.

Usage:
    toolkit blocker-hints --config candidates/terna-electricity-by-source/dataset.yml --year 2023
    toolkit blocker-hints --config candidates/terna-electricity-by-source/dataset.yml --year 2023 --json
"""

from __future__ import annotations

from pathlib import Path

from toolkit.mcp.schema_ops import blocker_hints as _blocker_hints
from toolkit.core.config import load_config

import typer


def blocker_hints(
    config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
    year: int | None = typer.Option(None, "--year", "-y", help="Dataset year (default: last declared year)"),
    as_json: bool = typer.Option(False, "--json", help="Emit JSON output"),
) -> None:
    """
    Mostra hint diagnostici per mismatch comuni tra config dichiarato e output.

    I blocker sono errori che impediscono al candidate di funzionare.
    I warning sono segnali di possibili problemi che non bloccano l'esecuzione.

    Exit code:
        0 — hint generati (anche se ci sono blocker, il comando funziona)
        1 — config non trovato o errore nell'analisi
    """
    try:
        # Use load_config like other CLI commands (run, init, status) so that
        # relative paths are resolved from the config file's base_dir, not from
        # WORKSPACE_ROOT. This matches the behavior of `toolkit run all` etc.
        load_config(config, strict_config=False)
        # Pass the resolved path of the actual config file, not a reconstructed name.
        config_path_resolved = str(Path(config).resolve())
        result = _blocker_hints(config_path_resolved, year)
    except FileNotFoundError:
        typer.echo(f"error: config file not found: {config}", err=True)
        raise typer.Exit(code=1)
    except Exception as exc:
        # Surface file-not-found errors (YAML read failure due to missing file)
        # with a clear message rather than a raw exception type.
        exc_msg = str(exc).lower()
        if "no such file or directory" in exc_msg or "non trovata" in exc_msg:
            typer.echo(f"error: config file not found: {config}", err=True)
        else:
            typer.echo(f"error: {type(exc).__name__}: {exc}", err=True)
        raise typer.Exit(code=1)

    if as_json:
        import json
        typer.echo(json.dumps(result, indent=2, ensure_ascii=False))
        return

    # Human-readable output
    dataset = result.get("dataset", "?")
    config_path = result.get("config_path", "?")
    year_val = result.get("year", "?")
    blocker_count = result.get("blocker_count", 0)
    warning_count = result.get("warning_count", 0)

    typer.echo(f"dataset: {dataset}")
    typer.echo(f"config: {config_path}")
    typer.echo(f"year: {year_val}")
    typer.echo(f"blockers: {blocker_count}")
    typer.echo(f"warnings: {warning_count}")
    typer.echo("")

    hints = result.get("hints", [])
    if not hints:
        typer.echo("nessun hint — config e output sono coerenti")
        return

    typer.echo("hints:")
    for hint in hints:
        severity = hint.get("severity", "?")
        code = hint.get("code", "?")
        message = hint.get("message", "")
        icon = "🔴" if severity == "blocker" else "⚠️"
        typer.echo(f"  {icon} [{severity}] {code}")
        typer.echo(f"      {message}")

    typer.echo("")
    if blocker_count > 0:
        typer.echo(f"🔴 {blocker_count} blocker(s) trovati — fix obbligatori prima del merge")
    else:
        typer.echo("✅ nessun blocker — config e output sono coerenti")


def register(app: typer.Typer) -> None:
    app.command("blocker-hints")(blocker_hints)
