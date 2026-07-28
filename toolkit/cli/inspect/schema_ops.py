"""Comando CLI ``inspect schema`` — wrapper Typer puro.

La logica e' in ``toolkit.domain.schema.show_schema``.
"""

from __future__ import annotations

import json

import typer

from toolkit.domain.schema import show_schema


def schema(
    config_path: str = typer.Argument(
        "", metavar="CONFIG", help="Path al dataset.yml (posizionale)"
    ),
    config: str = typer.Option(None, "--config", "-c", help="Path al dataset.yml", hidden=True),
    layer: str = typer.Option("clean", "--layer", "-l", help="Layer: raw, clean, mart"),
    year: int = typer.Option(0, "--year", "-y", help="Anno (default: ultimo)"),
    json_output: bool = typer.Option(False, "--json", help="Output JSON"),
) -> None:
    """Mostra lo schema (colonne + tipi) di raw, clean o mart.

    .. deprecated::
        Usa ``toolkit inspect config -c CONFIG -l {layer} -m schema`` invece.

    Il path config puo' essere passato come argomento posizionale
    (es. toolkit inspect schema path/to/dataset.yml, hidden alias)
    o con l'opzione --config / -c.
    """
    resolved_config = config or config_path
    if not resolved_config:
        typer.echo("error: specificare il path al dataset.yml (argomento o --config)", err=True)
        raise typer.Exit(code=1)

    try:
        result = show_schema(resolved_config, layer, year or None)
    except (ValueError, FileNotFoundError) as exc:
        if json_output:
            typer.echo(json.dumps({"status": "error", "message": str(exc)}, indent=2))
        else:
            typer.echo(f"Errore: {exc}")
        raise typer.Exit(code=1)

    status = "ok" if result.get("columns") or result.get("entries") else "empty"

    if json_output:
        typer.echo(json.dumps(result, indent=2, default=str))
        return

    if status == "error":
        typer.echo(f"Errore: {result.get('message', 'sconosciuto')}")
        raise typer.Exit(code=1)

    columns = result.get("columns", [])
    typer.echo(f"Layer: {result.get('layer', layer)}")
    typer.echo(f"Anno: {result.get('year', year or '?')}")
    typer.echo(f"Colonne: {len(columns)}")
    for col in columns:
        typer.echo(f"  {col.get('name', '?'):30s} {col.get('type', '?')}")
