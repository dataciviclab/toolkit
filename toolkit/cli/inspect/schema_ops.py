"""CLI per `inspect schema` — schema di un layer raw/clean/mart."""

from __future__ import annotations

import json

import typer

from toolkit.mcp.schema_ops import show_schema


def schema(
    config_path: str = typer.Argument("", metavar="CONFIG", help="Path al dataset.yml (posizionale)"),
    config: str = typer.Option(None, "--config", "-c", help="Path al dataset.yml", hidden=True),
    layer: str = typer.Option("clean", "--layer", "-l", help="Layer: raw, clean, mart"),
    year: int = typer.Option(0, "--year", "-y", help="Anno (default: ultimo)"),
    json_output: bool = typer.Option(False, "--json", help="Output JSON"),
) -> None:
    """Mostra lo schema (colonne + tipi) di raw, clean o mart.

    Chiama la stessa implementazione del tool MCP toolkit_show_schema.

    Il path config puo' essere passato come argomento posizionale
    (es. toolkit inspect schema path/to/dataset.yml)
    o con l'opzione --config / -c.
    """
    resolved_config = config or config_path
    if not resolved_config:
        typer.echo("error: specificare il path al dataset.yml (argomento o --config)", err=True)
        raise typer.Exit(code=1)

    result = show_schema(resolved_config, layer, year or None)
    status = result.get("status", "ok" if result.get("columns") else "empty")

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
