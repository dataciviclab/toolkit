"""CLI per `inspect schema` — schema di un layer raw/clean/mart."""

from __future__ import annotations

import json

import typer

from toolkit.mcp.schema_ops import show_schema


def schema(
    config_path: str = typer.Argument(..., help="Path al dataset.yml", metavar="CONFIG"),
    layer: str = typer.Option("clean", "--layer", "-l", help="Layer: raw, clean, mart"),
    year: int = typer.Option(0, "--year", "-y", help="Anno (default: ultimo)"),
    json_output: bool = typer.Option(False, "--json", help="Output JSON"),
) -> None:
    """Mostra lo schema (colonne + tipi) di raw, clean o mart.

    Chiama la stessa implementazione del tool MCP toolkit_show_schema.
    """
    result = show_schema(config_path, layer, year or None)
    status = result.get("status", "ok" if result.get("columns") else "empty")

    if json_output:
        print(json.dumps(result, indent=2, default=str))
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
