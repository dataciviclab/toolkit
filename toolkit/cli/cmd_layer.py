"""toolkit layer alias — hidden, delega a inspect config."""

from __future__ import annotations

import typer

from toolkit.cli.inspect.config_ops import config as _inspect_config


def layer_cmd(
    config: str = typer.Option(..., "--config", "-c", help="Path a dataset.yml o slug del dataset"),
    layer: str = typer.Option("clean", "--layer", "-l", help="Layer: raw, clean, mart"),
    mode: str = typer.Option(
        "schema",
        "--mode",
        "-m",
        help="Modalità: schema (default), preview, profile, sql",
    ),
    year: int = typer.Option(0, "--year", "-y", help="Anno (default: ultimo configurato)"),
    sql: str | None = typer.Option(None, "--sql", help="SQL query (solo mode=sql)"),
    limit: int = typer.Option(20, "--limit", help="Max righe (solo mode=preview/sql)"),
    mart_index: int = typer.Option(0, "--mart-index", help="Indice tabella mart (default 0)"),
    json_output: bool = typer.Option(False, "--json", help="Output JSON"),
) -> None:
    """Alias nascosto — usa ``toolkit inspect config``."""
    _inspect_config(
        config_path=config,
        layer=layer,
        mode=mode,
        year=year,
        sql=sql,
        limit=limit,
        mart_index=mart_index,
        diff=False,
        json_output=json_output,
    )


def register(app: typer.Typer) -> None:
    app.command("layer", hidden=True)(layer_cmd)
