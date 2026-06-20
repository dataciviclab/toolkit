"""CLI command: toolkit query

Esegue query SQL su parquet. Due modalità:
  - path diretto: toolkit query path/to/data.parquet [--sql "..."]
  - da config:   toolkit query -c dataset.yml -l clean [--sql "..."] [--year 2023]

Usage:
    toolkit query path/to/file.parquet [--limit 20] [--json]
    toolkit query path/to/file.parquet --sql "SELECT * FROM data WHERE anno > 2020"
    toolkit query -c dataset.yml -l clean [--sql "..."] [--year 2023] [--limit 20] [--json]
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import typer

from toolkit.core.config import load_config
from toolkit.core.duckdb_shape import parquet_preview


def _resolve_path_from_config(config_path: str, layer: str, year: int | None) -> Path:
    """Risolve il parquet clean o mart da dataset.yml + layer + year.

    Returns:
        Path al file parquet.

    Raises:
        FileNotFoundError: se il parquet non esiste su disco o non è configurato.
        ValueError: se layer non è 'clean' o 'mart'.
    """
    from toolkit.cli.inspect._helpers import _payload_for_year

    cfg = load_config(config_path)
    if year is None:
        year = max(cfg.years) if cfg.years else 0
    paths = _payload_for_year(cfg, year)

    if layer == "clean":
        parquet_str = paths["paths"]["clean"].get("output")
        if not parquet_str:
            raise FileNotFoundError(f"Nessun output clean configurato per {cfg.dataset}/{year}")
        parquet_path = Path(parquet_str)
    elif layer == "mart":
        outputs = paths["paths"]["mart"].get("outputs") or []
        if not outputs:
            raise FileNotFoundError(f"Nessun output mart configurato per {cfg.dataset}/{year}")
        parquet_path = Path(outputs[0])
    else:
        raise ValueError(f"layer deve essere 'clean' o 'mart', non '{layer}'")

    if not parquet_path.exists():
        raise FileNotFoundError(
            f"Parquet {layer} non trovato: {parquet_path}\n"
            f"  Esegui 'toolkit run all -c {config_path}' per generarlo."
        )
    return parquet_path


def _render_human(result: dict[str, Any]) -> None:
    """Stampa output query in formato leggibile."""
    path_label = result.get("path", "")
    row_count = result.get("row_count")
    col_count = result.get("column_count", 0)
    sql_used = result.get("sql")
    preview = result.get("preview", [])
    columns = result.get("columns", [])

    # Header
    dataset_label = result.get("dataset") or ""
    year_label = result.get("year") or ""
    parts = [f"path: {path_label}"]
    if dataset_label:
        parts.append(f"dataset: {dataset_label}")
    if year_label:
        parts.append(f"year: {year_label}")
    typer.echo("  ".join(parts))
    if sql_used:
        typer.echo(f"sql: {sql_used[:120]}{'...' if len(sql_used) > 120 else ''}")

    # Schema
    typer.echo(f"colonne: {col_count}")
    if row_count is not None:
        typer.echo(f"righe: {row_count}")
    else:
        typer.echo("righe: ?")
    typer.echo("")

    for col in columns:
        typer.echo(f"  {col.get('name', '?'):30s} {col.get('type', '?')}")

    typer.echo("")
    if not preview:
        typer.echo("(nessuna riga)")
        return

    # Tabella: calcola larghezza colonne
    col_names = [c["name"] for c in columns]
    # Pre-calcola larghezze minime
    widths = {name: len(name) for name in col_names}
    for row in preview:
        for name in col_names:
            val = row.get(name)
            str_val = str(val) if val is not None else ""
            widths[name] = max(widths[name], len(str_val))

    # Header row
    header = "  ".join(f"{name:{widths[name]}s}" for name in col_names)
    typer.echo(header)
    typer.echo("-" * len(header))

    # Data rows
    for row in preview:
        vals = []
        for name in col_names:
            val = row.get(name)
            str_val = str(val) if val is not None else "NULL"
            vals.append(f"{str_val:{widths[name]}s}")
        typer.echo("  ".join(vals))

    truncated = result.get("truncated", False)
    if truncated:
        typer.echo(f"\n(... mostra prime {len(preview)} righe su {row_count})")


def query(
    path_arg: str = typer.Argument("", metavar="PATH", help="Path al file parquet (posizionale)"),
    config: str = typer.Option(
        None, "--config", "-c", help="Path a dataset.yml (alternativo a PATH)"
    ),
    layer: str = typer.Option("clean", "--layer", "-l", help="Layer: clean o mart (solo con -c)"),
    year: int = typer.Option(
        0, "--year", "-y", help="Anno (default: ultimo del config, solo con -c)"
    ),
    sql: str = typer.Option(None, "--sql", help="SQL query (default: SELECT * LIMIT N)"),
    limit: int = typer.Option(20, "--limit", help="Max righe in output"),
    json_output: bool = typer.Option(False, "--json", help="Output JSON"),
) -> None:
    """Esegue una query SQL su un file parquet.

    Il parquet puo' essere specificato come argomento posizionale (PATH)
    o risolto da dataset.yml con --config / -c.

    La query SQL (--sql) puo' referenziare il parquet come tabella 'data'.
    Esempi: "SELECT * FROM data", "SELECT COUNT(*) FROM data WHERE ...".

    Se --sql non e' fornito, mostra le prime N righe (equivalente a SELECT *).
    """
    # Risolvi il path del parquet
    try:
        if config:
            # Da dataset.yml
            resolved_config = str(Path(config).resolve())
            parquet_path = _resolve_path_from_config(resolved_config, layer, year or None)
        elif path_arg:
            if path_arg.startswith("s3://"):
                # Path GCS: non esiste localmente, non usare Path.resolve()
                parquet_path = Path(path_arg)
            else:
                parquet_path = Path(path_arg).resolve()
                if not parquet_path.exists():
                    typer.echo(f"error: file non trovato: {parquet_path}", err=True)
                    raise typer.Exit(code=1)
        else:
            typer.echo("error: specificare un path parquet o --config", err=True)
            raise typer.Exit(code=1)
    except (FileNotFoundError, ValueError) as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1)

    # Esegui query
    try:
        result = parquet_preview(parquet_path, limit=limit, sql=sql)
    except Exception as exc:
        typer.echo(f"error: {type(exc).__name__}: {exc}", err=True)
        raise typer.Exit(code=1)

    if json_output:
        typer.echo(json.dumps(result, indent=2, ensure_ascii=False, default=str))
        return

    _render_human(result)


def register(app: typer.Typer) -> None:
    app.command("query", hidden=True)(query)
