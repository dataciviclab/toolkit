"""CLI per operazioni raw su file: describe, query.

Richiama la stessa logica di ``toolkit/mcp/raw_ops.py``.
"""

from __future__ import annotations

import json

import typer

from toolkit.mcp.raw_ops import describe, query

raw_app = typer.Typer(no_args_is_help=True, help="Operazioni raw su file (describe, query).")


@raw_app.command("describe")
def raw_describe(
    file_path: str = typer.Argument(..., help="Path al file: locale, gs://bucket/key o URL HTTPS."),
    json_output: bool = typer.Option(False, "--json", help="Output in formato JSON."),
) -> None:
    """DESCRIBE + row count di un file raw (parquet/CSV/JSON/Excel)."""
    result = describe(file_path)
    if json_output:
        print(json.dumps(result, indent=2))
    else:
        print(f"File: {result['file']}")
        print(f"Righe: {result['row_count']}")
        print(f"Colonne: {len(result['columns'])}")
        for col in result["columns"]:
            print(f"  {col['name']}: {col['type']}")


@raw_app.command("query")
def raw_query(
    sql: str = typer.Argument(..., help="Query SQL SELECT o WITH."),
    max_rows: int = typer.Option(100, "--max-rows", "-n", help="Massimo righe da ritornare."),
    json_output: bool = typer.Option(False, "--json", help="Output in formato JSON."),
) -> None:
    """Esegue una SELECT su file raw via DuckDB."""
    result = query(sql, max_rows=max_rows)
    if json_output:
        print(json.dumps(result, indent=2))
    else:
        print(f"Righe: {result['row_count']}" + (" (troncate)" if result["truncated"] else ""))
        print(f"Colonne: {result['columns']}")
        for row in result["rows"]:
            print("  " + "\t".join(str(v) if v is not None else "NULL" for v in row))


def register(parent: typer.Typer) -> None:
    parent.add_typer(raw_app, name="raw")
