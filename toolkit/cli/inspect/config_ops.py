"""inspect config — schema, preview, profile, SQL e diff di un dataset.

Comando canonico per ispezionare un dataset.
Sostituisce i vecchi alias ``toolkit layer``, ``inspect schema``/profile/query.
"""

from __future__ import annotations

import json

import typer

from toolkit.core.discovery import resolve_config_path
from toolkit.domain.layer import layer_query
from toolkit.domain.schema_diff import schema_diff_payload


def _resolve_config(config_path: str | None) -> str | None:
    """Risolve config_path con auto-detect, restituisce None solo se esplicitamente omesso."""
    if config_path is None:
        try:
            resolved = resolve_config_path(hint=None)
            return str(resolved)
        except FileNotFoundError:
            return None
    return config_path


def config(
    config_path: str | None = typer.Option(None, "--config", "-c", help="Path o slug dataset.yml"),
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
    diff: bool = typer.Option(False, "--diff", help="Schema-diff RAW tra anni"),
    json_output: bool = typer.Option(False, "--json", help="Output JSON"),
) -> None:
    """Ispeziona configurazione e dati di un dataset: schema, preview, profile, SQL o diff.

    Sostituisce i vecchi alias (``toolkit layer``, ``inspect schema``, ``inspect profile``,
    ``query``, ``inspect schema-diff``) in un unico comando con flag ``--mode`` e ``--diff``.

    Esempi:
        toolkit inspect config -c dataset.yml                           # colonne + tipi (default)
        toolkit inspect config -c dataset.yml -l raw -m profile         # encoding/delimiter
        toolkit inspect config -c dataset.yml -l clean -m preview       # prime righe
        toolkit inspect config -c dataset.yml -l clean -m sql --sql "SELECT count(*) FROM data"
        toolkit inspect config -c dataset.yml --diff                    # schema-diff raw
    """
    # Auto-detect se --config non passato
    config_path = _resolve_config(config_path)

    if diff:
        try:
            payload = schema_diff_payload(config_path)
        except (ValueError, FileNotFoundError) as exc:
            typer.echo(json.dumps({"error": str(exc)}, indent=2))
            raise typer.Exit(code=1)

        if json_output:
            typer.echo(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
        else:
            typer.echo(f"dataset: {payload['dataset']}")
            typer.echo(f"config_path: {payload['config_path']}")
            typer.echo(f"years: {', '.join(str(y) for y in payload['years'])}")
            typer.echo("")
            for entry in payload["entries"]:
                typer.echo(f"year: {entry['year']}")
                typer.echo(f"  raw_exists: {entry['raw_exists']}")
                typer.echo(f"  raw_dir: {entry['raw_dir']}")
                typer.echo(f"  primary_output_file: {entry['primary_output_file']}")
                typer.echo(f"  profile_source: {entry['profile_source']}")
                typer.echo(f"  encoding: {entry['encoding']}")
                typer.echo(f"  delim: {entry['delim']}")
                typer.echo(f"  decimal: {entry['decimal']}")
                typer.echo(f"  skip: {entry['skip']}")
                typer.echo(f"  columns_count: {entry['columns_count']}")
                typer.echo(f"  header_line: {entry['header_line']}")
                if entry.get("columns_preview"):
                    typer.echo("  columns_preview:")
                    for col in entry["columns_preview"]:
                        typer.echo(f"    - {col}")
                if entry.get("warnings"):
                    typer.echo("  warnings:")
                    for w in entry["warnings"]:
                        typer.echo(f"    - {w}")
                typer.echo("")
            if payload["comparisons"]:
                typer.echo("comparisons:")
                for c in payload["comparisons"]:
                    typer.echo(f"  {c['from_year']} -> {c['to_year']}:")
                    typer.echo(f"    counts: {c['from_columns_count']} -> {c['to_columns_count']}")
                    typer.echo(f"    changed: {c['changed']}")
                    if c.get("added_columns"):
                        typer.echo("    added_columns:")
                        for col in c["added_columns"]:
                            typer.echo(f"      - {col}")
                    if c.get("removed_columns"):
                        typer.echo("    removed_columns:")
                        for col in c["removed_columns"]:
                            typer.echo(f"      - {col}")
            else:
                typer.echo("comparisons: none")
        return

    try:
        result = layer_query(
            config_path,
            layer=layer,
            mode=mode,
            year=year or None,
            limit=limit,
            sql=sql,
            mart_index=mart_index,
        )
    except (ValueError, FileNotFoundError) as exc:
        if json_output:
            typer.echo(json.dumps({"error": str(exc)}, indent=2))
        else:
            typer.echo(f"Errore: {exc}", err=True)
        raise typer.Exit(code=1)

    if json_output:
        typer.echo(json.dumps(result, indent=2, ensure_ascii=False, default=str))
        return

    # Output human-readable
    typer.echo(f"Dataset: {result.get('dataset', '?')}")
    typer.echo(f"Layer:   {result.get('layer', layer)}")
    typer.echo(f"Anno:    {result.get('year', year or '?')}")

    if mode == "schema":
        columns = result.get("columns", [])
        entries = result.get("entries", [])
        if entries:
            typer.echo(f"Entry count: {result.get('entry_count', len(entries))}")
            for entry in entries:
                typer.echo(f"  Anno {entry.get('year', '?')}:")
                for col in entry.get("columns", []):
                    typer.echo(f"    {col.get('name', '?'):35s} {col.get('type', '?')}")
        else:
            typer.echo(f"Colonne: {len(columns)}")
            for col in columns:
                typer.echo(f"  {col.get('name', '?'):35s} {col.get('type', '?')}")

    elif mode == "profile":
        hints = result.get("read_hints", {})
        typer.echo(f"Encoding: {hints.get('encoding')}")
        typer.echo(f"Delim:    {repr(hints.get('delimiter'))}")
        typer.echo(f"Decimal:  {hints.get('decimal')}")
        typer.echo(f"Skip:     {hints.get('skip')}")
        cols = result.get("columns", {})
        raw_cols = cols.get("raw", [])
        typer.echo(f"Colonne:  {cols.get('count', len(raw_cols))}")
        for c in raw_cols[:12]:
            typer.echo(f"  {c}")
        if len(raw_cols) > 12:
            typer.echo(f"  ... ({len(raw_cols)} totali)")

    elif mode == "preview":
        columns = result.get("columns", [])
        preview = result.get("preview", [])
        row_count = result.get("row_count")
        if row_count is not None:
            typer.echo(f"Righe: {row_count}")
        typer.echo(f"Colonne: {len(columns)}")
        for col in columns:
            typer.echo(f"  {col.get('name', '?'):35s} {col.get('type', '?')}")
        typer.echo("")
        if preview:
            col_names = [c["name"] for c in columns]
            widths = {n: len(n) for n in col_names}
            for row in preview:
                for n in col_names:
                    v = row.get(n)
                    widths[n] = max(widths[n], len(str(v) if v is not None else ""))
            header = "  ".join(f"{n:{widths[n]}s}" for n in col_names)
            typer.echo(header)
            typer.echo("-" * len(header))
            for row in preview:
                vals = []
                for n in col_names:
                    v = row.get(n)
                    vals.append(f"{str(v) if v is not None else 'NULL':{widths[n]}s}")
                typer.echo("  ".join(vals))

    elif mode == "sql":
        columns = result.get("columns", [])
        preview = result.get("preview", [])
        row_count = result.get("row_count")
        sql_used = result.get("sql", sql)
        if sql_used:
            typer.echo(f"SQL: {sql_used[:120]}")
        if row_count is not None:
            typer.echo(f"Righe: {row_count}")
        typer.echo(f"Colonne: {len(columns)}")
        for col in columns:
            typer.echo(f"  {col.get('name', '?'):35s} {col.get('type', '?')}")
        typer.echo("")
        if preview:
            for row in preview:
                typer.echo(str(row))
