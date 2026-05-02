"""inspect probe command — probe SPARQL endpoints for schema and statistics."""

from __future__ import annotations

import json

import typer

from toolkit.core.exceptions import DownloadError
from toolkit.plugins.sparql import SparqlSource


def probe(
    source: str = typer.Option(..., "--source", "-s", help="Source type (e.g. sparql)"),
    endpoint: str | None = typer.Option(None, "--endpoint", help="SPARQL endpoint URL"),
    query: str | None = typer.Option(None, "--query", "-q", help="SPARQL SELECT query"),
    timeout: int = typer.Option(60, "--timeout", min=1, help="Timeout in seconds"),
    limit: int = typer.Option(100, "--limit", min=1, help="Max rows for probe sample"),
    as_json: bool = typer.Option(False, "--json", help="Emit JSON output"),
) -> None:
    """
    Probe a source endpoint to infer schema and basic statistics.
    Currently supports SPARQL endpoints.
    """
    if source != "sparql":
        typer.echo(f"error: source '{source}' not supported. Only 'sparql' is available.", err=True)
        raise typer.Exit(code=1)

    if not endpoint:
        typer.echo("error: --endpoint is required for sparql source.", err=True)
        raise typer.Exit(code=1)

    if not query:
        typer.echo("error: --query/-q is required for sparql source.", err=True)
        raise typer.Exit(code=1)

    src = SparqlSource(timeout=timeout)
    try:
        result = src.probe(endpoint, query, limit=limit)
    except DownloadError as exc:
        typer.echo(f"error: {exc}")
        raise typer.Exit(code=1) from exc

    if as_json:
        typer.echo(json.dumps(result, indent=2, ensure_ascii=False))
        return

    typer.echo(f"endpoint: {result['endpoint']}")
    typer.echo(f"query_time_ms: {result['query_time_ms']}")
    typer.echo(f"variables ({len(result['variables'])}): {', '.join(result['variables'])}")
    typer.echo(f"row_count: {result['row_count']}")
    typer.echo("null_counts:")
    for var, count in result["null_counts"].items():
        if count > 0:
            typer.echo(f"  {var}: {count}")
    if not any(c > 0 for c in result["null_counts"].values()):
        typer.echo("  (none)")

    if result["warnings"]:
        typer.echo("warnings:")
        for w in result["warnings"]:
            typer.echo(f"  - {w}")

    if result["sample_rows"]:
        typer.echo("sample_rows:")
        for row in result["sample_rows"]:
            typer.echo(f"  {row}")