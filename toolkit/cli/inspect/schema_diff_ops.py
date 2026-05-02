"""inspect schema-diff command — compare RAW schema signals across years."""

from __future__ import annotations

import json

import typer

from toolkit.cli.common import iter_years
from toolkit.core.config import load_config

from ._helpers import _compare_schema_entries, _raw_schema_payload


def schema_diff(
    config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
    as_json: bool = typer.Option(False, "--json", help="Emit JSON output"),
    strict_config: bool = typer.Option(
        False, "--strict-config", help="Treat deprecated config forms as errors"
    ),
):
    """
    Confronta i principali segnali di schema RAW tra gli anni configurati.
    """
    strict_config_flag = strict_config if isinstance(strict_config, bool) else False
    cfg = load_config(config, strict_config=strict_config_flag)
    entries = [_raw_schema_payload(cfg, selected_year) for selected_year in iter_years(cfg, None)]
    comparisons = _compare_schema_entries(entries)
    payload = {
        "dataset": cfg.dataset,
        "config_path": str(cfg.base_dir / "dataset.yml"),
        "years": [entry["year"] for entry in entries],
        "entries": entries,
        "comparisons": comparisons,
    }

    if as_json:
        typer.echo(json.dumps(payload, indent=2, ensure_ascii=False))
        return

    typer.echo(f"dataset: {payload['dataset']}")
    typer.echo(f"config_path: {payload['config_path']}")
    typer.echo(f"years: {', '.join(str(year) for year in payload['years'])}")
    typer.echo("")

    for entry in entries:
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
        if entry["columns_preview"]:
            typer.echo("  columns_preview:")
            for column in entry["columns_preview"]:
                typer.echo(f"    - {column}")
        if entry["warnings"]:
            typer.echo("  warnings:")
            for warning in entry["warnings"]:
                typer.echo(f"    - {warning}")
        typer.echo("")

    if comparisons:
        typer.echo("comparisons:")
        for comparison in comparisons:
            typer.echo(f"  {comparison['from_year']} -> {comparison['to_year']}:")
            typer.echo(
                f"    counts: {comparison['from_columns_count']} -> {comparison['to_columns_count']}"
            )
            typer.echo(f"    changed: {comparison['changed']}")
            if comparison["added_columns"]:
                typer.echo("    added_columns:")
                for column in comparison["added_columns"]:
                    typer.echo(f"      - {column}")
            if comparison["removed_columns"]:
                typer.echo("    removed_columns:")
                for column in comparison["removed_columns"]:
                    typer.echo(f"      - {column}")
    else:
        typer.echo("comparisons: none")