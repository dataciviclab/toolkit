"""inspect paths command — show stable output paths and run records."""

from __future__ import annotations

import json

import typer

from toolkit.cli.common import format_profile_preview, iter_years
from toolkit.core.config import load_config

from ._helpers import _payload_for_year


def paths(
    config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
    year: int | None = typer.Option(None, "--year", help="Dataset year"),
    as_json: bool = typer.Option(False, "--json", help="Emit JSON output for notebooks/scripts"),
    strict_config: bool = typer.Option(
        False, "--strict-config", help="Treat deprecated config forms as errors"
    ),
):
    """
    Mostra i path stabili di output e l'ultimo run record per dataset/year.
    """
    strict_config_flag = strict_config if isinstance(strict_config, bool) else False
    cfg = load_config(config, strict_config=strict_config_flag)
    years = iter_years(cfg, year)
    payload = [_payload_for_year(cfg, selected_year) for selected_year in years]

    if as_json:
        typer.echo(
            json.dumps(payload if len(payload) > 1 else payload[0], indent=2, ensure_ascii=False)
        )
        return

    for item in payload:
        typer.echo(f"dataset: {item['dataset']}")
        typer.echo(f"year: {item['year']}")
        typer.echo(f"config_path: {item['config_path']}")
        typer.echo(f"root: {item['root']}")
        typer.echo(f"raw_dir: {item['paths']['raw']['dir']}")
        typer.echo(f"raw_metadata: {item['paths']['raw']['metadata']}")
        typer.echo(f"raw_manifest: {item['paths']['raw']['manifest']}")
        typer.echo(f"raw_validation: {item['paths']['raw']['validation']}")
        typer.echo("raw_hints:")
        typer.echo(f"  - primary_output_file: {item['raw_hints']['primary_output_file']}")
        typer.echo(f"  - suggested_read_exists: {item['raw_hints']['suggested_read_exists']}")
        typer.echo(f"  - suggested_read_path: {item['raw_hints']['suggested_read_path']}")
        typer.echo(f"  - encoding: {item['raw_hints']['encoding']}")
        typer.echo(f"  - delim: {item['raw_hints']['delim']}")
        typer.echo(f"  - decimal: {item['raw_hints']['decimal']}")
        typer.echo(f"  - skip: {item['raw_hints']['skip']}")
        if item["raw_hints"]["warnings"]:
            typer.echo("  - warnings:")
            for warning in item["raw_hints"]["warnings"]:
                typer.echo(f"    - {warning}")
        if item["layer_profiles"]:
            typer.echo("layer_profiles:")
            clean_output = item["layer_profiles"].get("clean_output")
            if clean_output is not None:
                typer.echo(f"  clean_output: {format_profile_preview(clean_output)}")
            mart_clean_input = item["layer_profiles"].get("mart_clean_input")
            if mart_clean_input is not None:
                typer.echo(f"  mart_clean_input: {format_profile_preview(mart_clean_input)}")
            mart_tables = item["layer_profiles"].get("mart_tables") or []
            if mart_tables:
                typer.echo("  mart_tables:")
                for table in mart_tables:
                    typer.echo(f"    {table['name']}: {format_profile_preview(table)}")
            transitions = item["layer_profiles"].get("clean_to_mart") or []
            if transitions:
                typer.echo("  clean_to_mart:")
                for transition in transitions:
                    typer.echo(
                        f"    {transition['target_name']}: "
                        f"rows {transition['source_row_count']} -> {transition['target_row_count']} "
                        f"added={len(transition['added_columns'])} "
                        f"removed={len(transition['removed_columns'])} "
                        f"type_changes={transition['type_change_count']}"
                    )
        typer.echo(f"clean_dir: {item['paths']['clean']['dir']}")
        typer.echo(f"clean_output: {item['paths']['clean']['output']}")
        typer.echo(f"clean_manifest: {item['paths']['clean']['manifest']}")
        typer.echo(f"clean_metadata: {item['paths']['clean']['metadata']}")
        typer.echo(f"clean_validation: {item['paths']['clean']['validation']}")
        typer.echo(f"mart_dir: {item['paths']['mart']['dir']}")
        typer.echo("mart_outputs:")
        for output in item["paths"]["mart"]["outputs"]:
            typer.echo(f"  - {output}")
        typer.echo(f"mart_manifest: {item['paths']['mart']['manifest']}")
        typer.echo(f"mart_metadata: {item['paths']['mart']['metadata']}")
        typer.echo(f"mart_validation: {item['paths']['mart']['validation']}")
        if item["paths"]["support"]:
            typer.echo("support:")
            for support in item["paths"]["support"]:
                typer.echo(f"  - name: {support['name']}")
                typer.echo(f"    dataset: {support['dataset']}")
                typer.echo(f"    config_path: {support['config_path']}")
                typer.echo(
                    f"    years: {', '.join(str(year_value) for year_value in support['years'])}"
                )
                typer.echo(f"    mart: {support['mart']}")
                typer.echo("    outputs:")
                for output in support["outputs"]:
                    typer.echo(f"      - {output}")
        typer.echo(f"run_dir: {item['paths']['run_dir']}")
        latest_info = item.get("latest_run")
        if latest_info is None:
            typer.echo("latest_run: none")
        else:
            typer.echo(f"latest_run_id: {latest_info['run_id']}")
            typer.echo(f"latest_run_status: {latest_info['status']}")
            typer.echo(f"latest_run_record: {latest_info['path']}")
        typer.echo("")