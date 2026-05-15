from __future__ import annotations

from pathlib import Path
from typing import Any

import typer

from toolkit.cli.common import format_profile_preview, load_layer_profile_summaries
from toolkit.core.config import load_config
from toolkit.core.run_context import get_run_dir, read_run_record
from toolkit.mcp.schema_ops import summary as _summary


def _print_raw_hints(hints: dict[str, Any]) -> None:
    """Stampa i raw_hints dalla struttura di summary."""
    typer.echo("")
    typer.echo("raw_hints:")
    typer.echo(f"  primary_output_file: {hints.get('primary_output_file')}")
    typer.echo(f"  suggested_read_exists: {hints.get('suggested_read_exists')}")
    sr_path = hints.get("suggested_read_path")
    if sr_path:
        typer.echo(f"  suggested_read_path: {sr_path}")
    typer.echo(f"  encoding: {hints.get('encoding')}")
    typer.echo(f"  delim: {hints.get('delim')}")
    typer.echo(f"  decimal: {hints.get('decimal')}")
    typer.echo(f"  skip: {hints.get('skip')}")
    for w in hints.get("warnings") or []:
        typer.echo(f"  warning: {w}")


def _print_validation_summaries(layers: dict[str, Any]) -> None:
    """Stampa validation summaries dai dati di summary."""
    printed = False
    for layer_name in ("clean", "mart"):
        layer = layers.get(layer_name, {})
        val = layer.get("validation")
        if not val:
            continue
        if not printed:
            typer.echo("")
            typer.echo("validation_summary:")
            printed = True
        typer.echo(
            f"  {layer_name}: "
            f"ok={val.get('ok')} "
            f"errors={val.get('errors_count', 0)} "
            f"warnings={val.get('warnings_count', 0)}"
        )


def _print_layer_profiles(dataset: str, year: int, layers: dict[str, Any]) -> None:
    """Stampa layer profiles dalle metadata JSON."""
    # Cerca il path del clean da summary
    clean_layer = layers.get("clean", {})
    clean_output = clean_layer.get("output")
    if not clean_output or not Path(clean_output).exists():
        return

    # Legge i profili dalla funzione condivisa
    root = Path(clean_output).parents[3]  # risali da out/data/clean/{slug}/{year}/
    profiles = load_layer_profile_summaries(root, dataset, year)
    if profiles is None:
        return

    typer.echo("")
    typer.echo("layer_profiles:")
    clean_out = profiles.get("clean_output")
    if isinstance(clean_out, dict):
        typer.echo(f"  clean_output: {format_profile_preview(clean_out)}")
    mart_clean_input = profiles.get("mart_clean_input")
    if isinstance(mart_clean_input, dict):
        typer.echo(f"  mart_clean_input: {format_profile_preview(mart_clean_input)}")
    for table in (profiles.get("mart_tables") or []):
        if isinstance(table, dict):
            typer.echo(f"  mart_table {table.get('name', '?')}: {format_profile_preview(table)}")
    for item in (profiles.get("clean_to_mart") or []):
        if isinstance(item, dict):
            typer.echo(
                f"  clean_to_mart {item.get('target_name', '?')}: "
                f"rows {item.get('source_row_count', '?')} -> {item.get('target_row_count', '?')}"
            )


def status(
    dataset: str = typer.Option(..., "--dataset", help="Dataset name"),
    year: int = typer.Option(..., "--year", help="Dataset year"),
    run_id: str | None = typer.Option(None, "--run-id", help="Specific run id"),
    latest: bool = typer.Option(False, "--latest", help="Show latest run"),
    config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
    strict_config: bool = typer.Option(
        False, "--strict-config", help="Treat deprecated config forms as errors"
    ),
):
    """
    Mostra lo stato dell'ultimo run o di uno specifico run_id.
    """
    if run_id and latest:
        raise typer.BadParameter("Use either --run-id or --latest, not both")

    strict_config_flag = strict_config if isinstance(strict_config, bool) else False
    cfg = load_config(config, strict_config=strict_config_flag)

    # Usa summary() per i dati centralizzati
    s = _summary(config, year or None)
    record = (s.get("run") or {}).get("latest_run_record") or {}

    # Se run_id specifico, carica quel record invece del latest
    if run_id:
        run_dir = get_run_dir(cfg.root, dataset, year)
        specific = read_run_record(run_dir, run_id)
        if specific:
            record = specific

    layers = s.get("layers", {})

    # Layer run status: da summary di default, dal record specifico se --run-id
    if run_id:
        layer_run_statuses: dict[str, dict[str, Any]] = {}
        for layer_name in ("raw", "clean", "mart"):
            li = (record.get("layers") or {}).get(layer_name, {})
            lv = (record.get("validations") or {}).get(layer_name, {})
            layer_run_statuses[layer_name] = {
                "status": li.get("status", "PENDING"),
                "validation_passed": lv.get("passed"),
                "validation_errors": lv.get("errors_count", 0),
                "validation_warnings": lv.get("warnings_count", 0),
            }
    else:
        layer_run_statuses = {
            name: (layers.get(name) or {}).get("run_status") or {}
            for name in ("raw", "clean", "mart")
        }

    typer.echo(f"dataset: {record.get('dataset', dataset)}")
    typer.echo(f"year: {record.get('year', year)}")
    typer.echo(f"run_id: {record.get('run_id')}")
    typer.echo(f"started_at: {record.get('started_at')}")
    typer.echo(f"status: {record.get('status')}")

    # Raw hints da summary + inspect_paths per suggested_read_path
    raw_hints = {
        "primary_output_file": (layers.get("raw") or {}).get("primary_output_file"),
        "suggested_read_exists": (layers.get("raw") or {}).get("suggested_read_exists"),
        "suggested_read_path": None,
        "encoding": (layers.get("raw") or {}).get("encoding_suggested"),
        "delim": (layers.get("raw") or {}).get("delim_suggested"),
        "decimal": (layers.get("raw") or {}).get("decimal_suggested"),
    }
    # suggested_read_path non è in summary — lo ricostruiamo
    raw_dir = (layers.get("raw") or {}).get("dir")
    if raw_dir and raw_hints.get("suggested_read_exists"):
        raw_hints["suggested_read_path"] = str(Path(raw_dir) / "_profile" / "suggested_read.yml")
    _print_raw_hints(raw_hints)

    # Layer table
    typer.echo("")
    typer.echo("layer layer_status         validation_passed errors_count warnings_count")
    for layer_name in ("raw", "clean", "mart"):
        rs = layer_run_statuses.get(layer_name) or {}
        typer.echo(
            f"{layer_name:<5} "
            f"{str(rs.get('status', 'PENDING')):<20} "
            f"{str(rs.get('validation_passed')):<17} "
            f"{str(rs.get('validation_errors', 0)):<12} "
            f"{str(rs.get('validation_warnings', 0)):<14}"
        )

    _print_validation_summaries(layers)
    _print_layer_profiles(dataset, year, layers)

    if record.get("status") == "FAILED" and record.get("error"):
        typer.echo("")
        typer.echo(f"error: {record.get('error')}")


def register(app: typer.Typer) -> None:
    app.command("status")(status)
