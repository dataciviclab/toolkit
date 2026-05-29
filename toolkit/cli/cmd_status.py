from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import typer

from toolkit.cli.common import format_profile_preview, load_layer_profile_summaries
from toolkit.core.config import load_config
from toolkit.core.paths import METADATA, RAW_PROFILE_DIR, RAW_SUGGESTED_READ, layer_dataset_dir
from toolkit.core.run_records import get_run_dir, read_run_record
from toolkit.cli.inspect.readiness_ops import summary as _summary


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
        typer.echo(f"  {w}")


def _print_validation_summaries(layers: dict[str, Any]) -> None:
    """Stampa validation summaries dai dati di summary + validation JSON diretto."""
    printed = False
    for layer_name in ("clean", "mart"):
        val = (layers.get(layer_name) or {}).get("validation")
        if not val:
            continue

        state = "passed" if val.get("ok") else ("failed" if val.get("ok") is False else "?")
        errs = val.get("errors_count", 0)
        warns = val.get("warnings_count", 0)

        if not printed:
            typer.echo("")
            typer.echo("validation_summary:")
            printed = True

        typer.echo(f"  {layer_name}: state={state} warnings={warns} errors={errs}")

        # Dettagli extra letti dal validation JSON su disco
        layer_dir = (layers.get(layer_name) or {}).get("dir")
        if layer_dir:
            vpath = Path(layer_dir) / f"_validate/{layer_name}_validation.json"
            if vpath.exists():
                _print_validation_details(layer_name, vpath)


def _print_validation_details(layer_name: str, vpath: Path) -> None:
    """Legge validation JSON e stampa dettagli extra (missing columns, tables, ecc.)."""
    try:
        content = json.loads(vpath.read_text(encoding="utf-8"))
    except Exception:
        return
    summary = content.get("summary") or {}
    details: list[str] = []

    if layer_name == "clean":
        required = summary.get("required") or []
        columns = summary.get("columns") or []
        missing_cols = [c for c in required if c not in set(columns)] if isinstance(required, list) and isinstance(columns, list) else []
        if missing_cols:
            details.append(f"missing_columns={','.join(str(c) for c in missing_cols)}")

    if layer_name in ("mart",):
        required_tables = summary.get("required_tables") or []
        tables = summary.get("tables") or []
        missing_tables = [t for t in required_tables if t not in set(tables)] if isinstance(required_tables, list) and isinstance(tables, list) else []
        if missing_tables:
            details.append(f"missing_tables={','.join(str(t) for t in missing_tables)}")

    # missing outputs
    outputs = content.get("outputs") or content.get("sections", {}).get("outputs") or []
    if isinstance(outputs, list):
        missing = [o.get("file") for o in outputs if isinstance(o, dict) and not (Path(vpath).parent.parent / o.get("file", "")).exists()]
        if missing:
            details.append(f"missing_outputs={','.join(str(m) for m in missing)}")

    if details:
        typer.echo(f"    {' '.join(details)}")


def _print_layer_profiles(dataset: str, year: int, layers: dict[str, Any]) -> None:
    """Stampa layer profiles dalle metadata JSON."""
    # Cerca il path del clean da summary; fallback alla dir se output non esiste
    clean_layer = layers.get("clean", {})
    clean_output = clean_layer.get("output")
    if clean_output and Path(clean_output).exists():
        root = Path(clean_output).parents[3]  # risali da out/data/clean/{slug}/{year}/
    else:
        clean_dir = clean_layer.get("dir")
        if not clean_dir or not Path(clean_dir).exists():
            return
        root = Path(clean_dir).parents[3]
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
    mart_tables: list[dict[str, Any]] = profiles.get("mart_tables") or []  # type: ignore[assignment]
    if mart_tables:
        typer.echo("  mart_tables:")
        for table in mart_tables:
            if isinstance(table, dict):
                typer.echo(f"    {table.get('name', '?')}: {format_profile_preview(table)}")
    transitions: list[dict[str, Any]] = profiles.get("clean_to_mart") or []  # type: ignore[assignment]
    if transitions:
        typer.echo("  clean_to_mart:")
        for item in transitions:
            if isinstance(item, dict):
                typer.echo(
                    f"    {item.get('target_name', '?')}: "
                    f"rows {item.get('source_row_count', '?')} -> {item.get('target_row_count', '?')} "
                    f"added={len(item.get('added_columns', []))} "
                    f"removed={len(item.get('removed_columns', []))} "
                    f"type_changes={item.get('type_change_count', '?')}"
                )


def status(
    dataset: str = typer.Option(..., "--dataset", help="Dataset name"),
    year: int = typer.Option(..., "--year", help="Dataset year"),
    run_id: str | None = typer.Option(None, "--run-id", help="Specific run id"),
    latest: bool = typer.Option(False, "--latest", help="Show latest run"),
    config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
):
    """
    Mostra lo stato dell'ultimo run o di uno specifico run_id.
    """
    if run_id and latest:
        raise typer.BadParameter("Use either --run-id or --latest, not both")

    cfg = load_config(config)

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
    raw_layer = layers.get("raw") or {}
    raw_hints = {
        "primary_output_file": raw_layer.get("primary_output_file"),
        "suggested_read_exists": raw_layer.get("suggested_read_exists"),
        "suggested_read_path": None,
        "encoding": raw_layer.get("encoding_suggested"),
        "delim": raw_layer.get("delim_suggested"),
        "decimal": raw_layer.get("decimal_suggested"),
        "skip": raw_layer.get("skip_suggested"),
        "warnings": raw_layer.get("raw_warnings", []),
    }
    # suggested_read_path non è in summary — lo ricostruiamo
    raw_dir = (layers.get("raw") or {}).get("dir")
    if raw_dir and raw_hints.get("suggested_read_exists"):
        raw_hints["suggested_read_path"] = str(Path(raw_dir) / RAW_PROFILE_DIR / RAW_SUGGESTED_READ)
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

    # multi-year mart (ex cross_year)
    multi_year_tables = [t for t in cfg.mart.tables if t.years]
    if multi_year_tables:
        my_dir = layer_dataset_dir(cfg.root, "mart", dataset)
        my_meta = my_dir / METADATA
        if my_meta.exists():
            content = json.loads(my_meta.read_text(encoding="utf-8"))
            my_layer = content.get("layer", "")
            if my_layer == "mart_multi_year":
                my_tables = content.get("tables") or []
                typer.echo("")
                typer.echo(f"  multi_year_mart: {len(my_tables)} table(s)")
                for t in my_tables:
                    typer.echo(f"    - {t.get('name', '?')} years={t.get('years', [])}")

    _print_layer_profiles(dataset, year, layers)

    if record.get("status") == "FAILED" and record.get("error"):
        typer.echo("")
        typer.echo(f"error: {record.get('error')}")


def register(app: typer.Typer) -> None:
    app.command("status")(status)
