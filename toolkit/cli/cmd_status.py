from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import typer

from toolkit.cli.common import format_profile_preview, load_layer_profile_summaries
from toolkit.core.config import load_config
from toolkit.core.paths import layer_dataset_dir, layer_year_dir
from toolkit.core.run_context import get_run_dir, latest_run, read_run_record


def _layer_row(record: dict[str, object], layer: str) -> str:
    layer_info = (record.get("layers") or {}).get(layer, {})
    validation = (record.get("validations") or {}).get(layer, {})
    validation_passed = validation.get("passed")
    return (
        f"{layer:<5} "
        f"{str(layer_info.get('status', 'PENDING')):<20} "
        f"{str(validation_passed):<17} "
        f"{str(validation.get('errors_count', 0)):<12} "
        f"{str(validation.get('warnings_count', 0)):<14}"
    )


def _read_json(path: Path) -> dict[str, object] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None


def _raw_hints(root: Path, dataset: str, year: int) -> dict[str, object]:
    raw_dir = layer_year_dir(root, "raw", dataset, year)
    raw_manifest = _read_json(raw_dir / "manifest.json") or {}
    raw_metadata = _read_json(raw_dir / "metadata.json") or {}
    profile_hints = raw_metadata.get("profile_hints") or {}
    suggested_read_path = raw_dir / "_profile" / "suggested_read.yml"
    return {
        "primary_output_file": raw_manifest.get("primary_output_file"),
        "suggested_read_exists": suggested_read_path.exists(),
        "suggested_read_path": str(suggested_read_path),
        "encoding": profile_hints.get("encoding_suggested"),
        "delim": profile_hints.get("delim_suggested"),
        "decimal": profile_hints.get("decimal_suggested"),
        "skip": profile_hints.get("skip_suggested"),
        "warnings": profile_hints.get("warnings") or [],
    }


def _layer_artifacts_dir(root: Path, dataset: str, year: int, layer: str) -> Path:
    if layer == "cross_year":
        return layer_dataset_dir(root, "cross", dataset)
    return layer_year_dir(root, layer, dataset, year)


def _validation_counts(
    validation_payload: dict[str, Any] | None,
    manifest_payload: dict[str, Any] | None,
    record_summary: dict[str, Any] | None,
) -> tuple[bool | None, int | None, int | None]:
    if validation_payload is not None:
        return (
            validation_payload.get("ok"),
            len(validation_payload.get("errors") or []),
            len(validation_payload.get("warnings") or []),
        )

    manifest_summary = (manifest_payload or {}).get("summary") or {}
    if manifest_summary:
        return (
            manifest_summary.get("ok"),
            manifest_summary.get("errors_count"),
            manifest_summary.get("warnings_count"),
        )

    record_summary = record_summary or {}
    if record_summary:
        return (
            record_summary.get("passed"),
            record_summary.get("errors_count"),
            record_summary.get("warnings_count"),
        )

    return None, None, None


def _layer_validation_summary(
    root: Path,
    dataset: str,
    year: int,
    layer: str,
    record: dict[str, Any],
) -> dict[str, Any] | None:
    layer_dir = _layer_artifacts_dir(root, dataset, year, layer)
    manifest_payload = _read_json(layer_dir / "manifest.json")
    validation_rel = (manifest_payload or {}).get("validation")
    validation_payload = None
    validation_path = None
    if isinstance(validation_rel, str) and validation_rel.strip():
        validation_path = layer_dir / validation_rel
        validation_payload = _read_json(validation_path)

    record_summary = (record.get("validations") or {}).get(layer, {})
    ok, errors_count, warnings_count = _validation_counts(
        validation_payload,
        manifest_payload,
        record_summary if isinstance(record_summary, dict) else {},
    )

    has_any_data = any(
        [
            manifest_payload is not None,
            validation_payload is not None,
            bool(record_summary),
            layer_dir.exists(),
        ]
    )
    if not has_any_data:
        return None

    warnings = []
    errors = []
    details: list[str] = []
    if validation_payload is not None:
        warnings = [str(item) for item in (validation_payload.get("warnings") or [])]
        errors = [str(item) for item in (validation_payload.get("errors") or [])]

    if validation_path is not None and validation_payload is None:
        details.append(f"validation_missing={validation_path.name}")

    outputs = (manifest_payload or {}).get("outputs") or []
    if isinstance(outputs, list):
        missing_outputs = []
        for entry in outputs:
            if not isinstance(entry, dict):
                continue
            file_name = entry.get("file")
            if isinstance(file_name, str) and file_name and not (layer_dir / file_name).exists():
                missing_outputs.append(file_name)
        if missing_outputs:
            details.append(f"missing_outputs={', '.join(missing_outputs)}")

    summary = (validation_payload or {}).get("summary") or {}
    if layer == "clean":
        required = summary.get("required") or []
        columns = summary.get("columns") or []
        if isinstance(required, list) and isinstance(columns, list):
            missing_columns = [column for column in required if column not in set(columns)]
            if missing_columns:
                details.append(
                    f"missing_columns={', '.join(str(column) for column in missing_columns)}"
                )
    if layer in {"mart", "cross_year"}:
        required_tables = summary.get("required_tables") or []
        tables = summary.get("tables") or []
        if isinstance(required_tables, list) and isinstance(tables, list):
            missing_tables = [table for table in required_tables if table not in set(tables)]
            if missing_tables:
                details.append(
                    f"missing_tables={', '.join(str(table) for table in missing_tables)}"
                )

    if ok is True:
        state = "passed"
    elif ok is False:
        state = "failed"
    elif manifest_payload is not None:
        state = "not_validated"
    else:
        state = "unknown"

    return {
        "layer": layer,
        "state": state,
        "warnings_count": warnings_count,
        "errors_count": errors_count,
        "has_warnings": bool(warnings_count),
        "warning_items": warnings,
        "error_items": errors,
        "details": details,
    }


def _print_validation_summary(
    root: Path,
    dataset: str,
    year: int,
    record: dict[str, Any],
    has_cross_year: bool,
) -> None:
    summaries: list[dict[str, Any]] = []
    for layer in ("clean", "mart"):
        summary = _layer_validation_summary(root, dataset, year, layer, record)
        if summary is not None:
            summaries.append(summary)

    if has_cross_year:
        summary = _layer_validation_summary(root, dataset, year, "cross_year", record)
        if summary is not None:
            summaries.append(summary)

    if not summaries:
        return

    typer.echo("")
    typer.echo("validation_summary:")
    for summary in summaries:
        warnings_count = summary.get("warnings_count")
        errors_count = summary.get("errors_count")
        typer.echo(
            f"  {summary['layer']}: "
            f"state={summary['state']} "
            f"warnings={warnings_count if warnings_count is not None else '?'} "
            f"errors={errors_count if errors_count is not None else '?'}"
        )
        if summary.get("has_warnings"):
            typer.echo("    warnings_present: yes")
        for detail in summary.get("details") or []:
            typer.echo(f"    {detail}")


def _print_layer_profiles(root: Path, dataset: str, year: int) -> None:
    profiles = load_layer_profile_summaries(root, dataset, year)
    if profiles is None:
        return

    typer.echo("")
    typer.echo("layer_profiles:")

    clean_output = profiles.get("clean_output")
    if clean_output is not None:
        typer.echo(f"  clean_output: {format_profile_preview(clean_output)}")

    mart_clean_input = profiles.get("mart_clean_input")
    if mart_clean_input is not None:
        typer.echo(f"  mart_clean_input: {format_profile_preview(mart_clean_input)}")

    mart_tables = profiles.get("mart_tables") or []
    if mart_tables:
        typer.echo("  mart_tables:")
        for table in mart_tables:
            typer.echo(f"    {table['name']}: {format_profile_preview(table)}")

    transitions = profiles.get("clean_to_mart") or []
    if transitions:
        typer.echo("  clean_to_mart:")
        for item in transitions:
            typer.echo(
                f"    {item['target_name']}: "
                f"rows {item['source_row_count']} -> {item['target_row_count']} "
                f"added={len(item['added_columns'])} "
                f"removed={len(item['removed_columns'])} "
                f"type_changes={item['type_change_count']}"
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
    run_dir = get_run_dir(cfg.root, dataset, year)
    record = read_run_record(run_dir, run_id) if run_id else latest_run(run_dir)
    has_cross_year = bool((cfg.cross_year or {}).get("tables"))

    typer.echo(f"dataset: {record.get('dataset')}")
    typer.echo(f"year: {record.get('year')}")
    typer.echo(f"run_id: {record.get('run_id')}")
    typer.echo(f"started_at: {record.get('started_at')}")
    typer.echo(f"status: {record.get('status')}")
    portability = record.get("_portability") or {}
    if not portability.get("portable", True):
        typer.echo("portable: False")
    hints = _raw_hints(Path(cfg.root), dataset, year)
    typer.echo("")
    typer.echo("raw_hints:")
    typer.echo(f"  primary_output_file: {hints['primary_output_file']}")
    typer.echo(f"  suggested_read_exists: {hints['suggested_read_exists']}")
    typer.echo(f"  suggested_read_path: {hints['suggested_read_path']}")
    typer.echo(f"  encoding: {hints['encoding']}")
    typer.echo(f"  delim: {hints['delim']}")
    typer.echo(f"  decimal: {hints['decimal']}")
    typer.echo(f"  skip: {hints['skip']}")
    if hints["warnings"]:
        typer.echo("  warnings:")
        for warning in hints["warnings"]:
            typer.echo(f"    - {warning}")
    typer.echo("")
    typer.echo("layer layer_status         validation_passed errors_count warnings_count")
    for layer in ("raw", "clean", "mart"):
        typer.echo(_layer_row(record, layer))
    _print_validation_summary(Path(cfg.root), dataset, year, record, has_cross_year)
    _print_layer_profiles(Path(cfg.root), dataset, year)

    if record.get("status") == "FAILED" and record.get("error"):
        typer.echo("")
        typer.echo(f"error: {record.get('error')}")


def register(app: typer.Typer) -> None:
    app.command("status")(status)
