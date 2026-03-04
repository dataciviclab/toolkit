from __future__ import annotations

import json
from pathlib import Path

import typer

from toolkit.core.config import load_config
from toolkit.core.paths import layer_year_dir
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
    except Exception:
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


def status(
    dataset: str = typer.Option(..., "--dataset", help="Dataset name"),
    year: int = typer.Option(..., "--year", help="Dataset year"),
    run_id: str | None = typer.Option(None, "--run-id", help="Specific run id"),
    latest: bool = typer.Option(False, "--latest", help="Show latest run"),
    config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
    strict_config: bool = typer.Option(False, "--strict-config", help="Treat deprecated config forms as errors"),
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

    if record.get("status") == "FAILED" and record.get("error"):
        typer.echo("")
        typer.echo(f"error: {record.get('error')}")


def register(app: typer.Typer) -> None:
    app.command("status")(status)
