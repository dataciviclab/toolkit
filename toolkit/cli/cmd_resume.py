from __future__ import annotations

from pathlib import Path

import typer

from toolkit.cli.cmd_run import run_year
from toolkit.core.config import load_config
from toolkit.core.logging import get_logger
from toolkit.core.metadata import read_layer_metadata
from toolkit.core.paths import layer_year_dir
from toolkit.core.run_context import get_run_dir, latest_run, read_run_record


def _artifact_exists(path: Path) -> bool:
    return path.exists() and path.is_file()


_LAYER_ORDER = ("raw", "clean", "mart")


def _resume_layer(record: dict[str, object]) -> str | None:
    layers = record.get("layers") or {}
    for layer in _LAYER_ORDER:
        status = (layers.get(layer) or {}).get("status")
        if status != "SUCCESS":
            return layer
    return None


def _layer_artifacts_ok(root: Path, dataset: str, year: int, layer: str) -> tuple[bool, str]:
    layer_dir = layer_year_dir(root, layer, dataset, year)
    metadata_path = layer_dir / "metadata.json"

    if not _artifact_exists(metadata_path):
        return False, f"missing {layer}/metadata.json"

    meta = read_layer_metadata(layer_dir)

    if layer == "raw":
        primary_output = meta.get("primary_output_file")
        if not isinstance(primary_output, str) or not primary_output:
            return False, "raw metadata missing primary_output_file"
        if not _artifact_exists(layer_dir / primary_output):
            return False, f"missing raw primary output: {primary_output}"
        return True, ""

    if layer == "clean":
        parquet_path = layer_dir / f"{dataset}_{year}_clean.parquet"
        if not _artifact_exists(parquet_path):
            return False, f"missing clean parquet: {parquet_path.name}"
        return True, ""

    if layer == "mart":
        outputs = meta.get("outputs") or []
        if not outputs:
            return False, "mart metadata missing outputs"
        for output in outputs:
            rel_file = (output or {}).get("file")
            if isinstance(rel_file, str) and rel_file and _artifact_exists(layer_dir / rel_file):
                return True, ""
        return False, "missing mart parquet outputs declared in metadata"

    raise ValueError(f"Unsupported layer: {layer}")


def _resolve_resume_start(
    cfg,
    year: int,
    record: dict[str, object],
    *,
    requested_from_layer: str | None = None,
) -> tuple[str | None, list[str]]:
    notes: list[str] = []

    if requested_from_layer is not None:
        start_layer = requested_from_layer
    else:
        start_layer = _resume_layer(record)
        if start_layer is None and record.get("status") == "SUCCESS_WITH_WARNINGS":
            return None, [
                "Run completed with SUCCESS_WITH_WARNINGS. "
                "Use --from-layer raw|clean|mart to force a rerun from a specific layer."
            ]
        if start_layer is None:
            return None, []

    start_index = _LAYER_ORDER.index(start_layer)
    while start_index > 0:
        previous_layer = _LAYER_ORDER[start_index - 1]
        ok, reason = _layer_artifacts_ok(cfg.root, cfg.dataset, year, previous_layer)
        if ok:
            break
        notes.append(
            f"Previous layer '{previous_layer}' is marked SUCCESS in the run record "
            f"but required artifacts are missing ({reason}). Falling back to '{previous_layer}'."
        )
        start_index -= 1

    return _LAYER_ORDER[start_index], notes


def resume(
    dataset: str = typer.Option(..., "--dataset", help="Dataset name"),
    year: int = typer.Option(..., "--year", help="Dataset year"),
    run_id: str | None = typer.Option(None, "--run-id", help="Specific run id"),
    latest: bool = typer.Option(False, "--latest", help="Resume latest run"),
    compat: bool = typer.Option(False, "--compat", help="Allow resume from non-portable legacy run records"),
    from_layer: str | None = typer.Option(None, "--from-layer", help="Force restart from raw | clean | mart"),
    config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
    strict_config: bool = typer.Option(False, "--strict-config", help="Treat deprecated config forms as errors"),
):
    """
    Riprende un run dal primo layer non SUCCESS.
    """
    if run_id and latest:
        raise typer.BadParameter("Use either --run-id or --latest, not both")
    if from_layer is not None and from_layer not in _LAYER_ORDER:
        raise typer.BadParameter("--from-layer must be one of: raw, clean, mart")

    strict_config_flag = strict_config if isinstance(strict_config, bool) else False
    cfg = load_config(config, strict_config=strict_config_flag)
    if cfg.dataset != dataset:
        raise typer.BadParameter(f"Config dataset mismatch: expected {dataset}, found {cfg.dataset}")
    if year not in cfg.years:
        raise typer.BadParameter(f"Year {year} is not configured in {config}")

    run_dir = get_run_dir(cfg.root, dataset, year)
    try:
        record = read_run_record(run_dir, run_id) if run_id else latest_run(run_dir)
    except FileNotFoundError as exc:
        raise typer.BadParameter(str(exc)) from exc

    portability = record.get("_portability") or {}
    if not portability.get("portable", True):
        warning = (
            "Run record contains absolute paths outside the current root and is non-portable. "
            "Use --compat to resume anyway."
        )
        if not compat:
            typer.echo(warning, err=True)
            raise typer.Exit(code=2)
        typer.echo(f"warning: {warning}")

    start_from_layer, notes = _resolve_resume_start(
        cfg,
        year,
        record,
        requested_from_layer=from_layer,
    )
    for note in notes:
        typer.echo(f"warning: {note}")

    if start_from_layer is None:
        if notes:
            typer.echo(notes[-1], err=True)
            raise typer.Exit(code=2)
        typer.echo("Nothing to resume")
        return

    logger = get_logger()
    new_context = run_year(
        cfg,
        year,
        step="all",
        start_from_layer=start_from_layer,
        logger=logger,
        resumed_from=str(record.get("run_id")),
    )
    typer.echo(
        f"Resumed from {record.get('run_id')} starting at {start_from_layer}. New run_id: {new_context.run_id}"
    )


def register(app: typer.Typer) -> None:
    app.command("resume")(resume)
