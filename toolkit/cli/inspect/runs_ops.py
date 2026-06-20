"""inspect runs — cronologia run, dettagli e resume.

Comando canonico per gestire i run.
``toolkit resume`` e ``toolkit status --run-id`` sono alias che
delegano a questo comando.
"""

from __future__ import annotations

import json

import typer

from toolkit.cli.cmd_run import run_year
from toolkit.core.config import load_config
from toolkit.core.logging import get_logger
from toolkit.core.run_records import get_run_dir, latest_run, read_run_record

_LAYER_ORDER = ("raw", "clean", "mart")


def _find_resume_layer(record: dict) -> str | None:
    layers_raw = record.get("layers")
    layers = layers_raw if isinstance(layers_raw, dict) else {}
    for layer in _LAYER_ORDER:
        layer_info = layers.get(layer)
        layer_dict = layer_info if isinstance(layer_info, dict) else {}
        status = layer_dict.get("status")
        if status != "SUCCESS":
            return layer
    return None


def runs(
    config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
    year: int | None = typer.Option(None, "--year", "-y", help="Dataset year (default: first)"),
    resume: bool = typer.Option(False, "--resume", help="Resume latest/failed run"),
    run_id: str | None = typer.Option(None, "--run-id", help="Specific run id (show o resume)"),
    limit: int = typer.Option(10, "--limit", help="Max runs da elencare"),
    json_output: bool = typer.Option(False, "--json", help="Output JSON"),
):
    """Mostra cronologia run, dettagli, o riprende un run fallito.

    Con --resume riprende dal primo layer non SUCCESS.
    ``toolkit resume`` e ``toolkit status`` sono alias che delegano qui.

    Esempi:
        toolkit inspect runs -c dataset.yml                  # elenca ultimi run
        toolkit inspect runs -c dataset.yml --run-id <id>    # dettaglio specifico
        toolkit inspect runs -c dataset.yml --resume          # riprendi dal fallimento
    """
    cfg = load_config(config)
    yr = year if year is not None else (cfg.years[0] if cfg.years else None)
    if yr is None:
        raise typer.BadParameter(f"No years configured in {config}")

    run_dir = get_run_dir(cfg.root, cfg.dataset, yr)

    # Resume flow
    if resume:
        if not run_dir.exists():
            typer.echo("Nessun run precedente trovato. Esegui prima 'toolkit run'.", err=True)
            raise typer.Exit(code=1)

        try:
            record = read_run_record(run_dir, run_id) if run_id else latest_run(run_dir)
        except FileNotFoundError as exc:
            typer.echo(str(exc), err=True)
            raise typer.Exit(code=1) from exc

        start_layer = _find_resume_layer(record)
        if start_layer is None:
            typer.echo("Nessun layer da riprendere — run completato.")
            return

        logger = get_logger()
        new_context = run_year(
            cfg,
            yr,
            step="all",
            start_from_layer=start_layer,
            logger=logger,
            resumed_from=str(record.get("run_id")),
        )
        typer.echo(
            f"Ripreso da {record.get('run_id')} a partire da {start_layer}. "
            f"Nuovo run_id: {new_context.run_id}"
        )
        return

    # Show specific run
    if run_id:
        try:
            record = read_run_record(run_dir, run_id)
        except FileNotFoundError as exc:
            typer.echo(str(exc), err=True)
            raise typer.Exit(code=1) from exc

        if json_output:
            typer.echo(json.dumps(record, indent=2, ensure_ascii=False, default=str))
        else:
            layers_info = record.get("layers", {})
            typer.echo(f"run_id:    {record.get('run_id')}")
            typer.echo(f"status:    {record.get('status')}")
            typer.echo(f"dataset:   {record.get('dataset')}")
            typer.echo(f"year:      {record.get('year')}")
            typer.echo(f"started:   {record.get('started_at')}")
            typer.echo(f"completed: {record.get('completed_at')}")
            if record.get("error"):
                typer.echo(f"error:     {record.get('error')}")
            typer.echo("")
            typer.echo("layers:")
            for lname in ("raw", "clean", "mart"):
                li = layers_info.get(lname, {})
                status = li.get("status", "PENDING")
                typer.echo(f"  {lname}: {status}")
        return

    # List recent runs
    if not run_dir.exists():
        typer.echo("Nessun run trovato.")
        return

    run_files = sorted(run_dir.glob("*.json"), reverse=True)[:limit]
    records = []
    for f in run_files:
        rid = f.stem  # filename without .json
        try:
            rec = read_run_record(run_dir, rid)
            records.append(rec)
        except (FileNotFoundError, ValueError):
            continue

    if json_output:
        typer.echo(json.dumps(records, indent=2, ensure_ascii=False, default=str))
        return

    if not records:
        typer.echo("Nessun run trovato.")
        return

    typer.echo(f"Ultimi {len(records)} run per {cfg.dataset}/{yr}:")
    typer.echo("")
    for rec in records:
        rid = (rec.get("run_id") or "?")[:12]
        status = rec.get("status", "?")
        started = (rec.get("started_at") or "?")[:19]
        layers = rec.get("layers", {})
        layer_status = ",".join(
            f"{ln}:{layers.get(ln, {}).get('status', '?')[:4]}" for ln in ("raw", "clean", "mart")
        )
        typer.echo(f"  {rid}  {status:<20} {started}  {layer_status}")
