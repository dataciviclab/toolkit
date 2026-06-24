"""inspect runs — cronologia run, dettagli e resume.

Comando canonico per gestire i run.
``inspect runs --resume`` è il percorso canonico per il resume.
La logica di resume è in ``cmd_resume.resume()`` (condivisa).
"""

from __future__ import annotations

import json

import typer

from toolkit.cli.cmd_resume import resume as _resume
from toolkit.core.config import load_config
from toolkit.core.run_records import get_run_dir, read_run_record


def runs(
    config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
    year: int | None = typer.Option(None, "--year", "-y", help="Dataset year (default: first)"),
    resume: bool = typer.Option(False, "--resume", help="Resume latest/failed run"),
    run_id: str | None = typer.Option(None, "--run-id", help="Specific run id (show o resume)"),
    from_layer: str | None = typer.Option(
        None, "--from-layer", help="Force restart from raw | clean | mart (solo con --resume)"
    ),
    limit: int = typer.Option(10, "--limit", help="Max runs da elencare"),
    json_output: bool = typer.Option(False, "--json", help="Output JSON"),
):
    """Mostra cronologia run, dettagli, o riprende un run fallito.

    Con --resume riprende dal primo layer non SUCCESS.
    La logica condivisa è in ``cmd_resume.resume()``.

    Esempi:
        toolkit inspect runs -c dataset.yml                              # elenca ultimi run
        toolkit inspect runs -c dataset.yml --run-id <id>                 # dettaglio specifico
        toolkit inspect runs -c dataset.yml --resume                      # riprendi dal fallimento
        toolkit inspect runs -c dataset.yml --resume --from-layer clean  # forza ripartenza da clean
    """
    # Resume flow → delega alla logica completa di cmd_resume.resume()
    if resume:
        _resume(
            config=config,
            year=year,
            dataset=None,
            run_id=run_id,
            latest=(run_id is None),
            from_layer=from_layer,
        )
        return

    cfg = load_config(config)
    yr = year if year is not None else (cfg.years[0] if cfg.years else None)
    if yr is None:
        raise typer.BadParameter(f"No years configured in {config}")

    run_dir = get_run_dir(cfg.root, cfg.dataset, yr)

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
        rid = f.stem
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
