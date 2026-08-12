"""toolkit column-values — profilo valori delle colonne dimensionali.

Per un dataset clean, genera un JSON con gli aggregati di valore di ogni
colonna dimensionale: cardinalita', null, top-N valori con percentuale.

Prova locale (branch feat/column-values-profile): output in
``<workspace>/_local/generated/column_values/`` o con ``--out``.
"""

from __future__ import annotations

import json
from pathlib import Path

import typer

from toolkit.core.config import load_config
from toolkit.core.io import write_json_atomic
from toolkit.core.paths import WORKSPACE_ROOT
from toolkit.domain.column_values import (
    build_column_values_profile,
    generate_workspace_column_values,
)
from toolkit.domain.path_resolver import payload_for_year
from toolkit.registry.schema_reader import (
    load_semantic_types,
    parquet_columns,
)


def column_values(
    config: str = typer.Option(None, "--config", "-c", help="Path or slug to dataset.yml"),
    year: int | None = typer.Option(None, "--year", "-y", help="Dataset year"),
    top: int = typer.Option(20, "--top", help="Max top values per column"),
    out: str | None = typer.Option(
        None, "--out", help="Output dir (default: <workspace>/_local/generated/column_values/)"
    ),
    all_datasets: bool = typer.Option(
        False, "--all", help="Genera i profili per tutti i dataset clean locali del workspace"
    ),
    json_output: bool = typer.Option(False, "--json", help="Print JSON to stdout"),
):
    """Profilo valori delle colonne dimensionali di un dataset clean.

    Esempi:
        toolkit column-values -c dataset-incubator/candidates/camera-votazioni-sparql
        toolkit column-values -c camera_votazioni_sparql --year 2024 --top 10
        toolkit column-values --all
    """
    if all_datasets:
        if config:
            raise typer.BadParameter("--all non si combina con --config")
        out_dir = (
            Path(out).resolve()
            if out
            else WORKSPACE_ROOT / "_local" / "generated" / "column_values"
        )
        result = generate_workspace_column_values(
            WORKSPACE_ROOT,
            out_dir,
            top_n=top,
            latest_year_only=True,
            log=lambda msg: typer.echo(msg),
        )
        typer.echo(
            f"BATCH: processed={result.processed} written={result.written} "
            f"skipped={len(result.skipped)} errors={len(result.errors)} "
            f"in {result.duration_seconds}s"
        )
        if result.errors:
            typer.echo("ERRORI:", err=True)
            for e in result.errors[:20]:
                typer.echo(f"  {e}", err=True)
        return

    if not config:
        raise typer.BadParameter("Serve --config (path o slug a dataset.yml)")

    cfg = load_config(config)
    if year is None:
        year = max(cfg.years) if cfg.years else 0

    payload = payload_for_year(cfg, year)
    clean_output = (payload.get("paths") or {}).get("clean", {}).get("output")
    if not clean_output:
        raise typer.BadParameter(f"Nessun output clean configurato per {cfg.dataset}/{year}")
    parquet = Path(clean_output)
    if not parquet.is_file():
        typer.echo(f"ERRORE: parquet clean non trovato: {parquet}", err=True)
        raise typer.Exit(code=1)

    columns = parquet_columns(parquet, load_semantic_types())
    if not columns:
        typer.echo(f"ERRORE: nessuno schema leggibile da {parquet}", err=True)
        raise typer.Exit(code=1)

    profile = build_column_values_profile(parquet, columns, top_n=top)
    profile.update(
        {
            "schema_version": 1,
            "dataset": cfg.dataset,
            "year": year,
            "config_path": str(cfg.base_dir / "dataset.yml"),
            "parquet": str(parquet),
        }
    )

    if json_output:
        typer.echo(json.dumps(profile, indent=2, ensure_ascii=False, default=str))
        return

    n_rows = profile.get("n_rows")
    typer.echo(
        f"{cfg.dataset}/{year} — {profile['n_columns_profiled']} colonne dimensionali "
        f"(righe: {n_rows})"
    )
    for name, entry in profile["columns"].items():
        top_preview = ", ".join(f"{tv['value']}={tv['n']}" for tv in entry["top_values"][:3])
        suffix = "..." if entry["top_truncated"] else ""
        typer.echo(
            f"  {name}: distinct={entry['n_distinct']} null={entry['n_null']} "
            f"[{top_preview}{suffix}]"
        )

    out_dir = (
        Path(out).resolve() if out else WORKSPACE_ROOT / "_local" / "generated" / "column_values"
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{cfg.dataset}__column_values.json"
    write_json_atomic(out_path, profile)
    typer.echo(f"scritto {out_path}")


def register(app: typer.Typer) -> None:
    app.command("column-values")(column_values)
