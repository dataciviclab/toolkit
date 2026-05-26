"""CLI per `inspect schema` — schema di un layer raw/clean/mart.

Implementazione condivisa: sia CLI che MCP la chiamano.
MCP wrappa le eccezioni in ToolkitClientError.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import typer

from toolkit.cli.inspect._helpers import (
    _payload_for_year,
    _raw_schema_payload,
    _schema_from_parquet,
)
from toolkit.core.config import load_config


# --- Implementazione condivisa ---


def show_schema(config_path: str, layer: str = "clean", year: int | None = None) -> dict[str, Any]:
    """Mostra lo schema (colonne + tipi) di raw, clean o mart.

    Args:
        config_path: path al dataset.yml.
        layer: ``"raw"``, ``"clean"`` (default), o ``"mart"``.
        year: anno. Se ``None`` per dataset multi-year usa l'ultimo.

    Returns:
        Dict con schema del layer richiesto.

    Raises:
        ValueError: layer non valido o anno non disponibile.
        FileNotFoundError: file parquet o config non trovati.
    """
    cfg = load_config(config_path, strict_config=False)

    safe_layer = (layer or "clean").strip().lower()
    if safe_layer not in {"raw", "clean", "mart"}:
        raise ValueError(f"layer deve essere uno tra: raw, clean, mart (ricevuto: {layer})")

    if safe_layer == "raw":
        years = list(cfg.years or [])
        entries = [_raw_schema_payload(cfg, yr) for yr in years]
        if year is not None:
            entries = [e for e in entries if e.get("year") == year]
        return {
            "dataset": cfg.dataset,
            "layer": "raw",
            "year": year,
            "entry_count": len(entries),
            "entries": entries,
        }

    _target_year: int = year if year is not None else (max(cfg.years) if cfg.years else 0)
    paths = _payload_for_year(cfg, _target_year)
    if safe_layer == "clean":
        parquet_path_str = paths["paths"]["clean"].get("output")
        if not parquet_path_str:
            raise FileNotFoundError("Nessun output clean configurato")
        parquet_path = Path(parquet_path_str)
        payload = _schema_from_parquet(parquet_path)
    else:
        outputs = paths["paths"]["mart"].get("outputs") or []
        if not outputs:
            raise FileNotFoundError("Nessun output mart risolto dal toolkit")
        parquet_path = Path(outputs[0])
        payload = _schema_from_parquet(parquet_path)
        payload["available_outputs"] = outputs
        if len(outputs) > 1:
            payload["warning"] = (
                "Sono presenti piu' output mart; lo schema mostrato riguarda solo il primo output."
            )

    payload.update({
        "dataset": paths.get("dataset"),
        "year": paths.get("year"),
        "layer": safe_layer,
        "config_path": str(config_path),
    })
    return payload


# --- CLI wrapper (Typer) ---


def schema(
    config_path: str = typer.Argument("", metavar="CONFIG", help="Path al dataset.yml (posizionale)"),
    config: str = typer.Option(None, "--config", "-c", help="Path al dataset.yml", hidden=True),
    layer: str = typer.Option("clean", "--layer", "-l", help="Layer: raw, clean, mart"),
    year: int = typer.Option(0, "--year", "-y", help="Anno (default: ultimo)"),
    json_output: bool = typer.Option(False, "--json", help="Output JSON"),
) -> None:
    """Mostra lo schema (colonne + tipi) di raw, clean o mart.

    Il path config puo' essere passato come argomento posizionale
    (es. toolkit inspect schema path/to/dataset.yml)
    o con l'opzione --config / -c.
    """
    resolved_config = config or config_path
    if not resolved_config:
        typer.echo("error: specificare il path al dataset.yml (argomento o --config)", err=True)
        raise typer.Exit(code=1)

    try:
        result = show_schema(resolved_config, layer, year or None)
    except (ValueError, FileNotFoundError) as exc:
        if json_output:
            typer.echo(json.dumps({"status": "error", "message": str(exc)}, indent=2))
        else:
            typer.echo(f"Errore: {exc}")
        raise typer.Exit(code=1)

    status = "ok" if result.get("columns") or result.get("entries") else "empty"

    if json_output:
        typer.echo(json.dumps(result, indent=2, default=str))
        return

    if status == "error":
        typer.echo(f"Errore: {result.get('message', 'sconosciuto')}")
        raise typer.Exit(code=1)

    columns = result.get("columns", [])
    typer.echo(f"Layer: {result.get('layer', layer)}")
    typer.echo(f"Anno: {result.get('year', year or '?')}")
    typer.echo(f"Colonne: {len(columns)}")
    for col in columns:
        typer.echo(f"  {col.get('name', '?'):30s} {col.get('type', '?')}")
