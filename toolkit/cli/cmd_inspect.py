from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import typer

from toolkit.cli.common import iter_years
from toolkit.core.config import load_config
from toolkit.core.paths import layer_year_dir
from toolkit.core.run_context import get_run_dir, latest_run


def _clean_output_path(root: Path, dataset: str, year: int) -> Path:
    return layer_year_dir(root, "clean", dataset, year) / f"{dataset}_{year}_clean.parquet"


def _mart_output_paths(root: Path, year_dir: Path, tables: list[dict[str, Any]]) -> list[Path]:
    return [year_dir / f"{table['name']}.parquet" for table in tables if isinstance(table, dict) and table.get("name")]


def _payload_for_year(cfg, year: int) -> dict[str, Any]:
    root = Path(cfg.root)
    raw_dir = layer_year_dir(root, "raw", cfg.dataset, year)
    clean_dir = layer_year_dir(root, "clean", cfg.dataset, year)
    mart_dir = layer_year_dir(root, "mart", cfg.dataset, year)
    run_dir = get_run_dir(root, cfg.dataset, year)
    mart_tables = cfg.mart.get("tables") or []

    latest_payload: dict[str, Any] | None = None
    try:
        latest_record = latest_run(run_dir)
        latest_payload = {
            "run_id": latest_record.get("run_id"),
            "status": latest_record.get("status"),
            "started_at": latest_record.get("started_at"),
            "path": str(run_dir / f"{latest_record.get('run_id')}.json"),
        }
    except FileNotFoundError:
        latest_payload = None

    return {
        "dataset": cfg.dataset,
        "year": year,
        "config_path": str(cfg.base_dir / "dataset.yml"),
        "root": str(root),
        "paths": {
            "raw_dir": str(raw_dir),
            "clean_dir": str(clean_dir),
            "clean_output": str(_clean_output_path(root, cfg.dataset, year)),
            "mart_dir": str(mart_dir),
            "mart_outputs": [str(path) for path in _mart_output_paths(root, mart_dir, mart_tables)],
            "run_dir": str(run_dir),
        },
        "latest_run": latest_payload,
    }


def paths(
    config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
    year: int | None = typer.Option(None, "--year", help="Dataset year"),
    as_json: bool = typer.Option(False, "--json", help="Emit JSON output for notebooks/scripts"),
    strict_config: bool = typer.Option(False, "--strict-config", help="Treat deprecated config forms as errors"),
):
    """
    Mostra i path stabili di output e l'ultimo run record per dataset/year.
    """
    strict_config_flag = strict_config if isinstance(strict_config, bool) else False
    cfg = load_config(config, strict_config=strict_config_flag)
    years = iter_years(cfg, year)
    payload = [_payload_for_year(cfg, selected_year) for selected_year in years]

    if as_json:
        typer.echo(json.dumps(payload if len(payload) > 1 else payload[0], indent=2, ensure_ascii=False))
        return

    for item in payload:
        typer.echo(f"dataset: {item['dataset']}")
        typer.echo(f"year: {item['year']}")
        typer.echo(f"config_path: {item['config_path']}")
        typer.echo(f"root: {item['root']}")
        typer.echo(f"raw_dir: {item['paths']['raw_dir']}")
        typer.echo(f"clean_dir: {item['paths']['clean_dir']}")
        typer.echo(f"clean_output: {item['paths']['clean_output']}")
        typer.echo(f"mart_dir: {item['paths']['mart_dir']}")
        typer.echo("mart_outputs:")
        for output in item["paths"]["mart_outputs"]:
            typer.echo(f"  - {output}")
        typer.echo(f"run_dir: {item['paths']['run_dir']}")
        latest_info = item.get("latest_run")
        if latest_info is None:
            typer.echo("latest_run: none")
        else:
            typer.echo(f"latest_run_id: {latest_info['run_id']}")
            typer.echo(f"latest_run_status: {latest_info['status']}")
            typer.echo(f"latest_run_record: {latest_info['path']}")
        typer.echo("")


def register(app: typer.Typer) -> None:
    inspect_app = typer.Typer(no_args_is_help=True, add_completion=False)
    inspect_app.command("paths")(paths)
    app.add_typer(inspect_app, name="inspect")
