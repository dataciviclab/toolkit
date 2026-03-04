from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import typer

from toolkit.cli.common import iter_years
from toolkit.core.config import load_config
from toolkit.core.paths import layer_year_dir
from toolkit.core.run_context import get_run_dir, latest_run


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _raw_output_paths(root: Path, dataset: str, year: int) -> dict[str, str]:
    raw_dir = layer_year_dir(root, "raw", dataset, year)
    return {
        "dir": str(raw_dir),
        "manifest": str(raw_dir / "manifest.json"),
        "metadata": str(raw_dir / "metadata.json"),
        "validation": str(raw_dir / "raw_validation.json"),
    }


def _clean_output_path(root: Path, dataset: str, year: int) -> Path:
    return layer_year_dir(root, "clean", dataset, year) / f"{dataset}_{year}_clean.parquet"


def _clean_paths(root: Path, dataset: str, year: int) -> dict[str, str]:
    clean_dir = layer_year_dir(root, "clean", dataset, year)
    return {
        "dir": str(clean_dir),
        "output": str(_clean_output_path(root, dataset, year)),
        "manifest": str(clean_dir / "manifest.json"),
        "metadata": str(clean_dir / "metadata.json"),
        "validation": str(clean_dir / "_validate" / "clean_validation.json"),
    }


def _mart_output_paths(root: Path, year_dir: Path, tables: list[dict[str, Any]]) -> list[Path]:
    return [year_dir / f"{table['name']}.parquet" for table in tables if isinstance(table, dict) and table.get("name")]


def _mart_paths(root: Path, dataset: str, year: int, tables: list[dict[str, Any]]) -> dict[str, Any]:
    mart_dir = layer_year_dir(root, "mart", dataset, year)
    return {
        "dir": str(mart_dir),
        "outputs": [str(path) for path in _mart_output_paths(root, mart_dir, tables)],
        "manifest": str(mart_dir / "manifest.json"),
        "metadata": str(mart_dir / "metadata.json"),
        "validation": str(mart_dir / "_validate" / "mart_validation.json"),
    }


def _payload_for_year(cfg, year: int) -> dict[str, Any]:
    root = Path(cfg.root)
    run_dir = get_run_dir(root, cfg.dataset, year)
    mart_tables = cfg.mart.get("tables") or []
    raw_dir = layer_year_dir(root, "raw", cfg.dataset, year)
    raw_manifest = _read_json(raw_dir / "manifest.json") or {}
    raw_metadata = _read_json(raw_dir / "metadata.json") or {}
    suggested_read_path = raw_dir / "_profile" / "suggested_read.yml"
    profile_hints = raw_metadata.get("profile_hints") or {}

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
            "raw": _raw_output_paths(root, cfg.dataset, year),
            "clean": _clean_paths(root, cfg.dataset, year),
            "mart": _mart_paths(root, cfg.dataset, year, mart_tables),
            "run_dir": str(run_dir),
        },
        "raw_hints": {
            "primary_output_file": raw_manifest.get("primary_output_file"),
            "suggested_read_path": str(suggested_read_path),
            "suggested_read_exists": suggested_read_path.exists(),
            "encoding": profile_hints.get("encoding_suggested"),
            "delim": profile_hints.get("delim_suggested"),
            "decimal": profile_hints.get("decimal_suggested"),
            "skip": profile_hints.get("skip_suggested"),
            "warnings": profile_hints.get("warnings") or [],
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
        typer.echo(f"raw_dir: {item['paths']['raw']['dir']}")
        typer.echo(f"raw_manifest: {item['paths']['raw']['manifest']}")
        typer.echo(f"raw_metadata: {item['paths']['raw']['metadata']}")
        typer.echo(f"raw_validation: {item['paths']['raw']['validation']}")
        typer.echo("raw_hints:")
        typer.echo(f"  - primary_output_file: {item['raw_hints']['primary_output_file']}")
        typer.echo(f"  - suggested_read_exists: {item['raw_hints']['suggested_read_exists']}")
        typer.echo(f"  - suggested_read_path: {item['raw_hints']['suggested_read_path']}")
        typer.echo(f"  - encoding: {item['raw_hints']['encoding']}")
        typer.echo(f"  - delim: {item['raw_hints']['delim']}")
        typer.echo(f"  - decimal: {item['raw_hints']['decimal']}")
        typer.echo(f"  - skip: {item['raw_hints']['skip']}")
        if item["raw_hints"]["warnings"]:
            typer.echo("  - warnings:")
            for warning in item["raw_hints"]["warnings"]:
                typer.echo(f"    - {warning}")
        typer.echo(f"clean_dir: {item['paths']['clean']['dir']}")
        typer.echo(f"clean_output: {item['paths']['clean']['output']}")
        typer.echo(f"clean_manifest: {item['paths']['clean']['manifest']}")
        typer.echo(f"clean_metadata: {item['paths']['clean']['metadata']}")
        typer.echo(f"clean_validation: {item['paths']['clean']['validation']}")
        typer.echo(f"mart_dir: {item['paths']['mart']['dir']}")
        typer.echo("mart_outputs:")
        for output in item["paths"]["mart"]["outputs"]:
            typer.echo(f"  - {output}")
        typer.echo(f"mart_manifest: {item['paths']['mart']['manifest']}")
        typer.echo(f"mart_metadata: {item['paths']['mart']['metadata']}")
        typer.echo(f"mart_validation: {item['paths']['mart']['validation']}")
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
