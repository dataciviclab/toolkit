from __future__ import annotations

import json

import typer

from toolkit.cli.common import iter_selected_years, load_cfg_and_logger
from toolkit.clean.validate import run_clean_validation
from toolkit.core.dataset_loader import validate_config
from toolkit.mart.validate import run_mart_validation
from toolkit.raw.validate import run_raw_validation

_VALIDATORS = {
    "raw": lambda cfg, yr, lg: run_raw_validation(cfg.root, cfg.dataset, yr, lg),
    "clean": run_clean_validation,
    "mart": run_mart_validation,
}


def _validate_config_cmd(config_arg: str, as_json: bool) -> None:
    """Valida dataset.yml — cammini obbligatori, coerenza campi."""
    result = validate_config(config_arg)
    if as_json:
        typer.echo(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        slug = result["slug"]
        if result["errors"]:
            for e in result["errors"]:
                typer.echo(f"🔴 {slug}: {e}")
        for w in result["warnings"]:
            typer.echo(f"🟡 {slug}: {w}")
        if result["ok"]:
            typer.echo(f"✅ {slug}: configurazione valida")
    if not result["ok"]:
        raise typer.Exit(code=1)


def validate(
    step: str = typer.Argument(..., help="raw | clean | mart | all | config"),
    config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
    year: int | None = typer.Option(None, "--year", "-y", help="Single dataset year"),
    years: str | None = typer.Option(None, "--years", help="Comma-separated dataset years"),
    as_json: bool = typer.Option(False, "--json", help="Emit JSON output"),
):
    """
    Quality gate per dataset.yml, RAW, CLEAN e MART.

    - ``config``: valida il file dataset.yml (campi obbligatori, coerenza)
    - ``raw/clean/mart``: valida output della pipeline
    - ``all``: raw + clean + mart
    """
    if step == "config":
        _validate_config_cmd(config, as_json)
        return

    cfg, logger = load_cfg_and_logger(config)
    years_arg = years if isinstance(years, str) else None
    year_arg = year if isinstance(year, int) else None
    selected_years = iter_selected_years(cfg, year_arg=year_arg, years_arg=years_arg)

    # Silenzia logger per output JSON pulito
    if as_json:
        import logging as _logging

        _logging.getLogger("toolkit").setLevel(_logging.CRITICAL + 1)

    layers = ["raw", "clean", "mart"] if step == "all" else [step]
    results: list[dict[str, object]] = []

    for yr in selected_years:
        for layer in layers:
            fn = _VALIDATORS[layer]
            summary = fn(cfg, yr, logger)
            results.append(
                {
                    "year": yr,
                    "layer": layer,
                    "passed": summary.get("passed"),
                    "errors_count": summary.get("errors_count", 0),
                    "warnings_count": summary.get("warnings_count", 0),
                }
            )

    any_failed = any(not r["passed"] for r in results)

    if as_json:
        typer.echo(json.dumps(results, indent=2, ensure_ascii=False))
        if any_failed:
            raise typer.Exit(code=1)
        return
    for r in results:
        icon = "✅" if r["passed"] else "🔴"
        typer.echo(
            f"{icon} {r['year']}/{r['layer']}  errors={r['errors_count']} warnings={r['warnings_count']}"
        )

    if any_failed:
        raise typer.Exit(code=1)


def register(app: typer.Typer) -> None:
    app.command("validate")(validate)
