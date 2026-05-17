"""CLI command: toolkit batch — esegue piu' config in sequenza.

Con --validate esegue anche la validazione post-step (rispetta lo step).
Con --years filtra gli anni da processare.
"""

from __future__ import annotations

from pathlib import Path
from time import perf_counter

import typer

from toolkit.cli.common import iter_selected_years, load_cfg_and_logger
from toolkit.cli.cmd_run import run_cross_year_step, run_year
from toolkit.clean.validate import run_clean_validation
from toolkit.mart.validate import run_mart_validation
from toolkit.raw.validate import run_raw_validation

_ALLOWED_STEPS = {"raw", "clean", "mart", "cross_year", "all"}

# Layernames che ogni step valida. L'ordine non conta.
_STEP_LAYERS: dict[str, tuple[str, ...]] = {
    "raw": ("raw",),
    "clean": ("clean",),
    "mart": ("mart",),
    "all": ("raw", "clean", "mart"),
    "cross_year": ("cross_year",),
}


def _read_config_list(configs_file: Path) -> list[Path]:
    if not configs_file.exists():
        raise FileNotFoundError(f"Config list not found: {configs_file}")

    config_paths: list[Path] = []
    for raw_line in configs_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        config_path = Path(line)
        if not config_path.is_absolute():
            config_path = (configs_file.parent / config_path).resolve()
        config_paths.append(config_path)

    if not config_paths:
        raise ValueError(f"No config paths found in {configs_file}")

    return config_paths


def _format_years(years: list[int] | None) -> str:
    if not years:
        return "-"
    return ",".join(str(year) for year in years)


def _format_duration(seconds: float | None) -> str:
    if seconds is None:
        return "-"
    return f"{seconds:.3f}s"


def _print_table(rows: list[dict[str, str]]) -> None:
    headers = ["dataset", "years", "step", "status", "validate", "duration"]
    widths = {header: len(header) for header in headers}
    for row in rows:
        for header in headers:
            widths[header] = max(widths[header], len(str(row.get(header, ""))))

    def _render(row: dict[str, str]) -> str:
        return "  ".join(str(row.get(header, "")).ljust(widths[header]) for header in headers)

    typer.echo("Batch Report")
    typer.echo(_render({header: header for header in headers}))
    typer.echo("  ".join("-" * widths[header] for header in headers))
    for row in rows:
        typer.echo(_render(row))


def _validate_layers(cfg, year: int, layers: tuple[str, ...], logger=None) -> str:
    """Run validation for the given layers only. Returns 'passed' or 'failed'.

    layers e' una tupla come _STEP_LAYERS[step].
    """
    from toolkit.core.logging import get_logger
    log = logger or get_logger()
    checks: list[bool | None] = []
    for layer in layers:
        if layer == "raw":
            checks.append(run_raw_validation(cfg.root, cfg.dataset, year, log).get("passed"))
        elif layer == "clean":
            checks.append(run_clean_validation(cfg, year, log).get("passed"))
        elif layer == "mart":
            checks.append(run_mart_validation(cfg, year, log).get("passed"))
    return "passed" if all(checks) else "failed"


def batch(
    configs: str = typer.Option(
        ..., "--configs", help="Path to a text file with one dataset.yml path per line"
    ),
    step: str = typer.Option("all", "--step", help="raw | clean | mart | cross_year | all"),
    years: str | None = typer.Option(None, "--years", help="Comma-separated dataset years"),
    validate: bool = typer.Option(False, "--validate", help="Run validation after each step"),
    strict_config: bool = typer.Option(
        False, "--strict-config", help="Treat deprecated config forms as errors"
    ),
):
    """
    Esegue più config in sequenza e stampa un report aggregato finale.

    --validate esegue la validazione solo per i layer corrispondenti allo step
    (raw per --step raw, clean per --step clean, tutti per --step all).

    --years filtra gli anni dichiarati in ogni config.

    Esempio (sostituisce run_support_datasets.py):
        toolkit batch --configs support_list.txt --step all --validate --years 2023,2024
    """
    if step not in _ALLOWED_STEPS:
        raise typer.BadParameter("step must be one of: raw, clean, mart, cross_year, all")

    configs_file = Path(configs)
    config_paths = _read_config_list(configs_file)
    strict_config_flag = strict_config if isinstance(strict_config, bool) else False
    years_val = years if isinstance(years, str) else None
    validate_layers = _STEP_LAYERS[step]

    rows: list[dict[str, str]] = []
    failures: list[dict[str, str]] = []

    for config_path in config_paths:
        config_started_at = perf_counter()
        dataset_label = config_path.stem
        try:
            cfg, logger = load_cfg_and_logger(str(config_path), strict_config=strict_config_flag)
            dataset_label = cfg.dataset
            selected_years = iter_selected_years(cfg, years_arg=years_val)

            if step == "cross_year":
                run_started_at = perf_counter()
                status = "FAILED"
                validate_status = "-"
                try:
                    run_cross_year_step(cfg, logger=logger)
                    status = "SUCCESS"
                    if validate:
                        validate_status = _validate_layers(
                            cfg, selected_years[0], validate_layers, logger
                        ) if selected_years else "-"
                        if validate_status == "failed":
                            failures.append(
                                {
                                    "config": str(config_path),
                                    "dataset": dataset_label,
                                    "years": _format_years(selected_years),
                                    "error": "validation failed",
                                }
                            )
                except Exception as exc:
                    failures.append(
                        {
                            "config": str(config_path),
                            "dataset": dataset_label,
                            "years": _format_years(selected_years),
                            "error": str(exc),
                        }
                    )
                finally:
                    rows.append(
                        {
                            "dataset": dataset_label,
                            "years": _format_years(selected_years),
                            "step": step,
                            "status": status,
                            "validate": validate_status,
                            "duration": _format_duration(perf_counter() - run_started_at),
                        }
                    )
            else:
                for year in selected_years:
                    run_started_at = perf_counter()
                    status = "FAILED"
                    validate_status = "-"
                    try:
                        context = run_year(cfg, year, step=step, logger=logger)
                        status = context.status
                    except Exception as exc:
                        failures.append(
                            {
                                "config": str(config_path),
                                "dataset": dataset_label,
                                "years": str(year),
                                "error": str(exc),
                            }
                        )
                    else:
                        if validate and status == "SUCCESS":
                            try:
                                validate_status = _validate_layers(cfg, year, validate_layers, logger)
                                if validate_status == "failed":
                                    failures.append(
                                        {
                                            "config": str(config_path),
                                            "dataset": dataset_label,
                                            "years": str(year),
                                            "error": "validation failed",
                                        }
                                    )
                            except Exception as exc:
                                validate_status = "failed"
                                failures.append(
                                    {
                                        "config": str(config_path),
                                        "dataset": dataset_label,
                                        "years": str(year),
                                        "error": f"validate: {exc}",
                                    }
                                )
                    finally:
                        rows.append(
                            {
                                "dataset": dataset_label,
                                "years": str(year),
                                "step": step,
                                "status": status,
                                "validate": validate_status,
                                "duration": _format_duration(perf_counter() - run_started_at),
                            }
                        )
        except Exception as exc:
            failures.append(
                {
                    "config": str(config_path),
                    "dataset": dataset_label,
                    "years": "-",
                    "error": str(exc),
                }
            )
            rows.append(
                {
                    "dataset": dataset_label,
                    "years": "-",
                    "step": step,
                    "status": "FAILED",
                    "validate": "-",
                    "duration": _format_duration(perf_counter() - config_started_at),
                }
            )

    _print_table(rows)

    if failures:
        typer.echo("")
        typer.echo("Failures")
        for failure in failures:
            typer.echo(
                f"- config={failure['config']} dataset={failure['dataset']} "
                f"years={failure['years']} error={failure['error']}"
            )
        raise typer.Exit(code=1)


def register(app: typer.Typer) -> None:
    app.command("batch")(batch)
