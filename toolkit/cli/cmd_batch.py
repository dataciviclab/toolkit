from __future__ import annotations

import json
from pathlib import Path
from time import perf_counter
from typing import Any

import typer

from toolkit.cli.common import load_cfg_and_logger
from toolkit.cli.cmd_run import run_year

_ALLOWED_STEPS = {"raw", "clean", "mart", "all"}


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


def _print_table(rows: list[dict[str, str]], headers: list[str]) -> None:
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


def _build_row(
    dataset: str,
    config_path: str,
    years: str,
    step: str,
    status: str,
    duration: str,
) -> dict[str, str]:
    return {
        "dataset": dataset,
        "config": config_path,
        "years": years,
        "step": step,
        "status": status,
        "duration": duration,
    }


def batch(
    configs: str = typer.Option(
        ..., "--configs", help="Path to a text file with one dataset.yml path per line"
    ),
    step: str = typer.Option("all", "--step", help="raw | clean | mart | all"),
    smoke: bool = typer.Option(
        False, "--smoke", help="Alias per --sample-rows 1000 --sample-bytes 1048576"
    ),
    sample_rows: int | None = typer.Option(
        None, "--sample-rows", help="Leggi solo N righe in CLEAN (LIMIT N sul output SQL)"
    ),
    sample_bytes: int | None = typer.Option(
        None,
        "--sample-bytes",
        help="Scarica solo N bytes in RAW (HTTP Range header + troncamento locale)",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Print execution plan without executing"
    ),
    json_output: bool = typer.Option(
        False, "--json", help="Output in formato JSON (machine-readable)"
    ),
    strict_config: bool = typer.Option(
        False, "--strict-config", help="Treat deprecated config forms as errors"
    ),
):
    """
    Esegue più config in sequenza e stampa un report aggregato finale.

    Legge un file di testo con un dataset.yml per riga (righe vuote e commenti
    con # sono ignorati) e li esegue uno dopo l'altro per lo step indicato.
    """
    if step not in _ALLOWED_STEPS:
        raise typer.BadParameter("step must be one of: raw, clean, mart, all")

    dry_flag = dry_run if isinstance(dry_run, bool) else False
    sample_rows_final = 1000 if smoke else sample_rows
    sample_bytes_final = 1048576 if smoke else sample_bytes

    configs_file = Path(configs)
    config_paths = _read_config_list(configs_file)
    strict_config_flag = strict_config if isinstance(strict_config, bool) else False

    rows: list[dict[str, str]] = []
    failures: list[dict[str, str]] = []

    for config_path in config_paths:
        config_started_at = perf_counter()
        dataset_label = config_path.stem

        try:
            if smoke:
                # Carica prima senza override per scoprire cfg.root originale,
                # poi ricarica con root_override a {root}/smoke
                _cfg0, _logger0 = load_cfg_and_logger(
                    str(config_path), strict_config=strict_config_flag
                )
                cfg, logger = load_cfg_and_logger(
                    str(config_path),
                    strict_config=strict_config_flag,
                    root_override=str(_cfg0.root / "smoke"),
                )
            else:
                cfg, logger = load_cfg_and_logger(
                    str(config_path), strict_config=strict_config_flag
                )
            dataset_label = cfg.dataset

            for year in cfg.years:
                run_started_at = perf_counter()
                status = "FAILED"
                try:
                    context = run_year(
                        cfg,
                        year,
                        step=step,
                        dry_run=dry_flag,
                        logger=logger,
                        sample_rows=sample_rows_final,
                        sample_bytes=sample_bytes_final,
                    )
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
                finally:
                    rows.append(
                        _build_row(
                            dataset=dataset_label,
                            config_path=str(config_path),
                            years=str(year),
                            step=step,
                            status=status,
                            duration=_format_duration(perf_counter() - run_started_at),
                        )
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
                _build_row(
                    dataset=dataset_label,
                    config_path=str(config_path),
                    years="-",
                    step=step,
                    status="FAILED",
                    duration=_format_duration(perf_counter() - config_started_at),
                )
            )

    if json_output:
        report: dict[str, Any] = {
            "summary": {
                "total": len(rows),
                "passed": sum(1 for r in rows if r["status"] in ("SUCCESS", "DRY_RUN")),
                "failed": sum(1 for r in rows if r["status"] not in ("SUCCESS", "DRY_RUN")),
                "duration_seconds": sum(
                    float(r["duration"].rstrip("s")) for r in rows if r["duration"] != "-"
                ),
            },
            "rows": rows,
            "failures": failures,
        }
        typer.echo(json.dumps(report, indent=2, default=str))
    else:
        table_headers = ["dataset", "years", "step", "status", "duration"]
        _print_table(rows, table_headers)

        if failures:
            typer.echo("")
            typer.echo("Failures")
            for failure in failures:
                typer.echo(
                    f"- config={failure['config']} dataset={failure['dataset']} "
                    f"years={failure['years']} error={failure['error']}"
                )

    if failures:
        raise typer.Exit(code=1)


def register(app: typer.Typer) -> None:
    app.command("batch")(batch)
