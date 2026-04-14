from __future__ import annotations

from pathlib import Path
from time import perf_counter

import typer

from toolkit.cli.common import load_cfg_and_logger
from toolkit.cli.cmd_run import run_cross_year_step, run_year

_ALLOWED_STEPS = {"raw", "clean", "mart", "cross_year", "all"}


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
    headers = ["dataset", "years", "step", "status", "duration"]
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


def batch(
    configs: str = typer.Option(
        ..., "--configs", help="Path to a text file with one dataset.yml path per line"
    ),
    step: str = typer.Option("all", "--step", help="raw | clean | mart | cross_year | all"),
    strict_config: bool = typer.Option(
        False, "--strict-config", help="Treat deprecated config forms as errors"
    ),
):
    """
    Esegue più config in sequenza e stampa un report aggregato finale.
    """
    if step not in _ALLOWED_STEPS:
        raise typer.BadParameter("step must be one of: raw, clean, mart, cross_year, all")

    configs_file = Path(configs)
    config_paths = _read_config_list(configs_file)
    strict_config_flag = strict_config if isinstance(strict_config, bool) else False

    rows: list[dict[str, str]] = []
    failures: list[dict[str, str]] = []

    for config_path in config_paths:
        config_started_at = perf_counter()
        dataset_label = config_path.stem
        try:
            cfg, logger = load_cfg_and_logger(str(config_path), strict_config=strict_config_flag)
            dataset_label = cfg.dataset

            if step == "cross_year":
                run_started_at = perf_counter()
                status = "FAILED"
                try:
                    run_cross_year_step(cfg, logger=logger)
                    status = "SUCCESS"
                except Exception as exc:
                    failures.append(
                        {
                            "config": str(config_path),
                            "dataset": dataset_label,
                            "years": _format_years(list(cfg.years)),
                            "error": str(exc),
                        }
                    )
                finally:
                    rows.append(
                        {
                            "dataset": dataset_label,
                            "years": _format_years(list(cfg.years)),
                            "step": step,
                            "status": status,
                            "duration": _format_duration(perf_counter() - run_started_at),
                        }
                    )
            else:
                for year in cfg.years:
                    run_started_at = perf_counter()
                    status = "FAILED"
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
                    finally:
                        rows.append(
                            {
                                "dataset": dataset_label,
                                "years": str(year),
                                "step": step,
                                "status": status,
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
