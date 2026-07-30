"""Helper functions for batch execution — formattazione, silenziamento log."""

from __future__ import annotations

import contextlib
import logging
from typing import Any

import typer


def format_years(years: list[int] | None) -> str:
    """Formatta lista anni in stringa compatta."""
    if not years:
        return "-"
    return ",".join(str(year) for year in years)


def format_duration(seconds: float | None) -> str:
    """Formatta durata in secondi con 3 decimali."""
    if seconds is None:
        return "-"
    return f"{seconds:.3f}s"


def print_table(rows: list[dict[str, str]], headers: list[str]) -> None:
    """Stampa tabella batch report su stdout."""
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


def build_row(
    dataset: str,
    config_path: str,
    years: str,
    step: str,
    status: str,
    duration: str,
) -> dict[str, str]:
    """Costruisce una riga per il report batch."""
    return {
        "dataset": dataset,
        "config": config_path,
        "years": years,
        "step": step,
        "status": status,
        "duration": duration,
    }


@contextlib.contextmanager
def silence_typer_echo() -> Any:
    """Silenzia typer.echo durante run_year quando --json è attivo."""
    original_echo = typer.echo
    typer.echo = lambda *args, **kwargs: None
    try:
        yield
    finally:
        typer.echo = original_echo


def silence_logger() -> None:
    """Silenzia il logger 'toolkit' per output JSON pulito su stdout."""
    lg = logging.getLogger("toolkit")
    lg.setLevel(logging.CRITICAL + 1)
    lg.handlers.clear()
    lg.addHandler(logging.NullHandler())
    lg.propagate = False
