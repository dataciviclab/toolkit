"""Flag di CLI condivisi tra comandi toolkit.

Centralizza le definizioni di flag per evitare duplicazione e
garantire help text, tipi e default uniformi.
"""

from __future__ import annotations

import typer

# --- Config ---

CONFIG = typer.Option(
    ...,
    "--config",
    "-c",
    help="Path to dataset.yml",
    show_default=False,
)

# --- Years ---

YEAR = typer.Option(
    None,
    "--year",
    "-y",
    help="Single dataset year",
)

YEARS = typer.Option(
    None,
    "--years",
    help="Comma-separated dataset years",
)

# --- Execution modes ---

DRY_RUN = typer.Option(
    False,
    "--dry-run",
    help="Print execution plan without executing",
)

SMOKE = typer.Option(
    False,
    "--smoke",
    help="Alias per --sample-rows 1000 --sample-bytes 1048576",
)

SAMPLE_ROWS = typer.Option(
    None,
    "--sample-rows",
    help="Read only N rows in CLEAN (LIMIT N on output SQL)",
)

SAMPLE_BYTES = typer.Option(
    None,
    "--sample-bytes",
    help="Download only N bytes in RAW (HTTP Range header + local truncation)",
)

ROOT_OVERRIDE = typer.Option(
    None,
    "--root",
    help="Override root output directory (e.g. DCL_ROOT)",
)

# --- Output format ---

JSON = typer.Option(
    False,
    "--json",
    help="Emit JSON output",
)

# --- Step ---

STEP = typer.Option(
    "all",
    "--step",
    help="raw | clean | mart | all",
)
