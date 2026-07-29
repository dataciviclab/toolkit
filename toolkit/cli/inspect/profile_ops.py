"""inspect profile — profilo diagnostico del RAW (encoding, delim, colonne).

``csv_preview`` e' in ``toolkit.domain.profile``.
``run_profile`` (logica) e ``profile`` (CLI Typer) restano qui.
"""

from __future__ import annotations

import json
from logging import Logger
from pathlib import Path
from typing import Any

import typer

from toolkit.cli.common import dump_cfg_section, load_cfg_and_logger
from toolkit.domain.common import iter_selected_years
from toolkit.domain.profile import csv_preview
from toolkit.core.paths import layer_year_dir
from toolkit.core.config import ToolkitConfig
from toolkit.profile.raw import profile_raw, write_raw_profile


def run_profile(cfg: ToolkitConfig, years: list[int], logger: Logger) -> None:
    """Core logic: profiling RAW per ogni anno e scrittura su _profile/.

    Chiamabile sia da inspect/profile che da cmd_profile (deprecato).
    """
    clean_cfg: dict[str, Any] = dump_cfg_section(cfg.clean) or {}

    for y in years:
        raw_dir = layer_year_dir(cfg.root, "raw", cfg.dataset, y)
        out_dir = raw_dir / "_profile"
        out_dir.mkdir(parents=True, exist_ok=True)

        prof = profile_raw(raw_dir, cfg.dataset, y, read_cfg=clean_cfg.get("read"))
        paths = write_raw_profile(out_dir, prof)
        written_paths = list(paths.values())

        if written_paths:
            logger.info("PROFILE RAW -> %s", " | ".join(str(path) for path in written_paths))
        else:
            logger.info("PROFILE RAW -> no optional artifacts written for current policy")


def profile(
    config: str = typer.Option(None, "--config", "-c", help="Path to dataset.yml"),
    csv_path: str | None = typer.Option(
        None, "--csv-path", help="CSV file to preview (instead of --config)"
    ),
    year: int | None = typer.Option(None, "--year", "-y", help="Single dataset year"),
    years: str | None = typer.Option(None, "--years", help="Comma-separated dataset years"),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON output"),
):
    """
    Profilo diagnostico del RAW: encoding, delimitatore, colonne.

    Con --config: analizza il raw layer del dataset (stessa funzionalità di
    ``toolkit inspect config -c CONFIG -l raw -m profile``) e scrive raw_profile.json.
    Con --csv-path: sniffa direttamente un file CSV e stampa schema + preview
    (funzionalità esclusiva, non disponibile in inspect config).

    Esempi:
        toolkit inspect profile -c dataset.yml -l raw -m profile
        toolkit inspect profile --csv-path data/file.csv --json
    """
    if csv_path:
        if not Path(csv_path).exists():
            raise typer.BadParameter(f"File non trovato: {csv_path}")
        result = csv_preview(csv_path)
        if json_output:
            typer.echo(json.dumps(result, indent=2, default=str))
        else:
            typer.echo(f"File:    {result['path']}")
            typer.echo(f"Encoding: {result['encoding_suggested']}")
            typer.echo(f"Delim:   {repr(result['delim_suggested'])}")
            typer.echo(f"Decimal: {result['decimal_suggested']}")
            typer.echo(f"Skip:    {result['skip_suggested']}")
            typer.echo(f"Colonne: {result['column_count']}")
            typer.echo(f"Righe:   {result['row_count_estimate']}")
            if result["columns"]:
                typer.echo("")
                for c in result["columns"][:12]:
                    typer.echo(f"  {c['name']:40s} {c['inferred_type']}")
                if len(result["columns"]) > 12:
                    typer.echo(f"  ... ({len(result['columns'])} totali)")
        return

    if not config:
        raise typer.BadParameter("Serve --config o --csv-path")

    year_val = year if isinstance(year, int) else None
    years_val = years if isinstance(years, str) else None
    cfg, logger = load_cfg_and_logger(config)
    selected_years = iter_selected_years(cfg, year_arg=year_val, years_arg=years_val)
    run_profile(cfg, selected_years, logger)
