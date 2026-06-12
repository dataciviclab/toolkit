"""inspect profile — profilo diagnostico del RAW (encoding, delim, colonne).

run_profile() è usata da inspect/profile e da test_pipeline_integration.
csv_preview() è pubblica cosicché MCP csv_preview la chiami invece di
avere logica inline — CLI = logica, MCP = wrapper.
"""

from __future__ import annotations

import json
from logging import Logger
from pathlib import Path
from typing import Any

import typer

from lab_connectors.duckdb import safe_connect

from toolkit.cli.common import dump_cfg_section, iter_selected_years, load_cfg_and_logger
from toolkit.core.artifacts import should_write
from toolkit.core.csv_read import csv_read_option_strings, robust_preset
from toolkit.core.sql_utils import sql_str
from toolkit.core.paths import layer_year_dir
from toolkit.core.config import ToolkitConfig
from toolkit.profile.raw import (
    profile_raw,
    profile_with_read_cfg,
    sniff_source_file,
    write_raw_profile,
    write_suggested_read_yml,
)


def csv_preview(csv_path: str, limit: int = 20) -> dict[str, Any]:
    """Sniffa encoding/delim/colonne di un CSV e restituisce schema + preview.

    Stessa pipeline di ``sniff_source_file`` + ``profile_with_read_cfg``
    usata dal profiler RAW e da ``toolkit scout --scaffold``.

    Output compatibile col formato ``mapping_suggestions`` del profiler.

    Args:
        csv_path: Path assoluto o relativo al CWD.
        limit: Righe massime in preview.

    Returns:
        Dict con path, column_count, columns, row_count_estimate, preview,
        mapping_suggestions, delim_suggested, encoding_suggested,
        decimal_suggested, skip_suggested, robust_read_suggested.
    """
    path = Path(csv_path)
    sniff_hints = sniff_source_file(path)
    enc = sniff_hints["encoding_suggested"]
    delim = sniff_hints["delim_suggested"]
    dec = sniff_hints["decimal_suggested"]
    skip_n = sniff_hints["skip_suggested"]

    effective_read_cfg = {
        "encoding": enc,
        "delim": delim,
        "decimal": dec,
        "skip": skip_n,
        "header": True,
    }

    runtime_result = profile_with_read_cfg(path, sniff_hints, effective_read_cfg)
    mapping_suggestions = runtime_result["mapping_suggestions"]
    robust_read_suggested = runtime_result["robust_read_suggested"]

    if robust_read_suggested:
        preview_cfg = robust_preset(effective_read_cfg)
        preview_cfg.setdefault("auto_detect", False)
    else:
        preview_cfg = effective_read_cfg

    read_opts = csv_read_option_strings(preview_cfg, include_header_skip=True)
    opt_sql = f"union_by_name=true, {', '.join(read_opts)}"

    with safe_connect() as conn:
        conn.execute(
            f"CREATE VIEW csv_preview AS SELECT * FROM read_csv('{sql_str(str(path))}', {opt_sql})"
        )
        describe_rows = conn.execute("DESCRIBE csv_preview").fetchall()
        col_names = [str(row[0]) for row in describe_rows]
        duckdb_type_map = {str(row[0]): str(row[1]) for row in describe_rows}

        columns_info = [
            {"name": name, "inferred_type": dtype}
            for name, dtype in zip(col_names, [duckdb_type_map[c] for c in col_names])
        ]

        count_result = conn.execute(
            f"SELECT COUNT(*) FROM read_csv('{sql_str(str(path))}', {opt_sql})"
        ).fetchone()
        row_count_estimate = int(count_result[0]) if count_result else None

        preview_rows = conn.execute(f"SELECT * FROM csv_preview LIMIT {int(limit)}").fetchall()
        preview = [dict(zip(col_names, row)) for row in preview_rows]

    return {
        "path": str(path),
        "column_count": len(columns_info),
        "columns": columns_info,
        "row_count_estimate": row_count_estimate,
        "preview": preview,
        "mapping_suggestions": mapping_suggestions,
        "delim_suggested": delim,
        "encoding_suggested": enc,
        "decimal_suggested": dec,
        "skip_suggested": skip_n,
        "robust_read_suggested": robust_read_suggested,
    }


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

        if should_write("profile", "suggested_read", cfg):
            written_paths.append(write_suggested_read_yml(out_dir, prof.__dict__))

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

    Con --config: analizza il raw layer del dataset e scrive raw_profile.json.
    Con --csv-path: sniffa direttamente un file CSV e stampa schema + preview.

    Esempi:
        toolkit inspect profile -c dataset.yml
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
