"""Profilo diagnostico RAW — csv_preview.

``csv_preview()`` e' pubblica cosicché MCP la chiami invece di
avere logica inline.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from lab_connectors.duckdb import safe_connect

from toolkit.core.csv_read import csv_read_option_strings, robust_preset
from toolkit.core.sql_utils import sql_str
from toolkit.profile.raw import profile_with_read_cfg, sniff_source_file


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
