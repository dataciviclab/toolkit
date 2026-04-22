"""SQL execution and DuckDB export for the clean layer.

Provides:
- _run_sql: execute clean SQL against raw input files and export to Parquet
- _normalize_output_profile: normalize output profile to dict format
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb

from toolkit.clean.duckdb_read import read_raw_to_relation, sql_path
from toolkit.core.layer_profile import profile_relation


def _normalize_output_profile(output_profile: dict[str, Any] | int) -> dict[str, Any]:
    if isinstance(output_profile, dict):
        return output_profile
    return {
        "row_count": int(output_profile),
        "columns": [],
    }


def _run_sql(
    input_files: list[Path],
    sql_query: str,
    output_path: Path,
    *,
    read_cfg: dict[str, Any] | None = None,
    read_mode: str = "fallback",
    logger=None,
) -> tuple[str, dict[str, Any], dict[str, Any]]:
    """Execute clean SQL against raw inputs and export result to Parquet.

    Returns:
        tuple of (source, params_used, output_profile)
    """
    con = duckdb.connect(":memory:")
    try:
        read_info = read_raw_to_relation(con, input_files, read_cfg, read_mode, logger)
        con.execute(f"CREATE TABLE clean_out AS {sql_query}")
        output_profile = profile_relation(con, "clean_out")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        con.execute(
            f"COPY clean_out TO '{sql_path(output_path)}' (FORMAT PARQUET);"
        )
        return read_info.source, read_info.params_used, output_profile
    finally:
        con.close()
