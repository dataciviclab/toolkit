"""SQL execution and DuckDB export for the clean layer.

Provides:
- _run_sql: execute clean SQL against raw input files and export to Parquet
- _normalize_output_profile: normalize output profile to dict format
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from lab_connectors.duckdb import safe_connect

from toolkit.core.duckdb_read import read_raw_to_relation
from toolkit.core.layer_profile import profile_relation
from toolkit.core.sql_utils import sql_path


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
    sample_rows: int | None = None,
) -> tuple[str, dict[str, Any], dict[str, Any]]:
    """Execute clean SQL against raw inputs and export result to Parquet.

    If ``sample_rows`` is set, appends ``LIMIT N`` to the SQL query per
    DuckDB syntax (``SELECT * FROM ({query}) AS _smoke LIMIT N``).

    Returns:
        tuple of (source, params_used, output_profile)
    """
    with safe_connect() as con:
        read_info = read_raw_to_relation(con, input_files, read_cfg, read_mode, logger)
        if sample_rows is not None:
            # Strip trailing semicolons: clean.sql spesso termina con ;
            stripped = sql_query.rstrip().rstrip(";").rstrip()
            sql_query = f"SELECT * FROM ({stripped}) AS _smoke_sample LIMIT {int(sample_rows)}"
        con.execute(f"CREATE TABLE clean_out AS {sql_query}")
        output_profile = profile_relation(con, "clean_out")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        con.execute(
            f"COPY clean_out TO '{sql_path(output_path)}' (FORMAT PARQUET);"
        )
        return read_info.source, read_info.params_used, output_profile
