"""SQL execution and DuckDB export for the clean layer.

Provides:
- _run_sql: execute clean SQL against raw input files and export to Parquet
- _normalize_output_profile: normalize output profile to dict format
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from lab_connectors.duckdb import safe_connect

from toolkit.core.constants import CLEAN_OUT_TABLE
from toolkit.core.duckdb_read import read_raw_to_relation
from toolkit.core.input_file import RawInputFile
from toolkit.core.layer_profile import profile_relation
from toolkit.core.sql_utils import sql_path

_MACROS_PATH = Path(__file__).parent.parent / "sql" / "macros.sql"


def _load_standard_macros(con, logger) -> None:
    """Load the standard toolkit DuckDB macros into the connection.

    Reads ``macros.sql`` from the toolkit package and executes it.
    All macros use ``CREATE OR REPLACE`` so repeated calls are safe.

    Raises:
        FileNotFoundError: se ``macros.sql`` non esiste (toolkit
            installato senza package data o sviluppo incompleto).
    """
    if not _MACROS_PATH.exists():
        raise FileNotFoundError(
            f"Standard macros not found at {_MACROS_PATH}. "
            "Ensure toolkit is installed with sql/*.sql package data "
            "(pip install -e . rebuilds the package)."
        )
    macros_sql = _MACROS_PATH.read_text(encoding="utf-8")
    con.execute(macros_sql)
    if logger:
        logger.debug("Loaded %d bytes of standard macros from %s", len(macros_sql), _MACROS_PATH)


def _normalize_output_profile(output_profile: dict[str, Any] | int) -> dict[str, Any]:
    if isinstance(output_profile, dict):
        return output_profile
    return {
        "row_count": int(output_profile),
        "columns": [],
    }


def _run_sql(
    input_files: list[RawInputFile],
    sql_query: str,
    output_path: Path,
    *,
    read_cfg: dict[str, Any] | None = None,
    read_mode: str = "fallback",
    logger=None,
    sample_rows: int | None = None,
    memory_limit: str | None = None,
) -> tuple[str, dict[str, Any], dict[str, Any]]:
    """Execute clean SQL against raw inputs and export result to Parquet.

    If ``sample_rows`` is set, appends ``LIMIT N`` to the SQL query per
    DuckDB syntax (``SELECT * FROM ({query}) AS _smoke LIMIT N``).

    Standard toolkit macros (``normalize_italian_number``, ``decode_flag``,
    etc.) are automatically loaded at the start of the connection.

    Args:
        memory_limit: Override DuckDB memory_limit (es. "4GB") per dataset
            con join pesanti (da ``duckdb.memory_limit`` del dataset.yml).
            None = default lab (2GB o DUCKDB_MEMORY_LIMIT env).

    Returns:
        tuple of (source, params_used, output_profile)
    """
    duckdb_config = {"memory_limit": memory_limit} if memory_limit else None
    with safe_connect(config=duckdb_config) as con:
        _load_standard_macros(con, logger)
        read_info = read_raw_to_relation(con, input_files, read_cfg, read_mode, logger)
        # Strip trailing semicolons: clean.sql spesso termina con ;
        query = sql_query.rstrip().rstrip(";").rstrip()
        if sample_rows is not None:
            query = f"SELECT * FROM ({query}) AS _smoke_sample LIMIT {int(sample_rows)}"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        # COPY diretto dalla query (streaming): niente CREATE TABLE intermedia.
        # Sui cubi grandi (decine di milioni di righe) la materializzazione in
        # tabella esauriva la RAM; il COPY in streaming tiene il picco al solo
        # output. Il profilo e' poi letto dal parquet appena scritto.
        con.execute(
            f"COPY ({query}) TO '{sql_path(output_path)}' (FORMAT PARQUET, COMPRESSION ZSTD);"
        )
        con.execute(
            f"CREATE VIEW {CLEAN_OUT_TABLE} AS "
            f"SELECT * FROM read_parquet('{sql_path(output_path)}')"
        )
        output_profile = profile_relation(con, CLEAN_OUT_TABLE)
        return read_info.source, read_info.params_used, output_profile
