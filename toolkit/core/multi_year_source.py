"""Utility per raccolta file multi-anno e binding view DuckDB.

Condivisa tra mart (run_mart_multi_year) e altri potenziali consumer
che devono aggregare dati da piu' anni. Assorbe la logica dell'ex
modulo ``cross/``.
"""

from __future__ import annotations

from pathlib import Path

from toolkit.core.paths import layer_year_dir
from toolkit.core.sql_utils import sql_path


def collect_multi_year_files(
    root: str | None,
    dataset: str,
    *,
    years: list[int],
    source_layer: str = "clean",
    source_table: str | None = None,
) -> list[Path]:
    """Collect source parquet files from multiple years.

    Args:
        root: root directory for data output.
        dataset: dataset name.
        years: list of years to collect files for.
        source_layer: ``"clean"`` (default) or ``"mart"``.
        source_table: required when source_layer='mart'.

    Returns:
        List of parquet file paths from all specified years.
    """
    if not years:
        raise ValueError("multi-year source: years list must not be empty")

    files: list[Path] = []
    for y in years:
        if source_layer == "clean":
            src_dir = layer_year_dir(root, "clean", dataset, y)
            if not src_dir.exists():
                raise FileNotFoundError(
                    f"CLEAN dir not found: {src_dir}. Run: toolkit run clean -c dataset.yml"
                )
            year_files = sorted(src_dir.glob("*.parquet"))
            if not year_files:
                raise FileNotFoundError(f"No CLEAN parquet found in {src_dir}")
            files.extend(year_files)
        elif source_layer == "mart":
            if not source_table:
                raise ValueError("source_table is required when source_layer='mart'")
            src_file = layer_year_dir(root, "mart", dataset, y) / f"{source_table}.parquet"
            if not src_file.exists():
                raise FileNotFoundError(
                    f"MART parquet not found: {src_file}. Run: toolkit run mart -c dataset.yml"
                )
            files.append(src_file)
        else:
            raise ValueError(f"Unsupported source_layer: {source_layer}")
    return files


def bind_multi_year_view(con, files: list[Path], *, source_layer: str = "clean") -> None:
    """Bind views from multi-year parquet files into a DuckDB connection.

    Espone sempre ``source_input``, ``clean_input`` e ``clean``.
    Se ``source_layer="mart"``, espone anche ``mart_input``, ``mart``
    e ``mart_all_years`` per compatibilità con l'ex ``cross_year``.

    Args:
        con: DuckDB connection.
        files: list of parquet file paths to bind.
        source_layer: ``"clean"`` (default) or ``"mart"``.
    """
    if len(files) == 1:
        source_expr = f"read_parquet('{sql_path(files[0])}')"
    else:
        paths = ",".join(f"'{sql_path(p)}'" for p in files)
        source_expr = f"read_parquet([{paths}])"

    # Views universali (sempre disponibili)
    con.execute(f"CREATE OR REPLACE VIEW source_input AS SELECT * FROM {source_expr}")
    con.execute("CREATE OR REPLACE VIEW clean_input AS SELECT * FROM source_input")
    con.execute("CREATE OR REPLACE VIEW clean AS SELECT * FROM source_input")

    # Views specifiche per source_layer (ex cross_year contract)
    if source_layer == "mart":
        con.execute("CREATE OR REPLACE VIEW mart_input AS SELECT * FROM source_input")
        con.execute("CREATE OR REPLACE VIEW mart AS SELECT * FROM source_input")
        con.execute("CREATE OR REPLACE VIEW mart_all_years AS SELECT * FROM source_input")
