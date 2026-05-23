"""API pubblica stabile per path contract del toolkit.

Consumer esterni (dataset-incubator, data-explorer) importano da qui
invece di hardcodare i path. Le funzioni sono re-export da
``toolkit.core.paths`` — il modulo interno puo` cambiare, questo no.

Esempio::

    from toolkit.contracts import layer_year_dir

    clean_path = layer_year_dir(
        root="/path/to/out", layer="clean", dataset="mio_dataset", year=2024
    )
    # -> PosixPath('/path/to/out/data/clean/mio_dataset/2024')
"""

from __future__ import annotations

from pathlib import Path as _Path

from toolkit.core.paths import (
    layer_dataset_dir,
    layer_year_dir,
)
from toolkit.core.paths import resolve_root as _resolve_root

__all__ = [
    "layer_year_dir",
    "layer_dataset_dir",
    "METADATA_JSON",
    "MANIFEST_JSON",
    "clean_parquet_path",
    "mart_table_path",
    "run_record_dir",
]

# ---------------------------------------------------------------------------
# Costanti di path — pattern di file canonici
# ---------------------------------------------------------------------------

METADATA_JSON = "metadata.json"
MANIFEST_JSON = "manifest.json"


def clean_parquet_path(
    root: str | _Path,
    dataset: str,
    year: int,
) -> _Path:
    """Path completo al parquet CLEAN per un dataset e anno.

    Esempio::

        clean_parquet_path("/out", "mio_dataset", 2024)
        # -> PosixPath('/out/data/clean/mio_dataset/2024/mio_dataset_2024_clean.parquet')
    """
    base = layer_year_dir(str(root), "clean", dataset, year)
    return base / f"{dataset}_{year}_clean.parquet"


def mart_table_path(
    root: str | _Path,
    dataset: str,
    year: int,
    table_name: str,
) -> _Path:
    """Path completo a una tabella MART per un dataset, anno e nome tabella.

    Esempio::

        mart_table_path("/out", "mio_dataset", 2024, "rd_by_regione")
        # -> PosixPath('/out/data/mart/mio_dataset/2024/rd_by_regione.parquet')
    """
    base = layer_year_dir(str(root), "mart", dataset, year)
    return base / f"{table_name}.parquet"


def run_record_dir(
    root: str | _Path,
    dataset: str,
    year: int,
) -> _Path:
    """Directory dei run record per un dataset e anno.

    Esempio::

        run_record_dir("/out", "mio_dataset", 2024)
        # -> PosixPath('/out/data/_runs/mio_dataset/2024')
    """
    return _Path(str(_resolve_root(root))) / "data" / "_runs" / dataset / str(year)
