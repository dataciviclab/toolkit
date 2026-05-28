"""API pubblica stabile per path contract del toolkit.

Consumer esterni (dataset-incubator, data-explorer) importano da qui
invece di hardcodare i path. Le funzioni sono re-export da
``toolkit.core.paths`` — il modulo interno puo` cambiare, questo no.

Esempi::

    from toolkit.contracts import clean_parquet_path, layer_year_dir

    # Directory CLEAN per un dataset/anno
    layer_year_dir("/out", "clean", "mio_dataset", 2024)
    # -> PosixPath('/out/data/clean/mio_dataset/2024')

    # Path completo al parquet CLEAN
    clean_parquet_path("/out", "mio_dataset", 2024)
    # -> PosixPath('/out/data/clean/mio_dataset/2024/mio_dataset_2024_clean.parquet')

    # Directory radice risolta (abs, expanduser)
    resolve_root("~/projects/out")
    # -> PosixPath('/home/user/projects/out')

Costanti utili::

    CLEAN_PARQUET_SUFFIX   # "_clean.parquet" — per filtrare/globbare
    METADATA_JSON          # "metadata.json"
"""

from __future__ import annotations

from pathlib import Path as _Path

from toolkit.core.paths import (
    layer_dataset_dir,
    layer_year_dir,
    resolve_root,
)

__all__ = [
    # Layer directories
    "layer_year_dir",
    "layer_dataset_dir",
    # File paths
    "clean_parquet_path",
    "mart_table_path",
    "run_record_dir",
    # Utilities
    "resolve_root",
    # Costanti file
    "CLEAN_PARQUET_SUFFIX",
    "METADATA_JSON",
]

# ---------------------------------------------------------------------------
# Costanti — pattern di file canonici
# ---------------------------------------------------------------------------

CLEAN_PARQUET_SUFFIX = "_clean.parquet"
"""Suffisso del parquet CLEAN. Usato per filtrare/globbare i file.

Esempio::

    file.endswith(CLEAN_PARQUET_SUFFIX)
    # invece di: file.endswith(\"_clean.parquet\")
"""

METADATA_JSON = "metadata.json"


# ---------------------------------------------------------------------------
# Funzioni path — layer specifici
# ---------------------------------------------------------------------------


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
    return base / f"{dataset}_{year}{CLEAN_PARQUET_SUFFIX}"


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
    return _Path(str(resolve_root(root))) / "data" / "_runs" / dataset / str(year)
