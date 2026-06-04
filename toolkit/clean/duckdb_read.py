"""Backward-compat shim — spostato in ``toolkit.core.duckdb_read``.

Importare da ``toolkit.core.duckdb_read`` direttamente.
Questo modulo verrà rimosso in una versione futura.
"""

from __future__ import annotations

from toolkit.core.duckdb_read import (  # noqa: F401
    SUPPORTED_INPUT_EXTS,
    ReadInfo,
    _csv_read_options,
    read_raw_to_relation,
)
