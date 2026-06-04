"""Backward-compat shim — spostato in ``toolkit.core.read_sql_utils``.

Importare da ``toolkit.core.read_sql_utils`` direttamente.
Questo modulo verrà rimosso in una versione futura.
"""

from __future__ import annotations

from toolkit.core.read_sql_utils import (  # noqa: F401
    _parse_column_value,
    _run_sql_file,
    csv_trim_projection,
)
