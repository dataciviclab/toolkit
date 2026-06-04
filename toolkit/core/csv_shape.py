"""Backward-compat shim: ``csv_quick_shape`` è stata spostata in ``toolkit.core.parquet``.

Importare da ``toolkit.core.parquet`` direttamente.
Questo modulo verrà rimosso in una versione futura.
"""

from __future__ import annotations

from toolkit.core.duckdb_shape import csv_quick_shape  # noqa: F401
