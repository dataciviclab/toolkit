"""Backward-compat shim: ``csv_quick_shape`` è stata spostata in ``toolkit.core.duckdb_shape``.

Importare da ``toolkit.core.duckdb_shape`` direttamente.
Questo modulo verrà rimosso in una versione futura.
"""

from __future__ import annotations

from toolkit.core.duckdb_shape import csv_quick_shape  # noqa: F401
