"""Backward-compat shim — rinominato in ``toolkit.core.duckdb_shape``.

``parquet.py`` è stato rinominato in ``duckdb_shape.py`` perché ora
contiene anche ``csv_quick_shape``.
Importare da ``toolkit.core.duckdb_shape`` direttamente.
Questo modulo verrà rimosso in una versione futura.
"""

from __future__ import annotations

from toolkit.core.duckdb_shape import *  # noqa: F401, F403
