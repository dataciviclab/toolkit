"""Backward-compat shim — spostato in ``toolkit.core.duckdb_read``.

Importare da ``toolkit.core.duckdb_read`` direttamente.
Questo modulo verrà rimosso in una versione futura.
"""

from __future__ import annotations

from toolkit.core.duckdb_read import *  # noqa: F401, F403
