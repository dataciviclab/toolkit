"""Backward-compat shim — spostato in ``toolkit.core.read_csv_normalized``.

Importare da ``toolkit.core.read_csv_normalized`` direttamente.
Questo modulo verrà rimosso in una versione futura.
"""

from __future__ import annotations

from toolkit.core.read_csv_normalized import *  # noqa: F401, F403
