"""Backward-compat shim — spostato in ``toolkit.core.read_excel``.

Importare da ``toolkit.core.read_excel`` direttamente.
Questo modulo verrà rimosso in una versione futura.
"""

from __future__ import annotations

from toolkit.core.read_excel import *  # noqa: F401, F403
