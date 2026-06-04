"""Backward-compat shim — ``manifest.json`` è stato sostituito da ``metadata.json``.

Tutte le funzioni di lettura/scrittura manifest sono migrate in
``toolkit.core.metadata``. Questo modulo esiste solo per non rompere
``import toolkit.core.manifest`` in codice esterno.
Sarà rimosso in una versione futura.
"""

from __future__ import annotations

import warnings as _warnings

_warnings.warn(
    "toolkit.core.manifest è deprecato. Usa toolkit.core.metadata.",
    DeprecationWarning,
    stacklevel=2,
)
