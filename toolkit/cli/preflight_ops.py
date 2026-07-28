"""Pre-flight check — re-export per backward compat.

Tutta la logica e' in ``toolkit.domain.preflight``.
Questo modulo esiste solo per non rompere gli import esistenti.
"""

from toolkit.domain.preflight import run_preflight  # noqa: F401  # re-export
