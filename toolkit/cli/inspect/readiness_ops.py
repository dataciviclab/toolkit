"""Re-export dei servizi di dominio per backward compat.

Tutta la logica e' in ``toolkit.domain.readiness``.
"""

from toolkit.domain.readiness import (  # noqa: F401  # re-export per backward compat
    review_readiness,
    run_state,
    summary,
)
