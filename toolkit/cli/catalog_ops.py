"""Re-export dei servizi di dominio per backward compat.

Tutta la logica e' in ``toolkit.domain.catalog``.
Questo modulo esiste solo per non rompere gli import esistenti:
``toolkit.cli.catalog_ops`` → ``toolkit.domain.catalog``
"""

from toolkit.domain.catalog import (  # noqa: F401  # re-export per backward compat
    CatalogResolver,
    VALID_LAYERS,
    VALID_SOURCES,
    VALID_STAGES,
    VALID_RUN_STATUSES,
)
