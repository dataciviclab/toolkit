"""Re-export dei servizi di dominio per backward compat.

Tutta la logica e' in ``toolkit.domain.layer``.
Questo modulo esiste solo per non rompere gli import esistenti:
``toolkit.cli.layer_ops`` → ``toolkit.domain.layer``
"""

from toolkit.domain.layer import (  # noqa: F401  # re-export per backward compat
    clean_preview,
    layer_query,
    layer_sql,
    raw_preview,
    raw_profile,
)
from toolkit.domain.schema import show_schema  # noqa: F401  # re-export
