"""Re-export dei servizi di dominio per backward compat.

Tutta la logica e' in ``toolkit.domain.report``.
Questo modulo esiste solo per non rompere gli import esistenti:
``toolkit.cli.inspect.report_ops`` → ``toolkit.domain.report``
"""

from toolkit.domain.report import (  # noqa: F401  # re-export per backward compat
    _all_reports_for_dataset,
    _derive_overall_status,
    build_dataset_readme,
    build_run_report,
    write_dataset_readme,
    write_run_report,
)
