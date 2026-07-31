"""SQL dry-run validation — DEPRECATO, importa da toolkit.core.sql_validation."""

from __future__ import annotations

import warnings

from toolkit.core.sql_validation import *  # noqa: F403

warnings.warn(
    "toolkit.cli.sql_dry_run è deprecato, importa da toolkit.core.sql_validation",
    DeprecationWarning,
    stacklevel=2,
)
