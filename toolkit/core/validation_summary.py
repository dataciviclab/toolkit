from __future__ import annotations

import warnings

from toolkit.core.validation import build_validation_summary

warnings.warn(
    "Deprecated import path 'toolkit.core.validation_summary'; use 'toolkit.core.validation' instead; "
    "will be removed in v0.5.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["build_validation_summary"]
