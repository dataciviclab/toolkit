"""Compatibility shim for deprecated imports.

New code should import from `toolkit.core.validation`.
"""

from __future__ import annotations

import warnings

from toolkit.core.validation import ValidationResult
from toolkit.core.validation import required_columns_check as required_columns

warnings.warn(
    "Deprecated import path 'toolkit.core.validators'; use 'toolkit.core.validation' instead; "
    "will be removed in v0.5.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["ValidationResult", "required_columns"]
