"""inspect command — delegates to toolkit.cli.inspect package."""

from __future__ import annotations

from toolkit.cli.inspect import register as register

# Re-exported for backward compatibility (used by tests)
from toolkit.core.io import read_json_or_none as _read_json  # noqa: F401

__all__ = ["register", "_read_json"]