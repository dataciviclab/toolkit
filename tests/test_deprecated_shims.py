from __future__ import annotations

import importlib
import sys

import pytest


@pytest.mark.parametrize(
    ("module_name", "message"),
    [
        (
            "toolkit.core.validators",
            "Deprecated import path 'toolkit.core.validators'; use 'toolkit.core.validation' instead; "
            "will be removed in v0.5.",
        ),
        (
            "toolkit.core.validation_summary",
            "Deprecated import path 'toolkit.core.validation_summary'; use 'toolkit.core.validation' instead; "
            "will be removed in v0.5.",
        ),
    ],
)
def test_importing_compatibility_shim_emits_deprecation_warning(module_name: str, message: str) -> None:
    sys.modules.pop(module_name, None)

    with pytest.warns(DeprecationWarning, match=message):
        importlib.import_module(module_name)
