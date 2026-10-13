"""Guard di layering: core/ non deve importare layer superiori.

Contratto architetturale (README): core = infrastruttura pura al fondo;
sopra stanno domain (orchestration), raw/clean/mart (layer pipeline),
plugins, scout, scaffold, cli, mcp. Un import core→superiore è
un'inversione — storicamente nascosta da import function-local
(6 inversioni rimosse in PR #504).

Sostituisce lo script manuale scripts/audit-imports.py.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

pytestmark = pytest.mark.policy

_PKG = Path(__file__).resolve().parent.parent / "toolkit"
_UPPER = (
    "toolkit.clean",
    "toolkit.mart",
    "toolkit.raw",
    "toolkit.domain",
    "toolkit.scout",
    "toolkit.scaffold",
    "toolkit.plugins",
    "toolkit.cli",
    "toolkit.mcp",
)


def _imported_modules(tree: ast.AST) -> list[tuple[str, int]]:
    mods: list[tuple[str, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            mods.append((node.module, node.lineno))
        elif isinstance(node, ast.Import):
            mods.extend((alias.name, node.lineno) for alias in node.names)
    return mods


def test_core_does_not_import_upper_layers() -> None:
    """Nessun modulo toolkit.core.* importa layer superiori."""
    violations: list[str] = []
    for f in sorted((_PKG / "core").rglob("*.py")):
        mod = ".".join(f.with_suffix("").parts)
        for imported, lineno in _imported_modules(ast.parse(f.read_text(encoding="utf-8"))):
            for upper in _UPPER:
                if imported == upper or imported.startswith(upper + "."):
                    violations.append(f"{mod}:{lineno} -> {imported}")
    assert not violations, (
        "Inversioni di layering core→superiore:\n"
        + "\n".join(violations)
        + "\n(Muovere il modulo nel layer giusto, come in PR #504.)"
    )
