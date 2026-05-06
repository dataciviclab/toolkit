#!/usr/bin/env python3
"""Audit test markers in toolkit/tests/.

Non-blocking. Lists tests that lack any of the Lab-wide markers:
contract, policy, regression, adapter, pure_unit, smoke

Usage:
    python scripts/audit_test_markers.py            # all tests
    python scripts/audit_test_markers.py --fix     # print marker lines to add
    python scripts/audit_test_markers.py --json    # machine-readable output
"""

from __future__ import annotations

import ast
import json
import re
import sys
from pathlib import Path

MARKERS = {"contract", "policy", "regression", "adapter", "pure_unit", "smoke"}
#: ast.unparse returns 'pytest.mark.contract' (no @), so match after 'mark.'
_MARKER_RE = re.compile(r"mark\.(\w+)")


class TestCollector(ast.NodeVisitor):
    """Collect test functions and their markers via AST."""

    def __init__(self) -> None:
        self.results: list[dict] = []

    def visit_Module(self, node: ast.Module) -> None:
        for stmt in ast.iter_child_nodes(node):
            if isinstance(stmt, ast.FunctionDef) and stmt.name.startswith("test_"):
                self._visit_test_function(stmt)
            elif isinstance(stmt, ast.ClassDef):
                for child in ast.iter_child_nodes(stmt):
                    if isinstance(child, ast.FunctionDef) and child.name.startswith("test_"):
                        self._visit_test_function(child)
        self.generic_visit(node)

    def _visit_test_function(self, node: ast.FunctionDef) -> None:
        markers: set[str] = set()
        for decorator in reversed(node.decorator_list):  # reversed = bottom to top
            marker = self._get_marker_name(decorator)
            if marker:
                markers.add(marker)

        # Detect parametrize for informational note
        parametrized = any(
            isinstance(d, ast.Call)
            and isinstance(d.func, ast.Attribute)
            and d.func.attr == "parametrize"
            or isinstance(d, ast.Attribute)
            and d.attr == "parametrize"
            for d in node.decorator_list
        )

        self.results.append(
            {
                "test": node.name,
                "markers": sorted(markers & MARKERS),
                "missing": sorted(MARKERS - markers),
                "unmarked": not bool(markers & MARKERS),
                "parametrized": parametrized,
            }
        )

    def _get_marker_name(self, node: ast.expr) -> str | None:
        """Extract Lab marker name from a decorator via ast.unparse + regex."""
        try:
            src = ast.unparse(node)
            m = _MARKER_RE.search(src)
            if m and m.group(1) in MARKERS:
                return m.group(1)
        except Exception:
            pass
        return None


def collect_tests(tests_dir: Path) -> list[dict]:
    results = []
    for fpath in sorted(tests_dir.glob("test_*.py")):
        if fpath.name == "conftest.py":
            continue
        try:
            tree = ast.parse(fpath.read_text(), filename=fpath.name)
        except SyntaxError:
            continue
        visitor = TestCollector()
        visitor.visit(tree)
        for r in visitor.results:
            r["file"] = fpath.name
            results.append(r)
    return results


def main() -> None:
    scripts_dir = Path(__file__).parent
    toolkit_root = scripts_dir.parent
    tests_dir = toolkit_root / "tests"

    results = collect_tests(tests_dir)

    unmarked = [r for r in results if r["unmarked"]]
    marked = [r for r in results if not r["unmarked"]]

    mode = "json" if "--json" in sys.argv else "fix" if "--fix" in sys.argv else "text"

    if mode == "json":
        print(
            json.dumps(
                {"total": len(results), "marked": len(marked), "unmarked": len(unmarked), "tests": results},
                indent=2,
            )
        )
        return

    print("=== Test Marker Audit ===")
    print(f"Total: {len(results)} | Marked: {len(marked)} | Unmarked: {len(unmarked)}")
    print()

    if unmarked:
        print(f"--- UNMARKED TESTS ({len(unmarked)}) ---")
        for r in unmarked:
            note = " (parametrized)" if r["parametrized"] else ""
            print(f"  {r['file']}::{r['test']}{note}")
        print()

    if unmarked and mode == "fix":
        print("--- FIX: add these markers ---")
        for r in unmarked:
            print(f"# {r['file']}::{r['test']}")
            print("    @pytest.mark.pure_unit  # TODO: pick correct marker")
            print(f"    def {r['test']}(self): ...")

    if not unmarked:
        print("All tests have markers. OK")


if __name__ == "__main__":
    main()
