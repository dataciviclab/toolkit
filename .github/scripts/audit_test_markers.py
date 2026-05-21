#!/usr/bin/env python3
"""Audit test markers — cross-repo.

Usage:
    python3 _local/scripts/audit_test_markers.py tests/            # audit entire dir
    python3 _local/scripts/audit_test_markers.py tests/ --diff     # only unmarked (exit 1 if any)
    python3 _local/scripts/audit_test_markers.py tests/ --json     # machine-readable
    python3 _local/scripts/audit_test_markers.py tests/ --files f1.py f2.py  # specific files

Exit code:
    0 = all tests have markers (or --diff not set)
    1 = at least one test without marker (only with --diff)
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from pathlib import Path

MARKERS = {"contract", "policy", "regression", "adapter", "pure_unit", "smoke"}
_MARKER_RE = re.compile(r"mark\.(\w+)")


class TestCollector(ast.NodeVisitor):
    """Collect test functions and their markers via AST.

    Detects markers from:
    - @pytest.mark.xxx decorators on test functions/methods
    - Module-level ``pytestmark = pytest.mark.xxx`` (applied to all tests in file)
    """

    def __init__(self) -> None:
        self.results: list[dict] = []
        self._module_markers: set[str] = set()

    def visit_Module(self, node: ast.Module) -> None:
        # Collect module-level pytestmark first
        for stmt in ast.iter_child_nodes(node):
            if isinstance(stmt, ast.Assign):
                for target in stmt.targets:
                    if isinstance(target, ast.Name) and target.id == "pytestmark":
                        marker = self._get_marker_name(stmt.value)
                        if marker:
                            self._module_markers.add(marker)
                    # Also handle list: pytestmark = [pytest.mark.contract, pytest.mark.smoke]
                    elif isinstance(target, ast.Name) and target.id == "pytestmark":
                        if isinstance(stmt.value, ast.List):
                            for elt in stmt.value.elts:
                                m = self._get_marker_name(elt)
                                if m:
                                    self._module_markers.add(m)

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
        # Inherit module-level markers
        markers.update(self._module_markers)
        # Add per-function decorator markers
        for decorator in reversed(node.decorator_list):
            marker = self._get_marker_name(decorator)
            if marker:
                markers.add(marker)
        self.results.append(
            {
                "test": node.name,
                "markers": sorted(markers & MARKERS),
                "missing": sorted(MARKERS - markers),
                "unmarked": not bool(markers & MARKERS),
            }
        )

    def _get_marker_name(self, node: ast.expr) -> str | None:
        try:
            src = ast.unparse(node)
            m = _MARKER_RE.search(src)
            if m and m.group(1) in MARKERS:
                return m.group(1)
        except Exception:
            pass
        return None


def collect_tests(tests_dir: Path, file_filter: list[str] | None = None) -> list[dict]:
    results = []
    pattern = "test_*.py"
    for fpath in sorted(tests_dir.glob(pattern)):
        if fpath.name == "conftest.py":
            continue
        if file_filter and fpath.name not in file_filter:
            continue
        # Also look in subdirectories
        if not fpath.is_file():
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

    # Also scan subdirectories (e.g. tests/http/ in lab-connectors)
    for subdir in sorted(tests_dir.iterdir()):
        if not subdir.is_dir() or subdir.name.startswith("__") or subdir.name == "__pycache__":
            continue
        for fpath in sorted(subdir.glob("test_*.py")):
            if file_filter and fpath.name not in file_filter:
                continue
            try:
                tree = ast.parse(fpath.read_text(), filename=fpath.name)
            except SyntaxError:
                continue
            visitor = TestCollector()
            visitor.visit(tree)
            for r in visitor.results:
                r["file"] = str(Path(subdir.name) / fpath.name)
                results.append(r)

    return results


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Audit test markers in any repo's test directory."
    )
    parser.add_argument("tests_dir", type=str, help="Path to tests/ directory")
    parser.add_argument(
        "--diff",
        action="store_true",
        help="Exit 1 if any test is unmarked (for CI gate)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Machine-readable JSON output",
    )
    parser.add_argument(
        "--files",
        nargs="*",
        default=None,
        help="Check only specific test files (for PR diff checks)",
    )
    args = parser.parse_args()

    tests_path = Path(args.tests_dir)
    if not tests_path.is_dir():
        print(f"ERROR: directory not found: {tests_path}", file=sys.stderr)
        sys.exit(2)

    results = collect_tests(tests_path, args.files)
    unmarked = [r for r in results if r["unmarked"]]
    marked = [r for r in results if not r["unmarked"]]

    if args.json:
        print(
            json.dumps(
                {
                    "total": len(results),
                    "marked": len(marked),
                    "unmarked": len(unmarked),
                    "tests": results,
                },
                indent=2,
            )
        )
    else:
        print("=== Test Marker Audit ===")
        print(f"Tests: {len(results)} | Marked: {len(marked)} | Unmarked: {len(unmarked)}")
        print()
        if unmarked:
            print(f"--- UNMARKED TESTS ({len(unmarked)}) ---")
            for r in unmarked:
                print(f"  {r['file']}::{r['test']}")
            print()
            print("Suggested markers (pick one per test):")
            print("  @pytest.mark.contract  — public interface, artifact format, CLI output")
            print("  @pytest.mark.policy    — Lab rule not derivable from source code")
            print("  @pytest.mark.regression — documented bug fix (link issue/PR)")
            print("  @pytest.mark.adapter   — external service adapter logic")
            print("  @pytest.mark.pure_unit  — pure logic, zero side effects")
            print("  @pytest.mark.smoke     — golden path end-to-end")
            print()
        if not unmarked:
            print("All tests have markers. OK")

    if args.diff and unmarked:
        sys.exit(1)


if __name__ == "__main__":
    main()
