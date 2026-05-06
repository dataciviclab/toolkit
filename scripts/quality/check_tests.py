#!/usr/bin/env python3
"""Test quality ratchet — single entry point for CI and developers.

Usage:
    python scripts/quality/check_tests.py --strict     # fails on ratchet violations
    python scripts/quality/check_tests.py             # advisory (always passes)

Strict mode fails if:
  1. A strict file contains unmarked tests
  2. A NEW test file (not in legacy) has any unmarked tests

It does NOT fail on unmarked legacy tests.
"""

from __future__ import annotations

import argparse
import ast
import json
import sys
from pathlib import Path

MARKERS = {"contract", "policy", "regression", "adapter", "pure_unit", "smoke"}
_MARKER_RE = __import__("re").compile(r"mark\.(\w+)")


class TestCollector(ast.NodeVisitor):
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
        for decorator in reversed(node.decorator_list):
            try:
                src = ast.unparse(decorator)
                m = _MARKER_RE.search(src)
                if m and m.group(1) in MARKERS:
                    markers.add(m.group(1))
            except Exception:
                pass
        self.results.append(
            {
                "test": node.name,
                "markers": sorted(markers & MARKERS),
                "unmarked": not bool(markers & MARKERS),
            }
        )

    def __init__(self) -> None:
        self.results: list[dict] = []


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
    parser = argparse.ArgumentParser(prog="scripts/quality/check_tests.py")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail on ratchet violations (strict mode)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Machine-readable output",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Only print status line",
    )
    args = parser.parse_args()

    toolkit_root = Path(__file__).parent.parent.parent
    tests_dir = toolkit_root / "tests"
    scripts_dir = Path(__file__).parent

    # Load strict and legacy file lists
    strict_files = _read_file_list(scripts_dir / "strict_test_files.txt")
    legacy_files = _read_file_list(scripts_dir / "legacy_test_files.txt")

    results = collect_tests(tests_dir)

    # Aggregate by file
    file_results: dict[str, dict] = {}
    for r in results:
        fname = r["file"]
        if fname not in file_results:
            file_results[fname] = {"marked": 0, "unmarked": 0, "tests": []}
        file_results[fname]["tests"].append(r)
        if r["unmarked"]:
            file_results[fname]["unmarked"] += 1
        else:
            file_results[fname]["marked"] += 1

    total = len(results)
    marked = sum(1 for r in results if not r["unmarked"])
    unmarked = total - marked
    strict_failures: list[str] = []
    new_unclassified_files: list[str] = []

    for fname, fr in file_results.items():
        if fname in strict_files:
            if fr["unmarked"] > 0:
                strict_failures.append(f"  {fname}: {fr['unmarked']} unmarked test(s)")
        elif fname not in legacy_files:
            # New test file — ALL tests must be marked, partial marking is not acceptable
            if fr["unmarked"] > 0:
                new_unclassified_files.append(f"  {fname}")

    status = "ok"
    if args.strict:
        if strict_failures:
            status = "strict_fail"
        elif new_unclassified_files:
            status = "new_unclassified_fail"

    if args.json:
        output = {
            "total_tests": total,
            "marked_tests": marked,
            "unmarked_tests": unmarked,
            "strict_files": len(strict_files),
            "strict_failures": len(strict_failures),
            "new_unclassified_files": len(new_unclassified_files),
            "strict_failure_list": strict_failures,
            "new_unclassified_file_list": new_unclassified_files,
            "status": status,
        }
        print(json.dumps(output, indent=2))
        return

    # Human-readable output
    if not args.quiet:
        print("test quality ratchet")
        print(f"  total_tests:     {total}")
        print(f"  marked_tests:    {marked}")
        print(f"  unmarked_tests:  {unmarked}")
        print(f"  strict_files:   {len(strict_files)}")
        print(f"  strict_failures: {len(strict_failures)}")
        print(f"  new_unclassified_files: {len(new_unclassified_files)}")
        if strict_failures:
            print("  STRICT FAILURES:")
            for f in strict_failures:
                print(f)
        if new_unclassified_files:
            print("  NEW UNCLASSIFIED FILES (strict mode):")
            for f in new_unclassified_files:
                print(f)

    status_line = f"status: {status}"
    print(status_line)

    # Exit code
    if args.strict and status != "ok":
        sys.exit(1)


def _read_file_list(path: Path) -> set[str]:
    """Read file list, normalize to just filenames (no directory prefix)."""
    if not path.exists():
        return set()
    result: set[str] = set()
    for line in path.read_text().splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        # Normalize: "tests/foo.py" -> "foo.py", "foo.py" -> "foo.py"
        result.add(Path(stripped).name)
    return result


if __name__ == "__main__":
    main()
