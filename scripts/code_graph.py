#!/usr/bin/env python3
"""
Genera code_graph.json per il toolkit via analisi AST statica.

Output: generated/code_graph.json
Schema minimo:
- metadata: info generali
- nodes: {id, file, type, name, line, doc, params}
- edges: {from, to, type}  type in {imports, calls}
- cli_commands: {path, command, function, file}
- typer_groups: {file, group_name, commands[]}
"""

from __future__ import annotations

import ast
import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).parent.parent.resolve()
OUT_PATH = ROOT / "generated" / "code_graph.json"
SUMMARY_PATH = ROOT / "generated" / "code_graph_summary.md"
TEST_OUT_PATH = ROOT / "generated" / "code_graph_tests.json"
TEST_SUMMARY_PATH = ROOT / "generated" / "code_graph_tests_summary.md"

SKIP_DIRS = {"__pycache__", ".pytest_cache", ".ruff_cache", ".mypy_cache", "dataciviclab_toolkit.egg-info"}
SKIP_FILES = {"code_graph.py"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _iter_py_files(root: Path, *, include_tests: bool = False) -> list[Path]:
    files = []
    for p in (root / "toolkit").rglob("*.py"):
        if any(d in p.parts for d in SKIP_DIRS):
            continue
        if p.name in SKIP_FILES:
            continue
        files.append(p)
    if include_tests:
        for p in (root / "tests").rglob("*.py"):
            if any(d in p.parts for d in SKIP_DIRS):
                continue
            files.append(p)
    return sorted(files)


def _qual_id(node: ast.AST, full_module: str) -> str | None:
    """Return a qualified id for a Call/Name/Attribute node or None."""
    if isinstance(node, ast.Name):
        return f"{full_module}::{node.id}"
    if isinstance(node, ast.Attribute):
        value = _qual_id(node.value, full_module)
        if value:
            return f"{value}.{node.attr}"
    return None


def _get_docstring(node: ast.AST, max_len: int = 120) -> str | None:
    doc = ast.get_docstring(node)
    if doc:
        return doc.strip().split("\n")[0][:max_len]
    return None


def _get_params(node: ast.FunctionDef | ast.AsyncFunctionDef) -> list[dict]:
    params = []
    for arg in node.args.args:
        default = None
        if node.args.defaults:
            idx = len(node.args.args) - len(node.args.defaults)
            if node.args.args.index(arg) >= idx:
                def_idx = node.args.args.index(arg) - idx
                default = ast.unparse(node.args.defaults[def_idx])
        params.append({"name": arg.arg, "annotation": ast.unparse(arg.annotation) if arg.annotation else None, "default": default})
    return params


# ---------------------------------------------------------------------------
# Per-file analysis
# ---------------------------------------------------------------------------


def analyze_file(path: Path) -> dict:
    source = path.read_text(encoding="utf-8")
    rel = path.relative_to(ROOT).as_posix()
    full_module = rel.replace("/", ".").replace("\\", ".").replace(".py", "")

    tree = ast.parse(source, filename=str(path))

    nodes = []
    edges = []
    local_defs: dict[str, str] = {}  # local name -> qualified name

    # Module node
    nodes.append({
        "id": f"{full_module}",
        "file": rel,
        "type": "module",
        "name": path.stem,
        "line": 1,
        "doc": _get_docstring(tree) or None,
    })

    # Collect all function/method parents for call-graph attribution.
    # Each (parent_qid, parent_node) pair is walked for call edges.
    func_parents: list[tuple[str, ast.AST]] = []

    for item in ast.iter_child_nodes(tree):
        if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
            qid = f"{full_module}::{item.name}"
            local_defs[item.name] = qid
            nodes.append({
                "id": qid,
                "file": rel,
                "type": "function",
                "name": item.name,
                "line": item.lineno,
                "doc": _get_docstring(item),
                "params": _get_params(item),
            })
            func_parents.append((qid, item))
        elif isinstance(item, ast.ClassDef):
            qid = f"{full_module}::{item.name}"
            local_defs[item.name] = qid
            nodes.append({
                "id": qid,
                "file": rel,
                "type": "class",
                "name": item.name,
                "line": item.lineno,
                "doc": _get_docstring(item),
                "bases": [ast.unparse(b) for b in item.bases],
            })
            # Methods are added as separate call-graph parents (not under class)
            for sub in ast.iter_child_nodes(item):
                if isinstance(sub, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    sqid = f"{full_module}::{item.name}::{sub.name}"
                    # Also register under the method name alone for local call resolution
                    local_defs[f"{item.name}::{sub.name}"] = sqid
                    nodes.append({
                        "id": sqid,
                        "file": rel,
                        "type": "method",
                        "name": f"{item.name}::{sub.name}",
                        "line": sub.lineno,
                        "doc": _get_docstring(sub),
                        "params": _get_params(sub),
                    })
                    func_parents.append((sqid, sub))
        elif isinstance(item, ast.Import):
            for alias in item.names:
                target = alias.asname or alias.name
                edges.append({
                    "from": f"{full_module}",
                    "to": f"mod:{alias.name}",
                    "type": "imports",
                })
        elif isinstance(item, ast.ImportFrom):
            for alias in item.names:
                target = alias.asname or alias.name
                to_mod = f"{item.module}" if item.module else ""
                edges.append({
                    "from": f"{full_module}",
                    "to": f"mod:{to_mod}.{target}" if to_mod else f"mod:{target}",
                    "type": "imports",
                })

    # Static calls: walk each function/method body for calls to other local defs.
    # Uses func_parents list built above so each call is attributed to the
    # correct function/method node (not the class or module).
    for parent_qid, parent_node in func_parents:
        for node in ast.walk(parent_node):
            if isinstance(node, ast.Call):
                qid = _qual_id(node.func, full_module)
                if qid and qid.split("::")[0] == full_module:
                    # call to local function or method
                    rest = qid.split("::", 1)[1]
                    # Try both 'func' and 'ClassName::method' lookup keys
                    for lookup in (rest.split(".")[-1], rest):
                        if lookup in local_defs:
                            edges.append({
                                "from": parent_qid,
                                "to": local_defs[lookup],
                                "type": "calls",
                            })
                            break

    return {"rel": rel, "full_module": full_module, "nodes": nodes, "edges": edges}


# ---------------------------------------------------------------------------
# CLI commands extraction
# ---------------------------------------------------------------------------


def extract_cli_commands(files: list[Path]) -> list[dict]:
    """Extract CLI commands by parsing Typer registration patterns.

    Two patterns supported:
    1. app.py calls: register_fn(app) where register_fn registers commands
       directly on app via app.command("name")(func)
    2. cmd_run.py style: inside register(), a sub-typer is created and
       sub-typer.command("name")(func) is called, then app.add_typer(sub, name="run")
    """
    commands: list[dict] = []

    # Map: path -> list of (caller_node, register_func_body)
    # First pass: find register_* calls in app.py
    app_py = None
    for f in files:
        if f.name == "app.py":
            app_py = f
            break

    if not app_py:
        return commands

    app_source = app_py.read_text(encoding="utf-8")
    app_tree = ast.parse(app_source, filename=str(app_py))

    # Find all register_* calls in app.py
    register_calls: dict[str, Path] = {}  # func_name -> module path
    for item in ast.walk(app_tree):
        if isinstance(item, ast.Call) and isinstance(item.func, ast.Name):
            name = item.func.id
            if name.startswith("register_"):
                # The import should tell us which module
                for imp in ast.walk(app_tree):
                    if isinstance(imp, ast.ImportFrom):
                        for alias in imp.names:
                            if alias.asname == name or alias.name == name:
                                mod_name = imp.module or ""
                                # Find the module path
                                for f in files:
                                    if f.stem == mod_name.split(".")[-1]:
                                        register_calls[name] = f

    # Now analyze each register function
    for reg_name, reg_path in register_calls.items():
        reg_source = reg_path.read_text(encoding="utf-8")
        try:
            reg_tree = ast.parse(reg_source, filename=str(reg_path))
        except SyntaxError:
            continue

        rel = reg_path.relative_to(ROOT).as_posix()

        # Find the register function definition in this module
        reg_func = None
        for item in ast.iter_child_nodes(reg_tree):
            if isinstance(item, ast.FunctionDef) and item.name == "register":
                reg_func = item
                break

        if not reg_func:
            continue

        # The first argument of the register function is always the Typer app instance.
        typer_main_param = reg_func.args.args[0].arg if reg_func.args and reg_func.args.args else None

        # Collect typer sub-group vars and their group names
        typer_sub_vars: dict[str, str] = {}  # var name -> group name
        typer_main_vars: set[str] = set()  # var names of typer.Typer() assigned

        # First pass over TOP-LEVEL statements only (not nested functions).
        # This ensures add_typer vars are collected before any command() calls,
        # since add_typer always appears after the sub-typer creation and
        # command() registrations in the source order.
        for node in reg_func.body:
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        if isinstance(node.value, ast.Call):
                            func = node.value.func
                            if isinstance(func, ast.Attribute) and func.attr == "Typer":
                                typer_main_vars.add(target.id)
            if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
                call = node.value
                if isinstance(call.func, ast.Attribute) and call.func.attr == "add_typer":
                    group_name = None
                    sub_var = None
                    # The sub-typer instance is the first positional argument
                    if call.args and isinstance(call.args[0], ast.Name):
                        sub_var = call.args[0].id
                    # The caller (app) is call.func.value
                    for kw in call.keywords:
                        if kw.arg == "name" and isinstance(kw.value, ast.Constant):
                            group_name = kw.value.value
                    if sub_var and group_name:
                        typer_sub_vars[sub_var] = group_name

        # Second pass: collect command registrations.
        # Walk using a parent map to distinguish top-level Expr statements from
        # nested calls within a larger expression.
        parents: dict[ast.AST, ast.AST] = {}
        for parent in ast.walk(reg_func):
            for child in ast.iter_child_nodes(parent):
                parents[child] = parent

        for node in ast.walk(reg_func):
            if not isinstance(node, ast.Call):
                continue
            func = node.func

            # Pattern 2: sub_typer.command("name")  — func is Attribute (inner call).
            # Skip if parent is a Call node (inner call of a pattern-1 expression),
            # and if parent is an Expr (outer call wrapper — pattern 1 handles it).
            if isinstance(func, ast.Attribute) and func.attr == "command":
                parent = parents.get(node)
                if isinstance(parent, ast.Call):
                    # Inner call: skip (pattern 1 will handle the full expression)
                    continue
                is_sub = isinstance(func.value, ast.Name) and func.value.id in typer_sub_vars
                cmd_name = None
                if node.args and isinstance(node.args[0], ast.Constant):
                    cmd_name = node.args[0].value
                if is_sub:
                    group = typer_sub_vars.get(func.value.id)
                    commands.append({
                        "path": rel,
                        "command": f"{group} {cmd_name}" if group else cmd_name,
                        "function": None,
                        "group": group,
                    })
            # Pattern 1: app.command("name")(func)  — func is Call (outer call).
            # Fires for top-level Expr nodes only to avoid double-adding.
            elif isinstance(func, ast.Call) and isinstance(func.func, ast.Attribute) and func.func.attr == "command":
                parent = parents.get(node)
                if not isinstance(parent, ast.Expr):
                    continue
                inner = func
                cmd_name = None
                if inner.args and isinstance(inner.args[0], ast.Constant):
                    cmd_name = inner.args[0].value
                func_name = None
                if node.args and isinstance(node.args[0], ast.Name):
                    func_name = node.args[0].id
                sub_var = func.func.value.id if isinstance(func.func.value, ast.Name) else None
                if sub_var and sub_var in typer_sub_vars:
                    group = typer_sub_vars[sub_var]
                    commands.append({
                        "path": rel,
                        "command": f"{group} {cmd_name}" if group else cmd_name,
                        "function": func_name,
                        "group": group,
                    })
                elif sub_var and (sub_var in typer_main_vars or sub_var == typer_main_param):
                    commands.append({
                        "path": rel,
                        "command": cmd_name,
                        "function": func_name,
                        "group": None,
                    })

    return commands


# ---------------------------------------------------------------------------
# Build graph
# ---------------------------------------------------------------------------


def build_graph(*, include_tests: bool = False) -> dict:
    files = _iter_py_files(ROOT, include_tests=include_tests)
    all_nodes: list[dict] = []
    all_edges: list[dict] = []
    all_modules: list[dict] = []

    for path in files:
        result = analyze_file(path)
        all_nodes.extend(result["nodes"])
        all_edges.extend(result["edges"])
        all_modules.append({
            "file": result["rel"],
            "module": result["full_module"],
        })

    cli_commands = extract_cli_commands(files)

    # Deduplicate edges
    seen = set()
    deduped_edges = []
    for e in all_edges:
        key = (e["from"], e["to"], e["type"])
        if key not in seen:
            seen.add(key)
            deduped_edges.append(e)

    return {
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "root": str(ROOT.name),
            "includes_tests": include_tests,
            "python_files": len(all_modules),
            "total_nodes": len(all_nodes),
            "total_edges": len(deduped_edges),
            "cli_commands": len(cli_commands),
        },
        "modules": all_modules,
        "nodes": all_nodes,
        "edges": deduped_edges,
        "cli_commands": cli_commands,
    }


def build_summary(graph: dict) -> str:
    nodes = graph["nodes"]
    edges = graph["edges"]
    cli_commands = graph["cli_commands"]

    node_file = {n["id"]: n["file"] for n in nodes}
    node_type = {n["id"]: n["type"] for n in nodes}

    file_stats: dict[str, dict[str, int]] = defaultdict(
        lambda: {"nodes": 0, "call_out": 0, "call_in": 0, "import_out": 0}
    )
    internal_importers: Counter[str] = Counter()
    calls_out: Counter[str] = Counter()
    calls_in: Counter[str] = Counter()

    for node in nodes:
        file_stats[node["file"]]["nodes"] += 1

    for edge in edges:
        src_file = node_file.get(edge["from"])
        dst_file = node_file.get(edge["to"])

        if edge["type"] == "calls":
            calls_out[edge["from"]] += 1
            calls_in[edge["to"]] += 1
            if src_file:
                file_stats[src_file]["call_out"] += 1
            if dst_file:
                file_stats[dst_file]["call_in"] += 1
        else:
            if src_file:
                file_stats[src_file]["import_out"] += 1
            if edge["from"].startswith("toolkit.") and edge["to"].startswith("mod:toolkit."):
                internal_importers[edge["from"]] += 1
            if edge["from"].startswith("tests.") and edge["to"].startswith("mod:toolkit."):
                internal_importers[edge["from"]] += 1

    hotspot_rows = []
    for file_path, stats in file_stats.items():
        score = stats["nodes"] + stats["call_out"] + stats["call_in"] + stats["import_out"]
        hotspot_rows.append((score, file_path, stats))
    hotspot_rows.sort(reverse=True)

    config_rows = []
    for file_path, stats in sorted(file_stats.items()):
        if "config_models" not in file_path:
            continue
        score = stats["nodes"] + stats["call_out"] + stats["call_in"] + stats["import_out"]
        config_rows.append((score, file_path, stats))

    lines = [
        "# Toolkit Code Graph Summary",
        "",
        "## Metadata",
        f"- Generated at: `{graph['metadata']['generated_at']}`",
        f"- Includes tests: `{graph['metadata'].get('includes_tests', False)}`",
        f"- Python files: `{graph['metadata']['python_files']}`",
        f"- Nodes: `{graph['metadata']['total_nodes']}`",
        f"- Edges: `{graph['metadata']['total_edges']}`",
        f"- CLI commands: `{graph['metadata']['cli_commands']}`",
        "",
        "## CLI Commands",
    ]
    for item in sorted(cli_commands, key=lambda x: x["command"]):
        lines.append(f"- `{item['command']}` -> `{item.get('function') or '(decorator-only)'}`")

    lines.extend(
        [
            "",
            "## Top Hotspots",
            "| Score | File | Nodes | Call out | Call in | Import out |",
            "|---|---|---:|---:|---:|---:|",
        ]
    )
    for score, file_path, stats in hotspot_rows[:15]:
        lines.append(
            f"| {score} | `{file_path}` | {stats['nodes']} | {stats['call_out']} | {stats['call_in']} | {stats['import_out']} |"
        )

    lines.extend(
        [
            "",
            "## Top Internal Importers",
            "| Imports | Module |",
            "|---:|---|",
        ]
    )
    for module, count in internal_importers.most_common(15):
        lines.append(f"| {count} | `{module}` |")

    lines.extend(
        [
            "",
            "## Top Call Out",
            "| Calls | Type | Symbol |",
            "|---:|---|---|",
        ]
    )
    for symbol, count in calls_out.most_common(15):
        lines.append(f"| {count} | `{node_type.get(symbol, '?')}` | `{symbol}` |")

    lines.extend(
        [
            "",
            "## Top Call In",
            "| Calls | Type | Symbol |",
            "|---:|---|---|",
        ]
    )
    for symbol, count in calls_in.most_common(15):
        lines.append(f"| {count} | `{node_type.get(symbol, '?')}` | `{symbol}` |")

    lines.extend(
        [
            "",
            "## Config Models Focus",
            "| Score | File | Nodes | Call out | Call in | Import out |",
            "|---|---|---:|---:|---:|---:|",
        ]
    )
    for score, file_path, stats in sorted(config_rows, reverse=True):
        lines.append(
            f"| {score} | `{file_path}` | {stats['nodes']} | {stats['call_out']} | {stats['call_in']} | {stats['import_out']} |"
        )

    return "\n".join(lines) + "\n"


def build_hotspots(graph: dict, *, top_n: int = 20) -> str:
    """Build a focused hotspots artifact for human review.

    Contains:
    - Top file hotspots by composite score
    - Top function/method hotspots by call volume
    - Max ~80 lines — scannable in 2 minutes
    """
    nodes = graph["nodes"]
    edges = graph["edges"]

    node_file = {n["id"]: n["file"] for n in nodes}
    node_type = {n["id"]: n["type"] for n in nodes}

    file_stats: dict[str, dict[str, int]] = defaultdict(
        lambda: {"nodes": 0, "call_out": 0, "call_in": 0, "import_out": 0}
    )
    calls_out: Counter[str] = Counter()
    calls_in: Counter[str] = Counter()

    for node in nodes:
        file_stats[node["file"]]["nodes"] += 1

    for edge in edges:
        src_file = node_file.get(edge["from"])
        dst_file = node_file.get(edge["to"])

        if edge["type"] == "calls":
            calls_out[edge["from"]] += 1
            calls_in[edge["to"]] += 1
            if src_file:
                file_stats[src_file]["call_out"] += 1
            if dst_file:
                file_stats[dst_file]["call_in"] += 1
        else:
            if src_file:
                file_stats[src_file]["import_out"] += 1

    # Top files by score
    file_rows = []
    for file_path, stats in file_stats.items():
        score = stats["nodes"] + stats["call_out"] + stats["call_in"] + stats["import_out"]
        file_rows.append((score, file_path, stats))
    file_rows.sort(reverse=True)

    # Top functions by call volume (calls_out + calls_in)
    func_scores: dict[str, int] = {}
    for sym in set(list(calls_out) + list(calls_in)):
        func_scores[sym] = calls_out.get(sym, 0) + calls_in.get(sym, 0)

    func_rows = sorted(func_scores.items(), key=lambda x: x[1], reverse=True)[:top_n]

    generated = graph["metadata"]["generated_at"]

    lines = [
        "# Toolkit Code Graph — Hotspots",
        "",
        f"_Generated: {generated}_",
        "",
        "## Top Files by Score",
        "",
        "| Score | File | Nodes | Calls out | Calls in | Imports out |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for score, file_path, stats in file_rows[:top_n]:
        lines.append(
            f"| {score} | `{file_path}` | {stats['nodes']} | "
            f"{stats['call_out']} | {stats['call_in']} | {stats['import_out']} |"
        )

    lines.extend([
        "",
        "## Top Functions/Methods by Call Volume",
        "",
        "| Volume | Out | In | Type | Symbol |",
        "|---:|---:|---:|---|---|",
    ])
    for sym, total in func_rows:
        out = calls_out.get(sym, 0)
        inc = calls_in.get(sym, 0)
        ntype = node_type.get(sym, "?")
        lines.append(f"| {total} | {out} | {inc} | `{ntype}` | `{sym}` |")

    return "\n".join(lines) + "\n"


def show_impact(graph: dict, symbol: str) -> None:
    """Print a human-readable impact report for a symbol.

    Shows:
    - Node type and file
    - Call-in: who calls this symbol
    - Call-out: what this symbol calls
    - Risk classification
    """
    nodes = graph["nodes"]
    edges = graph["edges"]

    # Find node
    node = None
    for n in nodes:
        if n["id"] == symbol:
            node = n
            break

    if node is None:
        print(f"[ERROR] Symbol '{symbol}' not found in graph.", file=sys.stderr)
        print("  Run with --list to see available symbols.", file=sys.stderr)
        return

    node_type = node.get("type", "?")
    node_file = node.get("file", "?")

    # Build call-in and call-out sets
    call_in: list[tuple[str, str]] = []  # (from_symbol, edge_type)
    call_out: list[tuple[str, str]] = []  # (to_symbol, edge_type)

    for edge in edges:
        if edge["type"] != "calls":
            continue
        if edge["to"] == symbol:
            call_in.append((edge["from"], edge["type"]))
        elif edge["from"] == symbol:
            call_out.append((edge["to"], edge["type"]))

    # Count for risk classification
    in_count = len(call_in)
    out_count = len(call_out)

    if in_count == 0 and out_count > 0:
        risk = "STAR_EMITTER  — calls others, never called directly"
    elif out_count == 0 and in_count > 0:
        risk = "STAR_RECEIVER — called by many, calls nothing"
    elif in_count == 0 and out_count == 0:
        risk = "ISOLATED     — no call-graph edges (may be dead or only used via runtime dispatch)"
    else:
        risk = f"BALANCED      — {in_count} callers, {out_count} callees"

    print()
    print(f"  Symbol: {symbol}")
    print(f"  Type:   {node_type}")
    print(f"  File:   {node_file}")
    print(f"  Risk:   {risk}")
    print()

    if call_in:
        print(f"  Called by ({in_count}):")
        # Deduplicate by from symbol (same symbol can appear multiple times from different paths)
        seen: set[str] = set()
        for from_sym, _ in call_in:
            if from_sym not in seen:
                seen.add(from_sym)
                print(f"    -> {from_sym}")
    else:
        print("  Called by: (none)")

    print()

    if call_out:
        print(f"  Calls ({out_count}):")
        seen: set[str] = set()
        for to_sym, _ in call_out:
            if to_sym not in seen:
                seen.add(to_sym)
                print(f"    -> {to_sym}")
    else:
        print("  Calls: (none)")

    print()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate static code graph for toolkit.")
    parser.add_argument(
        "--include-tests",
        action="store_true",
        help="Include tests/ in a separate graph output.",
    )
    parser.add_argument(
        "--hotspots",
        action="store_true",
        help="Also generate code_graph_hotspots.md (focused artifact for review).",
    )
    parser.add_argument(
        "--impact",
        metavar="SYMBOL",
        help="Show call impact for a symbol (e.g. toolkit.raw.run::run_raw).",
    )
    parser.add_argument(
        "--list",
        "-l",
        action="store_true",
        help="List all symbol IDs in the graph (for use with --impact).",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    args = parse_args()

    # Query-only modes: no files written
    if args.list:
        graph = build_graph(include_tests=False)
        for n in sorted(graph["nodes"], key=lambda x: x["id"]):
            print(n["id"])
        return

    if args.impact:
        graph = build_graph(include_tests=False)
        show_impact(graph, args.impact)
        return

    graph = build_graph(include_tests=False)
    summary = build_summary(graph)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(graph, indent=2, ensure_ascii=False), encoding="utf-8")
    SUMMARY_PATH.write_text(summary, encoding="utf-8")
    print(f"code_graph.json written to {OUT_PATH}")
    print(f"code_graph_summary.md written to {SUMMARY_PATH}")
    print(f"  modules: {graph['metadata']['python_files']}")
    print(f"  nodes: {graph['metadata']['total_nodes']}")
    print(f"  edges: {graph['metadata']['total_edges']}")
    print(f"  CLI commands: {graph['metadata']['cli_commands']}")
    if args.include_tests:
        test_graph = build_graph(include_tests=True)
        test_summary = build_summary(test_graph)
        TEST_OUT_PATH.write_text(json.dumps(test_graph, indent=2, ensure_ascii=False), encoding="utf-8")
        TEST_SUMMARY_PATH.write_text(test_summary, encoding="utf-8")
        print(f"code_graph_tests.json written to {TEST_OUT_PATH}")
        print(f"code_graph_tests_summary.md written to {TEST_SUMMARY_PATH}")
        print(f"  test modules: {test_graph['metadata']['python_files']}")
        print(f"  test nodes: {test_graph['metadata']['total_nodes']}")
        print(f"  test edges: {test_graph['metadata']['total_edges']}")


if __name__ == "__main__":
    sys.exit(main())
