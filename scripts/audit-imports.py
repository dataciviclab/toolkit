#!/usr/bin/env python3
"""Audit delle import: mappa chi usa cosa nei moduli centrali del toolkit.

Scansiona tutti i file Python del toolkit (esclusi test e venv) e produce:

1. Per ogni modulo in ``toolkit/core/``, la lista dei consumer (chi lo importa)
2. Pattern "bypass": codice che fa cose già centralizzate in core ma non le usa

Uso:
    python scripts/audit-imports.py                     # stampa tutto
    python scripts/audit-imports.py --consumer cli       # solo consumer in cli/
    python scripts/audit-imports.py --provider core      # solo cosa importa da core/
    python scripts/audit-imports.py --bypass             # solo pattern bypass
    python scripts/audit-imports.py --markdown           # output in markdown
"""

from __future__ import annotations

import ast
import os
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

TOOLKIT_DIR = Path(__file__).resolve().parent.parent

# Moduli centrali (provider)
CORE_MODULES = [
    "toolkit.core",
    "toolkit.core.paths",
    "toolkit.core.sql_utils",
    "toolkit.core.io",
    "toolkit.core.csv_read",
    "toolkit.core.parquet",
    "toolkit.core.config",
    "toolkit.core.metadata",
    "toolkit.core.validation",
    "toolkit.core.run_records",
    "toolkit.core.run_context",
    "toolkit.core.template",
    "toolkit.core.support",
    "toolkit.core.column_rules",
    "toolkit.core.layer_profile",
    "toolkit.core.artifacts",
    "toolkit.core.registry",
    "toolkit.core.multi_year_source",
    "toolkit.core.exceptions",
    "toolkit.core.logging",
]

# Pattern di bypass da rilevare (patrono → suggerimento)
BYPASS_PATTERNS: list[tuple[str, str, str]] = [
    # DuckDB connection
    (
        r"duckdb\.connect\(",
        "usa safe_connect() da lab_connectors.duckdb invece di duckdb.connect() diretto",
        "duckdb.connect diretto",
    ),
    # JSON I/O
    (
        r"(?<!read_json_or_none)json\.loads\(.*\.read_text",
        "usa read_json_or_none() da core.io invece di json.loads + read_text",
        "json.loads diretto",
    ),
    (
        r"\.write_text\(.*json\.dumps",
        "usa write_json_atomic() da core.io invece di write_text + json.dumps",
        "json.dumps diretto",
    ),
    # Path construction
    (
        r'f"[^"]*data/(raw|clean|mart)/[^"]*"',
        "usa layer_year_dir() / dataset_dir() da core.paths invece di costruire path a mano",
        "path data/ costruito a mano",
    ),
    (
        r"Path\(.*root.*\).*[/]data[/]",
        "usa layer_year_dir() / dataset_dir() da core.paths",
        "path root/data/ costruito a mano",
    ),
    # DuckDB view creation
    (
        r"CREATE OR REPLACE VIEW.*raw_input",
        "usa read_raw_to_relation() da clean.duckdb_read invece di CREATE VIEW manuale",
        "CREATE VIEW raw_input manuale",
    ),
    # SQL path quoting
    (
        r"read_parquet\('",
        "usa parquet_schema/row_count/preview da core.parquet invece di read_parquet diretto",
        "read_parquet diretto",
    ),
    (
        r"read_csv_auto\('",
        "usa csv_quick_shape da core.parquet o profile_with_read_cfg da profile.raw invece di read_csv_auto diretto",
        "read_csv_auto diretto",
    ),
]

EXCLUDE_DIRS = {
    "__pycache__",
    ".ruff_cache",
    ".mypy_cache",
    ".pytest_cache",
    "build",
    "dist",
    "egg-info",
}
EXCLUDE_FILES = {"__init__.py"}


def scan_imports(filepath: Path) -> list[dict[str, Any]]:
    """Estrae tutte le import da un file Python."""
    imports: list[dict[str, Any]] = []
    try:
        source = filepath.read_text(encoding="utf-8")
        tree = ast.parse(source)
    except (SyntaxError, UnicodeDecodeError):
        return imports

    for node in ast.walk(tree):
        # import X.Y.Z
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.append(
                    {
                        "type": "import",
                        "module": alias.name,
                        "name": alias.asname or alias.name,
                        "line": node.lineno,
                    }
                )
        # from X.Y import Z
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                for alias in node.names:
                    imports.append(
                        {
                            "type": "from",
                            "module": node.module,
                            "name": alias.name,
                            "asname": alias.asname,
                            "line": node.lineno,
                        }
                    )
    return imports


def scan_bypass_patterns(filepath: Path) -> list[dict[str, Any]]:
    """Cerca pattern di bypass in un file."""
    findings: list[dict[str, Any]] = []
    try:
        source = filepath.read_text(encoding="utf-8")
    except (UnicodeDecodeError, OSError):
        return findings

    for i, line in enumerate(source.split("\n"), 1):
        for pattern, suggestion, label in BYPASS_PATTERNS:
            if re.search(pattern, line):
                findings.append(
                    {
                        "line": i,
                        "pattern": label,
                        "code": line.strip()[:100],
                        "suggestion": suggestion,
                    }
                )
    return findings


def module_short(module: str) -> str:
    """Rende un modulo leggibile: toolkit.core.paths → core/paths."""
    return module.replace("toolkit.", "").replace(".", "/")


def run_audit(
    consumer_filter: str | None = None,
    provider_filter: str | None = None,
    only_bypass: bool = False,
    output_markdown: bool = False,
) -> str:
    # Raccogli tutti i file Python del toolkit
    py_files: list[Path] = []
    for root, dirs, files in os.walk(TOOLKIT_DIR / "toolkit"):
        dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]
        for f in files:
            if f.endswith(".py") and f not in EXCLUDE_FILES:
                py_files.append(Path(root) / f)

    # Raccogli import per file
    file_imports: dict[str, list[dict[str, Any]]] = {}
    for fp in py_files:
        rel = fp.relative_to(TOOLKIT_DIR).as_posix()
        imports = scan_imports(fp)
        if imports:
            file_imports[rel] = imports

    # Mappa inversa: modulo_core → [consumer]
    consumer_map: dict[str, list[tuple[str, int, str]]] = defaultdict(list)
    for filepath, imports in file_imports.items():
        for imp in imports:
            mod = imp["module"]
            # match qualsiasi modulo che inizi con toolkit.core
            for core_mod in CORE_MODULES:
                if mod == core_mod or mod.startswith(core_mod + "."):
                    consumer_map[core_mod].append(
                        (
                            filepath,
                            imp["line"],
                            imp["name"],
                        )
                    )

    # Bypass per file
    bypass_by_file: dict[str, list[dict[str, Any]]] = {}
    for fp in py_files:
        rel = fp.relative_to(TOOLKIT_DIR).as_posix()
        findings = scan_bypass_patterns(fp)
        if findings:
            bypass_by_file[rel] = findings

    # Genera output
    result: list[str] = []

    if output_markdown:
        result.append("# Audit import toolkit core\n")
        result.append(f"Generato da `scripts/audit-imports.py` su {len(py_files)} file.\n")
    else:
        result.append(f"Audit import toolkit core — {len(py_files)} file scansionati\n")
        result.append("=" * 60 + "\n")

    if not only_bypass:
        # --- Sezione 1: Consumer per provider ---
        if output_markdown:
            result.append("## Consumer per modulo core\n")
        else:
            result.append("CONSUMER PER MODULO CORE\n")
            result.append("-" * 40)

        for core_mod in sorted(CORE_MODULES):
            consumers = consumer_map.get(core_mod, [])
            if provider_filter and provider_filter not in core_mod:
                continue

            if output_markdown:
                result.append(f"\n### `{module_short(core_mod)}` ({len(consumers)} consumer)\n")
            else:
                result.append(f"\n{module_short(core_mod)} ({len(consumers)} import)")

            if not consumers:
                result.append("  (nessun consumer)")
                continue

            # Raggruppa per file consumer
            by_file: dict[str, list[tuple[int, str]]] = defaultdict(list)
            for filepath, line, name in consumers:
                if consumer_filter and consumer_filter not in filepath:
                    continue
                by_file[filepath].append((line, name))

            for filepath in sorted(by_file):
                # meglio: elenca ciò che importa
                imported = sorted(set(n for _, n in by_file[filepath]))
                if output_markdown:
                    result.append(f"  - `{filepath}` → {', '.join(f'`{n}`' for n in imported)}")
                else:
                    result.append(f"  {filepath}")
                    for n in imported:
                        result.append(f"    - {n}")

        # --- Sezione 2: File che non importano mai core (segnali) ---
        non_consumers: list[str] = []
        for fp in py_files:
            rel = fp.relative_to(TOOLKIT_DIR).as_posix()
            if consumer_filter and consumer_filter not in rel:
                continue
            imports = file_imports.get(rel, [])
            imports_core = [
                i
                for i in imports
                if any(i["module"] == m or i["module"].startswith(m + ".") for m in CORE_MODULES)
            ]
            if not imports_core and rel.startswith("toolkit"):
                non_consumers.append(rel)

        if non_consumers:
            if output_markdown:
                result.append(f"\n### File che NON importano core ({len(non_consumers)})\n")
                for f in sorted(non_consumers):
                    result.append(f"  - `{f}`")
            else:
                result.append(f"\nFILE CHE NON IMPORTANO CORE ({len(non_consumers)})\n")
                result.append("-" * 40)
                for f in sorted(non_consumers):
                    result.append(f"  {f}")

    # --- Sezione 3: Pattern bypass ---
    if bypass_by_file:
        if output_markdown:
            result.append("\n## Pattern bypass (non usa contratti core)\n")
        else:
            result.append("\nPATTERN BYPASS (non usa contratti core)\n")
            result.append("-" * 40)

        for filepath in sorted(bypass_by_file):
            if consumer_filter and consumer_filter not in filepath:
                continue
            findings = bypass_by_file[filepath]
            if output_markdown:
                result.append(f"\n### `{filepath}` ({len(findings)})\n")
                for f in findings:
                    result.append(f"  - Riga {f['line']}: `{f['code']}`")
                    result.append(f"    → {f['suggestion']}")
            else:
                result.append(f"\n{filepath}")
                for f in findings:
                    result.append(f"  Riga {f['line']:4d} | {f['pattern']}")
                    result.append(f"         {f['code']}")
                    result.append(f"         → {f['suggestion']}")

    total_imports = sum(len(v) for v in consumer_map.values())
    total_bypass = sum(len(v) for v in bypass_by_file.values())

    if output_markdown:
        result.append(
            f"\n---\n**Riepilogo**: {total_imports} import da core, {total_bypass} pattern bypass in {len(bypass_by_file)} file.\n"
        )
    else:
        result.append(
            f"\n--- Riepilogo: {total_imports} import da core, {total_bypass} bypass in {len(bypass_by_file)} file ---\n"
        )

    return "\n".join(result)


def main():
    flags = set(a for a in sys.argv[1:] if a.startswith("--"))

    consumer_filter = None
    provider_filter = None
    if "consumer" in str(sys.argv):
        idx = next(i for i, a in enumerate(sys.argv) if a == "--consumer")
        if idx + 1 < len(sys.argv):
            consumer_filter = sys.argv[idx + 1]
    if "provider" in str(sys.argv):
        idx = next(i for i, a in enumerate(sys.argv) if a == "--provider")
        if idx + 1 < len(sys.argv):
            provider_filter = sys.argv[idx + 1]

    output = run_audit(
        consumer_filter=consumer_filter,
        provider_filter=provider_filter,
        only_bypass="--bypass" in flags,
        output_markdown="--markdown" in flags,
    )
    print(output)


if __name__ == "__main__":
    main()
