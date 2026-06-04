#!/usr/bin/env python3
"""Generate CONTRACTS.md — mappa delle funzioni pubbliche nei moduli centrali.

Scansiona toolkit/core/ (esclusi __init__, config_models, manifest),
toolkit/plugins/_http_utils.py e toolkit/profile/raw.py.
Per ogni funzione/class pubblica (non ``_``-prefissa) estrae nome,
firma e prima riga di docstring.

Uso:
    python scripts/generate-contracts-map.py        # stampa su stdout
    python scripts/generate-contracts-map.py --write # sovrascrive CONTRACTS.md
"""

from __future__ import annotations

import ast
import pathlib
import re
import sys

TOOLKIT_DIR = pathlib.Path(__file__).resolve().parent.parent

# Moduli da scandire: path relativo al toolkit
SCAN_PATHS: list[str] = [
    "toolkit/core/*.py",
    "toolkit/plugins/_http_utils.py",
    "toolkit/profile/raw.py",
]

EXCLUDE_FILES = {
    "__init__.py",
    "manifest.py",
}

EXCLUDE_DIRS = {
    "config_models",
}


def _first_docstring(node: ast.AST) -> str:
    """Estrae la prima riga della docstring da un body."""
    if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        return ""
    if not node.body:
        return ""
    first = node.body[0]
    if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant):
        text = first.value.value
        if isinstance(text, str):
            # prima riga significativa
            line = text.strip().split("\n")[0]
            if len(line) > 80:
                line = line[:77] + "..."
            return line
    return ""


def _is_public(name: str) -> bool:
    return not name.startswith("_")


def scan_file(filepath: pathlib.Path) -> list[dict]:
    entries: list[dict] = []

    try:
        source = filepath.read_text(encoding="utf-8")
    except Exception:
        return entries

    try:
        tree = ast.parse(source)
    except SyntaxError:
        return entries

    rel_path = filepath.relative_to(TOOLKIT_DIR).as_posix()

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and _is_public(node.name):
            doc = _first_docstring(node)
            args = ast.unparse(node.args) if hasattr(node, "args") else "(...)"
            # pulisci args: rimuovi self/
            if args.startswith("self"):
                args = args[4:].lstrip(", ")
            sig = f"{node.name}({args})"
            entries.append({
                "kind": "fun",
                "name": node.name,
                "signature": sig,
                "doc": doc,
                "file": rel_path,
                "line": node.lineno,
            })
        elif isinstance(node, ast.ClassDef) and _is_public(node.name):
            doc = _first_docstring(node)
            entries.append({
                "kind": "class",
                "name": node.name,
                "signature": node.name,
                "doc": doc,
                "file": rel_path,
                "line": node.lineno,
            })
            # metodi pubblici della classe
            for item in node.body:
                if isinstance(item, ast.FunctionDef) and _is_public(item.name) and item.name != "__init__":
                    mdoc = _first_docstring(item)
                    margs = ast.unparse(item.args) if hasattr(item, "args") else "(...)"
                    # skip self
                    if margs.startswith("self"):
                        margs = margs[4:].lstrip(", ")
                    entries.append({
                        "kind": "method",
                        "name": f"  {item.name}",
                        "signature": f"  {item.name}({margs})",
                        "doc": mdoc,
                        "file": rel_path,
                        "line": item.lineno,
                    })

    return entries


def generate() -> str:
    lines: list[str] = []
    lines.append("# Mappa dei contratti centrali del toolkit")
    lines.append("")
    lines.append("Generata automaticamente da `scripts/generate-contracts-map.py`.")
    lines.append("Elenco delle funzioni pubbliche nei moduli centrali (`toolkit/core/`,")
    lines.append("`toolkit/plugins/_http_utils.py`, `toolkit/profile/raw.py`).")
    lines.append("")
    lines.append("Consulta questa mappa **prima** di implementare una nuova funzione")
    lines.append("per verificare se il contratto esiste già.")
    lines.append("")
    lines.append(f"Totale: {{count}} funzioni/metodi in {{files}} file.")
    lines.append("")

    # Raccogli per file
    all_entries: list[dict] = []
    all_files: set[str] = set()

    for pattern in SCAN_PATHS:
        for path in TOOLKIT_DIR.glob(pattern):
            if path.name in EXCLUDE_FILES:
                continue
            if path.parent.name in EXCLUDE_DIRS:
                continue
            if not path.is_file() or path.suffix != ".py":
                continue

            entries = scan_file(path)
            all_entries.extend(entries)
            if entries:
                all_files.add(path.relative_to(TOOLKIT_DIR).as_posix())

    # Raggruppa per file
    by_file: dict[str, list[dict]] = {}
    for e in all_entries:
        by_file.setdefault(e["file"], []).append(e)

    for filepath in sorted(by_file):
        lines.append(f"## `{filepath}`")
        lines.append("")
        for e in by_file[filepath]:
            if e["doc"]:
                lines.append(f"- **{e['signature']}** — {e['doc']}")
            else:
                lines.append(f"- **{e['signature']}**")
        lines.append("")

    result = "\n".join(lines)
    result = result.replace("{count}", str(len(all_entries)))
    result = result.replace("{files}", str(len(all_files)))

    return result


def main():
    output = generate()

    if "--write" in sys.argv:
        dest = TOOLKIT_DIR / "CONTRACTS.md"
        dest.write_text(output, encoding="utf-8")
        print(f"Scritto {dest} ({len(output)} byte)")
    else:
        print(output)


if __name__ == "__main__":
    main()
