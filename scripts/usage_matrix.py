from __future__ import annotations

import argparse
from pathlib import Path


TARGETS = {
    "dataset.test.yml": "dataset.test.yml",
    "clean_ispra_test.sql": "clean_ispra_test.sql",
}

SKIP_DIRS = {
    ".git",
    ".pytest_cache",
    ".ruff_cache",
    ".mypy_cache",
    "__pycache__",
    "dataciviclab_toolkit.egg-info",
}

TEXT_EXTS = {
    ".py",
    ".md",
    ".toml",
    ".yml",
    ".yaml",
    ".json",
    ".ini",
    ".cfg",
    ".txt",
    ".sql",
    ".gitignore",
    ".editorconfig",
}


def _is_text_file(path: Path) -> bool:
    return path.suffix.lower() in TEXT_EXTS or path.name in {".gitignore", ".editorconfig"}


def _iter_repo_files(root: Path) -> list[Path]:
    files: list[Path] = []
    self_path = Path(__file__).resolve()
    for path in root.rglob("*"):
        if any(part in SKIP_DIRS for part in path.parts):
            continue
        if path.resolve() == self_path:
            continue
        if path.is_file() and _is_text_file(path):
            files.append(path)
    return sorted(files)


def _scan_file(path: Path, root: Path) -> dict[str, list[tuple[str, str]]]:
    matches: dict[str, list[tuple[str, str]]] = {label: [] for label in TARGETS}
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        text = path.read_text(encoding="utf-8", errors="replace")

    for lineno, line in enumerate(text.splitlines(), start=1):
        for label, needle in TARGETS.items():
            if needle in line:
                relpath = path.relative_to(root).as_posix()
                matches[label].append((f"{relpath}:{lineno}", line.strip()))
    return {label: refs for label, refs in matches.items() if refs}


def build_report(root: Path) -> str:
    references: dict[str, list[tuple[str, str]]] = {label: [] for label in TARGETS}

    for path in _iter_repo_files(root):
        file_matches = _scan_file(path, root)
        for label, refs in file_matches.items():
            references[label].extend(refs)

    lines: list[str] = []
    lines.append("# Dev Usage Matrix")
    lines.append("")
    lines.append(f"Scanned root: `{root}`")
    lines.append("")

    for label in TARGETS:
        lines.append(f"## `{label}`")
        lines.append("")
        refs = references[label]
        if not refs:
            lines.append("_No references found._")
            lines.append("")
            continue

        lines.append("| Referencing File | Snippet |")
        lines.append("| --- | --- |")
        for location, snippet in refs:
            escaped = snippet.replace("|", "\\|").replace("`", "\\`")
            lines.append(f"| `{location}` | `{escaped}` |")
        lines.append("")

    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate an internal usage matrix for selected files/symbols.")
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="Repository root to scan.",
    )
    parser.add_argument(
        "--write",
        type=Path,
        default=None,
        help="Optional path to write the markdown report instead of stdout.",
    )
    args = parser.parse_args()

    root = args.root.resolve()
    report = build_report(root)

    if args.write is not None:
        out = args.write
        if not out.is_absolute():
            out = root / out
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(report, encoding="utf-8")
    else:
        print(report, end="")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
