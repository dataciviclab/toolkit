"""Shared test helpers for toolkit tests.

Functions here are importable by any test file::

    from tests.helpers import make_dataset_yml, make_standard_sql, write_text

Fixtures belong in ``conftest.py``; pure helpers that take arguments
and return values belong here.
"""
from __future__ import annotations

import textwrap
from pathlib import Path


def write_text(path: Path, content: str) -> None:
    """Write *content* (dedented, stripped, newline-terminated) to *path*.

    Creates parent directories as needed.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(content).strip() + "\n", encoding="utf-8")


def make_standard_sql(base_dir: Path, /) -> dict[str, Path]:
    """Create standard ``sql/clean.sql`` and ``sql/mart/mart_example.sql``.

    Returns dict with keys ``clean``, ``mart_dir``, ``mart``.
    """
    sql_dir = base_dir / "sql" / "mart"
    sql_dir.mkdir(parents=True, exist_ok=True)
    clean = base_dir / "sql" / "clean.sql"
    clean.write_text("select 1 as value", encoding="utf-8")
    mart = sql_dir / "mart_example.sql"
    mart.write_text("select * from clean_input", encoding="utf-8")
    return {"clean": clean, "mart_dir": sql_dir, "mart": mart}


def make_dataset_yml(
    path: Path,
    *,
    name: str = "demo_ds",
    root: Path | None = None,
    years: list[int] | None = None,
    clean_sql: str = "sql/clean.sql",
    mart_tables: list[tuple[str, str]] | None = None,
    extra: str | None = None,
) -> Path:
    """Write a minimal ``dataset.yml`` config file.

    Args:
        path: Output path.
        name: Dataset slug (``dataset: name``).
        root: Root directory (defaults to parent of *path* + ``out``).
        years: List of years (default ``[2022]``).
        clean_sql: Relative path to clean SQL.
        mart_tables: ``[(table_name, sql_path), ...]``.
        extra: Extra raw YAML lines appended before closing newline.

    Returns:
        *path* for chaining.

    Example::

        yml = make_dataset_yml(
            tmp_path / "dataset.yml",
            name="demo",
            mart_tables=[("m1", "sql/m1.sql")],
        )
    """
    root_val = root or (path.parent / "out")
    yml_years = years or [2022]
    yml_years_str = ", ".join(str(y) for y in yml_years)

    lines: list[str] = [
        f'root: "{root_val.as_posix()}"',
        "dataset:",
        f'  name: "{name}"',
        f"  years: [{yml_years_str}]",
        "raw: {}",
    ]

    if clean_sql is not None:
        lines.append("clean:")
        lines.append(f'  sql: "{clean_sql}"')

    if mart_tables:
        lines.append("mart:")
        lines.append("  tables:")
        for table_name, table_sql in mart_tables:
            lines.append(f'    - name: "{table_name}"')
            lines.append(f'      sql: "{table_sql}"')

    if extra:
        lines.append(extra)

    body = "\n".join(lines) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    return path
