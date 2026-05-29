"""Shared test helpers for toolkit tests.

Functions here are importable by any test file::

    from tests.helpers import NoopLogger, make_config, make_dataset_yml, make_standard_sql, write_text, write_parquet

Fixtures belong in ``conftest.py``; pure helpers that take arguments
and return values belong here.
"""
from __future__ import annotations

import textwrap
from pathlib import Path
from typing import Any


class NoopLogger:
    """Logger finto che non stampa nulla. Usato nei test che richiedono un logger ma non ne verificano l'output."""
    def debug(self, *_a, **_kw): return None
    def info(self, *_a, **_kw): return None
    def warning(self, *_a, **_kw): return None
    def error(self, *_a, **_kw): return None


def write_parquet(path: Path, sql: str, *, table: str = "t") -> None:
    """Crea un parquet da una query SQL DuckDB in memoria.
    
    Args:
        path: Output path per il parquet.
        sql: CREATE TABLE + INSERT (es. ``CREATE TABLE t AS SELECT 1 AS x``).
        table: Nome della tabella temporanea (default ``t``).
    """
    import duckdb
    con = duckdb.connect(":memory:")
    con.execute(sql)
    con.execute(f"COPY {table} TO '{path.as_posix()}' (FORMAT 'parquet')")
    con.close()


def write_text(path: Path, content: str) -> None:
    """Write *content* (dedented, stripped, newline-terminated) to *path*.

    Creates parent directories as needed.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(content).strip() + "\n", encoding="utf-8")


def make_config(
    *,
    base_dir: Path | None = None,
    root: Path | None = None,
    dataset: str = "test",
    years: list[int] | None = None,
    source_id: str | None = None,
    raw: dict[str, Any] | None = None,
    clean: dict[str, Any] | None = None,
    mart: dict[str, Any] | None = None,
    support: list[dict[str, Any]] | None = None,
) -> Any:
    """Create a ToolkitConfig in-memory without writing any file.

    Defaults to a minimal valid config (raw: {}, clean: {}, mart without
    tables). Override any section by passing a dict with the same shape
    as the YAML section.

    Returns a ``ToolkitConfig`` with real Pydantic models — no dict mocks.
    All attribute access (``cfg.clean.sql``, ``cfg.mart.tables``) works.
    """
    from toolkit.core.config_models import ToolkitConfigModel, DatasetBlock, RawConfig, CleanConfig, MartConfig

    _root = root or Path("/tmp/toolkit-test-root")
    _base = base_dir or _root
    _years = years or [2024]

    model = ToolkitConfigModel(
        base_dir=_base,
        root=_root,
        root_source="test",
        dataset=DatasetBlock(name=dataset, years=_years, source_id=source_id),
        raw=RawConfig.model_validate(raw or {}),
        clean=CleanConfig.model_validate(clean or {}),
        mart=MartConfig.model_validate(mart or {}),
        support=support or [],
    )

    from toolkit.core.config import ToolkitConfig
    return ToolkitConfig(
        base_dir=model.base_dir,
        schema_version=model.schema_version,
        root=model.root,
        root_source=model.root_source,
        dataset=model.dataset.name,
        source_id=model.dataset.source_id,
        years=list(model.dataset.years),
        time_coverage=model.dataset.time_coverage,
        _model=model,
    )


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
