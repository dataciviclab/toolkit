from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb

from toolkit.core.metadata import write_metadata
from toolkit.core.paths import layer_year_dir
from toolkit.core.template import render_template


def run_mart(
    dataset: str,
    year: int,
    root: str | None,
    mart_cfg: dict,
    logger,
    *,
    base_dir: Path | None = None,
):
    clean_dir = layer_year_dir(root, "clean", dataset, year)
    mart_dir = layer_year_dir(root, "mart", dataset, year)
    mart_dir.mkdir(parents=True, exist_ok=True)

    if not clean_dir.exists():
        raise FileNotFoundError(f"CLEAN dir not found: {clean_dir}. Run: toolkit run clean -c dataset.yml")

    clean_files = list(clean_dir.glob("*.parquet"))
    if not clean_files:
        raise FileNotFoundError(f"No CLEAN parquet found in {clean_dir}")

    con = duckdb.connect(":memory:")

    # clean_input view
    if len(clean_files) == 1:
        con.execute(f"CREATE VIEW clean_input AS SELECT * FROM read_parquet('{clean_files[0]}')")
    else:
        paths = ",".join([f"'{p}'" for p in clean_files])
        con.execute(f"CREATE VIEW clean_input AS SELECT * FROM read_parquet([{paths}])")

    # alias for backward-compatible SQL (old templates may reference "clean")
    con.execute("CREATE OR REPLACE VIEW clean AS SELECT * FROM clean_input")

    tables = mart_cfg.get("tables") or []
    if not isinstance(tables, list) or not tables:
        raise ValueError("mart.tables missing or empty in dataset.yml")

    template_ctx = {"year": year, "dataset": dataset}

    run_dir = mart_dir / "_run"
    run_dir.mkdir(parents=True, exist_ok=True)

    written: list[str] = []
    executed: list[dict[str, Any]] = []

    for i, table in enumerate(tables, start=1):
        if not isinstance(table, dict):
            raise ValueError("Each entry in mart.tables must be a mapping (dict).")

        name = table.get("name")
        sql_rel = table.get("sql")
        if not name or not sql_rel:
            raise ValueError("Each mart.tables entry must include: name, sql")

        sql_path = Path(sql_rel)
        if base_dir and not sql_path.is_absolute():
            sql_path = base_dir / sql_path
        if not sql_path.exists():
            raise FileNotFoundError(f"MART SQL file not found: {sql_path}")

        sql = sql_path.read_text(encoding="utf-8")
        sql = render_template(sql, template_ctx)

        # Save rendered SQL for audit/debug
        rendered_sql_path = run_dir / f"{i:02d}_{name}_rendered.sql"
        rendered_sql_path.write_text(sql, encoding="utf-8")

        # Create table and export
        con.execute(f"CREATE OR REPLACE TABLE {name} AS {sql}")

        out = mart_dir / f"{name}.parquet"
        con.execute(f"COPY {name} TO '{out}' (FORMAT PARQUET);")

        written.append(str(out))
        executed.append(
            {
                "name": name,
                "sql": str(sql_path),
                "sql_rendered": str(rendered_sql_path),
                "output": str(out),
            }
        )

    write_metadata(
        mart_dir,
        {
            "layer": "mart",
            "dataset": dataset,
            "year": year,
            "template_ctx": template_ctx,
            "tables": executed,
            "outputs": written,
        },
    )
    logger.info(f"MART → {mart_dir}")