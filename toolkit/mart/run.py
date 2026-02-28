from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb

from toolkit.core.artifacts import ARTIFACT_POLICY_DEBUG, resolve_artifact_policy, should_write
from toolkit.core.metadata import config_hash_for_year, file_record, write_manifest, write_metadata
from toolkit.core.paths import layer_year_dir, resolve_root, to_root_relative
from toolkit.core.template import render_template


def _serialize_metadata_path(path: Path | None, rel_root: Path | None) -> str | None:
    if path is None:
        return None
    if rel_root is None:
        return path.as_posix()
    return to_root_relative(path, rel_root)


def run_mart(
    dataset: str,
    year: int,
    root: str | None,
    mart_cfg: dict,
    logger,
    *,
    base_dir: Path | None = None,
    output_cfg: dict[str, Any] | None = None,
):
    policy = resolve_artifact_policy(output_cfg)
    root_dir = resolve_root(root)
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

    run_dir: Path | None = None
    if should_write("mart", "rendered_sql", policy, {"output": output_cfg or {}}):
        run_dir = mart_dir / "_run"
        run_dir.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []
    executed: list[dict[str, Any]] = []
    debug_tables: list[dict[str, Any]] = []

    for i, table in enumerate(tables, start=1):
        if not isinstance(table, dict):
            raise ValueError("Each entry in mart.tables must be a mapping (dict).")

        name = table.get("name")
        sql_rel = table.get("sql")
        if not name or not sql_rel:
            raise ValueError("Each mart.tables entry must include: name, sql")

        sql_path = Path(sql_rel)
        if not sql_path.exists():
            raise FileNotFoundError(f"MART SQL file not found: {sql_path}")

        sql = sql_path.read_text(encoding="utf-8")
        sql = render_template(sql, template_ctx)

        # Save rendered SQL for audit/debug
        rendered_sql_path: Path | None = None
        if run_dir is not None:
            rendered_sql_path = run_dir / f"{i:02d}_{name}_rendered.sql"
            rendered_sql_path.write_text(sql, encoding="utf-8")

        # Create table and export
        con.execute(f"CREATE OR REPLACE TABLE {name} AS {sql}")

        out = mart_dir / f"{name}.parquet"
        con.execute(f"COPY {name} TO '{out}' (FORMAT PARQUET);")

        written.append(out)
        executed.append(
            {
                "name": name,
                "sql": _serialize_metadata_path(sql_path, base_dir),
                "sql_rendered": _serialize_metadata_path(rendered_sql_path, root_dir),
                "output": _serialize_metadata_path(out, root_dir),
            }
        )
        if policy == ARTIFACT_POLICY_DEBUG:
            debug_tables.append(
                {
                    "name": name,
                    "sql_absolute": str(sql_path.resolve()),
                    "sql_rendered_absolute": str(rendered_sql_path.resolve()) if rendered_sql_path else None,
                    "output_absolute": str(out.resolve()),
                }
            )

    outputs = [file_record(p) for p in written]
    metadata_payload = {
        "layer": "mart",
        "dataset": dataset,
        "year": year,
        "config_hash": config_hash_for_year(base_dir, year),
        "inputs": [file_record(p) for p in clean_files],
        "outputs": outputs,
        "output_paths": [_serialize_metadata_path(p, root_dir) for p in written],
        "template_ctx": template_ctx,
        "tables": executed,
    }
    if policy == ARTIFACT_POLICY_DEBUG:
        metadata_payload["debug"] = {
            "output_root_absolute": str(root_dir.resolve()),
            "tables": debug_tables,
        }
    metadata_path = write_metadata(
        mart_dir,
        metadata_payload,
    )
    write_manifest(
        mart_dir,
        metadata_path=metadata_path.name,
        validation_path="_validate/mart_validation.json",
        outputs=outputs,
        ok=None,
        errors_count=None,
        warnings_count=None,
    )
    logger.info(f"MART -> {mart_dir}")
