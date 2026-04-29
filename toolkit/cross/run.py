from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb

from toolkit.core.artifacts import ARTIFACT_POLICY_DEBUG, resolve_artifact_policy, should_write
from toolkit.core.metadata import file_record, sha256_bytes, write_layer_manifest, write_metadata
from toolkit.core.paths import layer_dataset_dir, layer_year_dir, resolve_root, resolve_sql_path, serialize_metadata_path
from toolkit.core.template import render_template


def _config_hash(base_dir: Path | None) -> str | None:
    if base_dir is None:
        return None
    path = Path(base_dir) / "dataset.yml"
    if not path.exists():
        return None
    return sha256_bytes(path.read_bytes())


def _source_files(root: str | None, dataset: str, years: list[int], table_cfg: dict[str, Any]) -> list[Path]:
    source_layer = table_cfg.get("source_layer", "clean")
    source_table = table_cfg.get("source_table")
    files: list[Path] = []

    if source_layer == "clean":
        for year in years:
            clean_dir = layer_year_dir(root, "clean", dataset, year)
            if not clean_dir.exists():
                raise FileNotFoundError(f"CLEAN dir not found: {clean_dir}. Run: toolkit run clean -c dataset.yml")
            year_files = sorted(clean_dir.glob("*.parquet"))
            if not year_files:
                raise FileNotFoundError(f"No CLEAN parquet found in {clean_dir}")
            files.extend(year_files)
        return files

    if source_layer == "mart":
        if not source_table:
            raise ValueError("cross_year.tables[].source_table is required when source_layer = mart")
        for year in years:
            mart_file = layer_year_dir(root, "mart", dataset, year) / f"{source_table}.parquet"
            if not mart_file.exists():
                raise FileNotFoundError(
                    f"MART parquet not found: {mart_file}. Run: toolkit run mart -c dataset.yml"
                )
            files.append(mart_file)
        return files

    raise ValueError(f"Unsupported cross_year source_layer: {source_layer}")


def _bind_source_view(con: duckdb.DuckDBPyConnection, files: list[Path], source_layer: str) -> None:
    if len(files) == 1:
        source_expr = f"read_parquet('{files[0]}')"
    else:
        paths = ",".join(f"'{path}'" for path in files)
        source_expr = f"read_parquet([{paths}])"

    con.execute(f"CREATE OR REPLACE VIEW source_input AS SELECT * FROM {source_expr}")
    con.execute(f"CREATE OR REPLACE VIEW {source_layer}_input AS SELECT * FROM source_input")
    con.execute(f"CREATE OR REPLACE VIEW {source_layer} AS SELECT * FROM source_input")
    con.execute(f"CREATE OR REPLACE VIEW {source_layer}_all_years AS SELECT * FROM source_input")


def run_cross_year(
    dataset: str,
    years: list[int],
    root: str | None,
    cross_year_cfg: dict[str, Any],
    logger,
    *,
    base_dir: Path | None = None,
    output_cfg: dict[str, Any] | None = None,
) -> None:
    policy = resolve_artifact_policy(output_cfg)
    root_dir = resolve_root(root)
    cross_dir = layer_dataset_dir(root, "cross", dataset)
    cross_dir.mkdir(parents=True, exist_ok=True)

    tables = cross_year_cfg.get("tables") or []
    if not isinstance(tables, list) or not tables:
        raise ValueError("cross_year.tables missing or empty in dataset.yml")

    con = duckdb.connect(":memory:")
    try:
        template_ctx = {
            "dataset": dataset,
            "years": ",".join(str(year) for year in years),
            "years_csv": ",".join(str(year) for year in years),
        }

        run_dir: Path | None = None
        if should_write("mart", "rendered_sql", policy, {"output": output_cfg or {}}):
            run_dir = cross_dir / "_run"
            run_dir.mkdir(parents=True, exist_ok=True)

        written: list[Path] = []
        executed: list[dict[str, Any]] = []
        debug_tables: list[dict[str, Any]] = []

        for i, table in enumerate(tables, start=1):
            if not isinstance(table, dict):
                raise ValueError("Each entry in cross_year.tables must be a mapping (dict).")

            name = table.get("name")
            sql_rel = table.get("sql")
            source_layer = table.get("source_layer", "clean")
            if not name or not sql_rel:
                raise ValueError("Each cross_year.tables entry must include: name, sql")

            files = _source_files(root, dataset, years, table)
            _bind_source_view(con, files, source_layer)

            sql_path = resolve_sql_path(sql_rel, base_dir=base_dir)
            if not sql_path.exists():
                raise FileNotFoundError(f"CROSS_YEAR SQL file not found: {sql_path}")

            sql = render_template(sql_path.read_text(encoding="utf-8"), template_ctx)

            rendered_sql_path: Path | None = None
            if run_dir is not None:
                rendered_sql_path = run_dir / f"{i:02d}_{name}_rendered.sql"
                rendered_sql_path.write_text(sql, encoding="utf-8")

            con.execute(f"CREATE OR REPLACE TABLE {name} AS {sql}")
            out = cross_dir / f"{name}.parquet"
            con.execute(f"COPY {name} TO '{out}' (FORMAT PARQUET);")

            written.append(out)
            executed.append(
                {
                    "name": name,
                    "sql": serialize_metadata_path(sql_path, base_dir),
                    "sql_rendered": serialize_metadata_path(rendered_sql_path, root_dir),
                    "output": serialize_metadata_path(out, root_dir),
                    "source_layer": source_layer,
                    "source_table": table.get("source_table"),
                    "source_inputs": [serialize_metadata_path(path, root_dir) for path in files],
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
    finally:
        con.close()

    outputs = [file_record(path) for path in written]
    metadata_payload = {
        "layer": "cross",
        "dataset": dataset,
        "years": years,
        "config_hash": _config_hash(base_dir),
        "outputs": outputs,
        "output_paths": [serialize_metadata_path(path, root_dir) for path in written],
        "tables": executed,
    }
    if policy == ARTIFACT_POLICY_DEBUG:
        metadata_payload["debug"] = {
            "output_root_absolute": str(root_dir.resolve()),
            "tables": debug_tables,
        }
    metadata_path = write_metadata(cross_dir, metadata_payload)
    write_layer_manifest(
        cross_dir,
        metadata_path=metadata_path.name,
        validation_path=None,
        outputs=outputs,
        ok=None,
        errors_count=None,
        warnings_count=None,
    )
    logger.info("CROSS_YEAR -> %s", cross_dir)
