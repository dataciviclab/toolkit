from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from lab_connectors.duckdb import safe_connect

from toolkit.core.artifacts import resolve_artifact_policy, should_write
from toolkit.core.config import ensure_dict
from toolkit.core.layer_profile import compare_layer_profiles, profile_relation, profile_parquet_files
from toolkit.core.metadata import config_hash_for_year, file_record, write_layer_manifest, write_metadata
from toolkit.core.multi_year_source import bind_multi_year_view, collect_multi_year_files
from toolkit.core.paths import layer_dataset_dir, layer_year_dir, resolve_root, resolve_sql_path, serialize_metadata_path
from toolkit.core.support import flatten_support_template_ctx, resolve_support_payloads
from toolkit.core.template import build_runtime_template_ctx, public_template_ctx, render_template


_CLEAN_INPUT_TOKEN_RE = re.compile(r"\bclean_input\b", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Multi-year mart tables (assorbe ex-cross_year)
# ---------------------------------------------------------------------------


def run_mart_multi_year(
    dataset: str,
    dataset_years: list[int],
    root: str | None,
    mart_cfg: dict[str, Any],
    logger,
    *,
    base_dir: Path | None = None,
    output_cfg: dict[str, Any] | None = None,
    support_cfg: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Run multi-year MART tables (tables with explicit ``years`` in config).

    Assorbe la funzionalita' dell'ex layer cross_year: ogni tabella
    con ``years`` viene eseguita una volta aggregando i parquet di
    tutti gli anni specificati.

    Output: ``data/mart/{dataset}/{name}.parquet`` (dataset-level, no year subdir).
    """
    mart_cfg = ensure_dict(mart_cfg)
    output_cfg = ensure_dict(output_cfg)
    support_cfg = ensure_dict(support_cfg)
    policy = resolve_artifact_policy(output_cfg)
    root_dir = resolve_root(root)
    multi_year_dir = layer_dataset_dir(root, "mart", dataset)
    multi_year_dir.mkdir(parents=True, exist_ok=True)

    tables = mart_cfg.get("tables") or []
    multi_year_tables = [
        t for t in tables
        if isinstance(t, dict) and t.get("years")
    ]
    if not multi_year_tables:
        return {"output_rows": 0, "output_bytes": 0, "tables_count": 0, "col_count": None}

    support_payloads = resolve_support_payloads(support_cfg, require_exists=True)
    base_ctx = build_runtime_template_ctx(
        dataset=dataset,
        year=dataset_years[0] if dataset_years else 0,
        root=root_dir,
        base_dir=base_dir,
        support=flatten_support_template_ctx(support_payloads),
    )
    years_csv = ",".join(str(y) for y in dataset_years)
    base_ctx["years"] = years_csv
    base_ctx["years_csv"] = years_csv

    run_dir: Path | None = None
    if should_write("mart", "rendered_sql", policy, {"output": output_cfg or {}}):
        run_dir = multi_year_dir / "_run"
        run_dir.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []
    executed: list[dict[str, Any]] = []
    total_rows = 0

    with safe_connect() as con:
        for i, table in enumerate(multi_year_tables, start=1):
            name = table.get("name")
            sql_rel = table.get("sql")
            if not name or not sql_rel:
                raise ValueError("Each mart multi-year table entry must include: name, sql")

            files = collect_multi_year_files(
                root, dataset,
                years=table.get("years", []),
                source_layer=table.get("source_layer", "clean"),
                source_table=table.get("source_table"),
            )
            bind_multi_year_view(con, files, source_layer=table.get("source_layer", "clean"))

            sql_path = resolve_sql_path(sql_rel, base_dir=base_dir)
            if not sql_path.exists():
                raise FileNotFoundError(f"MART multi-year SQL file not found: {sql_path}")

            sql = sql_path.read_text(encoding="utf-8")
            sql = render_template(sql, base_ctx)

            rendered_sql_path: Path | None = None
            if run_dir is not None:
                rendered_sql_path = run_dir / f"{i:02d}_{name}_rendered.sql"
                rendered_sql_path.write_text(sql, encoding="utf-8")

            con.execute(f"CREATE OR REPLACE TABLE {name} AS {sql}")
            output_profile = profile_relation(con, name)
            row_count = int(output_profile.get("row_count") or 0)
            total_rows += row_count

            out = multi_year_dir / f"{name}.parquet"
            con.execute(f"COPY {name} TO '{out}' (FORMAT PARQUET);")

            written.append(out)
            executed.append({
                "name": name,
                "sql": serialize_metadata_path(sql_path, base_dir),
                "sql_rendered": serialize_metadata_path(rendered_sql_path, root_dir),
                "output": serialize_metadata_path(out, root_dir),
                "years": table.get("years", []),
                "source_layer": table.get("source_layer", "clean"),
                "source_table": table.get("source_table"),
                "source_inputs": [serialize_metadata_path(p, root_dir) for p in files],
            })

    outputs = [file_record(p) for p in written]
    metadata_payload = {
        "layer": "mart_multi_year",
        "dataset": dataset,
        "years": dataset_years,
        "config_hash": config_hash_for_year(base_dir, dataset_years[0]) if dataset_years else None,
        "outputs": outputs,
        "output_paths": [serialize_metadata_path(p, root_dir) for p in written],
        "tables": executed,
    }
    metadata_path = write_metadata(multi_year_dir, metadata_payload)
    write_layer_manifest(
        multi_year_dir,
        metadata_path=metadata_path.name,
        validation_path=None,
        outputs=outputs,
        ok=None,
        errors_count=None,
        warnings_count=None,
    )
    total_bytes = sum(p.stat().st_size for p in written if p.exists())
    col_count = sum(
        len(t.get("output_profile", {}).get("columns", []))
        for t in executed if "output_profile" in t
    ) or None
    logger.info("MART multi-year -> %s (%d tables)", multi_year_dir, len(written))
    return {"output_rows": total_rows, "output_bytes": total_bytes, "tables_count": len(written), "col_count": col_count}


# ---------------------------------------------------------------------------
# Single-year mart tables
# ---------------------------------------------------------------------------


def run_mart(
    dataset: str,
    year: int,
    root: str | None,
    mart_cfg: dict,
    logger,
    *,
    base_dir: Path | None = None,
    clean_cfg: dict[str, Any] | None = None,
    output_cfg: dict[str, Any] | None = None,
    support_cfg: list[dict[str, Any]] | None = None,
):
    mart_cfg = ensure_dict(mart_cfg)
    clean_cfg = ensure_dict(clean_cfg)
    output_cfg = ensure_dict(output_cfg)
    support_cfg = ensure_dict(support_cfg)
    policy = resolve_artifact_policy(output_cfg)
    root_dir = resolve_root(root)
    clean_dir = layer_year_dir(root, "clean", dataset, year)
    mart_dir = layer_year_dir(root, "mart", dataset, year)
    mart_dir.mkdir(parents=True, exist_ok=True)

    clean_sql_configured = bool((clean_cfg or {}).get("sql"))
    clean_files: list[Path] = []
    if clean_sql_configured:
        if not clean_dir.exists():
            raise FileNotFoundError(
                f"CLEAN dir not found: {clean_dir}. Run: toolkit run clean -c dataset.yml"
            )
        clean_files = list(clean_dir.glob("*.parquet"))
        if not clean_files:
            raise FileNotFoundError(f"No CLEAN parquet found in {clean_dir}")
    clean_input_profile = profile_parquet_files(clean_files) if clean_files else None

    with safe_connect() as con:
        if clean_files:
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

        support_payloads = resolve_support_payloads(support_cfg, require_exists=True)
        template_ctx = build_runtime_template_ctx(
            dataset=dataset,
            year=year,
            root=root_dir,
            base_dir=base_dir,
            support=flatten_support_template_ctx(support_payloads),
        )

        run_dir: Path | None = None
        if should_write("mart", "rendered_sql", policy, {"output": output_cfg or {}}):
            run_dir = mart_dir / "_run"
            run_dir.mkdir(parents=True, exist_ok=True)

        written: list[Path] = []
        executed: list[dict[str, Any]] = []
        table_profiles: dict[str, Any] = {}
        transition_profiles: list[dict[str, Any]] = []
        total_rows = 0

        # Filtra tabelle multi-year: vengono eseguite da run_mart_multi_year()
        single_year_tables = [t for t in tables if isinstance(t, dict) and not t.get("years")]

        for i, table in enumerate(single_year_tables, start=1):
            if not isinstance(table, dict):
                raise ValueError("Each entry in mart.tables must be a mapping (dict).")

            name = table.get("name")
            sql_rel = table.get("sql")
            if not name or not sql_rel:
                raise ValueError("Each mart.tables entry must include: name, sql")

            sql_path = resolve_sql_path(sql_rel, base_dir=base_dir)
            if not sql_path.exists():
                raise FileNotFoundError(f"MART SQL file not found: {sql_path}")

            sql = sql_path.read_text(encoding="utf-8")
            sql = render_template(sql, template_ctx)

            if not clean_sql_configured and _CLEAN_INPUT_TOKEN_RE.search(sql):
                raise ValueError(
                    "MART SQL references clean_input but clean.sql is not configured in dataset.yml"
                )

            # Save rendered SQL for audit/debug
            rendered_sql_path: Path | None = None
            if run_dir is not None:
                rendered_sql_path = run_dir / f"{i:02d}_{name}_rendered.sql"
                rendered_sql_path.write_text(sql, encoding="utf-8")

            # Create table and export
            con.execute(f"CREATE OR REPLACE TABLE {name} AS {sql}")
            output_profile = profile_relation(con, name)
            row_count = int(output_profile.get("row_count") or 0)
            total_rows += row_count

            out = mart_dir / f"{name}.parquet"
            con.execute(f"COPY {name} TO '{out}' (FORMAT PARQUET);")

            written.append(out)
            table_profiles[name] = output_profile
            transition_profile = compare_layer_profiles(
                clean_input_profile,
                output_profile,
                source_layer="clean",
                target_layer="mart",
                target_name=name,
            )
            if transition_profile is not None:
                transition_profiles.append(transition_profile)
            executed.append(
                {
                    "name": name,
                    "sql": serialize_metadata_path(sql_path, base_dir),
                    "sql_rendered": serialize_metadata_path(rendered_sql_path, root_dir),
                    "output": serialize_metadata_path(out, root_dir),
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
        "output_paths": [serialize_metadata_path(p, root_dir) for p in written],
        "template_ctx": public_template_ctx(template_ctx),
        "tables": executed,
        "clean_input_profile": clean_input_profile,
        "table_profiles": table_profiles,
        "transition_profiles": transition_profiles,
    }
    metadata_path = write_metadata(
        mart_dir,
        metadata_payload,
    )
    write_layer_manifest(
        mart_dir,
        metadata_path=metadata_path.name,
        validation_path="_validate/mart_validation.json",
        outputs=outputs,
        ok=None,
        errors_count=None,
        warnings_count=None,
    )
    total_bytes = sum(p.stat().st_size for p in written if p.exists())
    col_count = sum(len(tp.get("columns", [])) for tp in table_profiles.values()) if table_profiles else None
    logger.info(f"MART -> {mart_dir}")
    return {"output_rows": total_rows, "output_bytes": total_bytes, "tables_count": len(written), "col_count": col_count}
