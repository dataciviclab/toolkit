from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb

from toolkit.clean.duckdb_read import (
    SUPPORTED_INPUT_EXTS,
    read_raw_to_relation,
    resolve_clean_read_cfg,
    sql_path,
)
from toolkit.clean.input_selection import select_raw_input
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


def _run_sql(
    input_files: list[Path],
    sql_query: str,
    output_path: Path,
    *,
    read_cfg: dict[str, Any] | None = None,
    read_mode: str = "fallback",
    logger=None,
) -> tuple[str, dict[str, Any]]:
    con = duckdb.connect(":memory:")
    try:
        read_info = read_raw_to_relation(con, input_files, read_cfg, read_mode, logger)
        con.execute(f"CREATE TABLE clean_out AS {sql_query}")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        con.execute(
            f"COPY clean_out TO '{sql_path(output_path)}' (FORMAT PARQUET);"
        )
        return read_info.source, read_info.params_used
    finally:
        con.close()


def run_clean(
    dataset: str,
    year: int,
    root: str | None,
    clean_cfg: dict,
    logger,
    *,
    base_dir: Path | None = None,
    output_cfg: dict[str, Any] | None = None,
):
    policy = resolve_artifact_policy(output_cfg)
    root_dir = resolve_root(root)
    raw_dir = layer_year_dir(root, "raw", dataset, year)
    out_dir = layer_year_dir(root, "clean", dataset, year)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not raw_dir.exists():
        raise FileNotFoundError(f"RAW dir not found: {raw_dir}. Run: toolkit run raw -c dataset.yml")

    read_mode = str(clean_cfg.get("read_mode", "fallback"))
    read_cfg, relation_read_cfg, read_params_source = resolve_clean_read_cfg(raw_dir, clean_cfg, logger)
    selection_mode = read_cfg.get("mode")
    glob_pattern = read_cfg.get("glob", "*")
    prefer_from_raw_run = bool(read_cfg.get("prefer_from_raw_run", True))
    allow_ambiguous = bool(read_cfg.get("allow_ambiguous", False))

    sql_rel = clean_cfg.get("sql")
    if not sql_rel:
        raise ValueError("clean.sql missing in dataset.yml (expected: clean: { sql: 'sql/clean.sql' })")

    sql_path_obj = Path(sql_rel)
    if not sql_path_obj.exists():
        raise FileNotFoundError(f"CLEAN SQL file not found: {sql_path_obj}")

    sql = sql_path_obj.read_text(encoding="utf-8")
    template_ctx = {"year": year, "dataset": dataset}
    sql = render_template(sql, template_ctx)

    rendered_sql_path: Path | None = None
    if should_write("clean", "rendered_sql", policy, {"output": output_cfg or {}}):
        run_dir = out_dir / "_run"
        run_dir.mkdir(parents=True, exist_ok=True)
        rendered_sql_path = run_dir / "clean_rendered.sql"
        rendered_sql_path.write_text(sql, encoding="utf-8")

    if selection_mode is None and read_cfg.get("include") is not None:
        selection_mode = "explicit"
        allow_ambiguous = True
    elif selection_mode is None:
        logger.warning(
            "CLEAN input selection defaulting to largest file (legacy). "
            "Set clean.read.mode explicitly to avoid ambiguity."
        )
        selection_mode = "largest"

    input_files = select_raw_input(
        raw_dir,
        logger,
        mode=str(selection_mode),
        root=root,
        dataset=dataset,
        year=year,
        glob=glob_pattern,
        prefer_from_raw_run=prefer_from_raw_run,
        include=read_cfg.get("include"),
        allow_ambiguous=allow_ambiguous,
    )
    if not input_files:
        raise FileNotFoundError(
            f"No usable RAW files found in {raw_dir}. "
            f"Expected one of: {sorted(SUPPORTED_INPUT_EXTS)}"
        )

    if len(input_files) > 1 and selection_mode != "all":
        raise ValueError(
            "CLEAN input selection returned multiple files for a single-file mode. "
            "Use clean.read.mode=all to read multiple inputs."
        )

    if len(input_files) == 1:
        logger.info(f"CLEAN selected RAW input -> {input_files[0]}")
    else:
        logger.info("CLEAN selected RAW inputs -> %s", [str(path) for path in input_files])

    output_path = out_dir / f"{dataset}_{year}_clean.parquet"
    read_source_used, read_params_used = _run_sql(
        input_files,
        sql,
        output_path,
        read_cfg=relation_read_cfg,
        read_mode=read_mode,
        logger=logger,
    )

    outputs = [file_record(output_path)]
    metadata_payload = {
        "layer": "clean",
        "dataset": dataset,
        "year": year,
        "sql": _serialize_metadata_path(sql_path_obj, base_dir),
        "sql_rendered": _serialize_metadata_path(rendered_sql_path, root_dir),
        "template_ctx": template_ctx,
        "read": clean_cfg.get("read"),
        "read_mode": read_mode,
        "read_params_source": read_params_source,
        "read_source_used": read_source_used,
        "read_params_used": read_params_used,
        "config_hash": config_hash_for_year(base_dir, year),
        "inputs": [file_record(p) for p in input_files],
        "outputs": outputs,
        "input_files": [p.name for p in input_files],
    }
    if policy == ARTIFACT_POLICY_DEBUG:
        metadata_payload["debug"] = {
            "sql_absolute": str(sql_path_obj.resolve()),
            "sql_rendered_absolute": str(rendered_sql_path.resolve()) if rendered_sql_path else None,
            "output_root_absolute": str(root_dir.resolve()),
        }
    metadata_path = write_metadata(
        out_dir,
        metadata_payload,
    )
    write_manifest(
        out_dir,
        metadata_path=metadata_path.name,
        validation_path="_validate/clean_validation.json",
        outputs=outputs,
        ok=None,
        errors_count=None,
        warnings_count=None,
    )
    logger.info(f"CLEAN -> {output_path}")
