from __future__ import annotations

import re
from typing import Any

import duckdb

from toolkit.clean.run import _load_clean_sql
from toolkit.core.template import render_template
from toolkit.mart.run import _resolve_sql_path as _resolve_mart_sql_path

_QUOTED_IDENTIFIER_RE = re.compile(r'"([^"]+)"')


def _dedupe_preserve_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if not item or item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _placeholder_columns(clean_cfg: dict[str, Any], sql: str) -> list[str]:
    columns: list[str] = []
    read_cfg = clean_cfg.get("read") or {}
    read_columns = read_cfg.get("columns") or {}
    if isinstance(read_columns, dict):
        columns.extend(str(name) for name in read_columns.keys())

    # Fallback minimale: raccoglie identifier quoted dal SQL per costruire un
    # raw_input placeholder abbastanza utile nel dry-run. E' deliberatamente
    # approssimativo: puo' includere nomi non-colonna e non copre colonne non
    # quotate se non sono gia' dichiarate in clean.read.columns.
    columns.extend(match.group(1) for match in _QUOTED_IDENTIFIER_RE.finditer(sql))
    return _dedupe_preserve_order(columns)


def _quoted_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _normalize_sql(sql: str) -> str:
    return sql.strip().rstrip(";").strip()


def _create_placeholder_raw_input(con: duckdb.DuckDBPyConnection, clean_cfg: dict[str, Any], sql: str) -> None:
    columns = _placeholder_columns(clean_cfg, sql)
    if columns:
        projection = ", ".join(f"NULL::VARCHAR AS {_quoted_identifier(name)}" for name in columns)
    else:
        projection = "NULL::VARCHAR AS __dry_run_placeholder"
    con.execute(f"CREATE OR REPLACE VIEW raw_input AS SELECT {projection} LIMIT 0")


def _build_clean_preview(
    cfg,
    *,
    year: int,
    con: duckdb.DuckDBPyConnection,
) -> None:
    clean_sql_path, clean_sql, _ = _load_clean_sql(
        cfg.clean,
        dataset=cfg.dataset,
        year=year,
        base_dir=cfg.base_dir,
    )
    clean_sql = _normalize_sql(clean_sql)
    _create_placeholder_raw_input(con, cfg.clean, clean_sql)
    try:
        con.execute(f"CREATE OR REPLACE TABLE __dry_run_clean_preview AS SELECT * FROM ({clean_sql}) AS q LIMIT 0")
    except Exception as exc:
        raise ValueError(f"CLEAN SQL dry-run failed ({clean_sql_path}): {exc}") from exc


def _validate_mart_sql(cfg, *, year: int, con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE OR REPLACE VIEW clean_input AS SELECT * FROM __dry_run_clean_preview")
    con.execute("CREATE OR REPLACE VIEW clean AS SELECT * FROM clean_input")

    tables = cfg.mart.get("tables") or []
    template_ctx = {"year": year, "dataset": cfg.dataset}

    for table in tables:
        name = table.get("name")
        sql_ref = table.get("sql")
        sql_path = _resolve_mart_sql_path(sql_ref, base_dir=cfg.base_dir)
        sql = _normalize_sql(render_template(sql_path.read_text(encoding="utf-8"), template_ctx))
        try:
            con.execute(f"EXPLAIN SELECT * FROM ({sql}) AS q LIMIT 0")
        except Exception as exc:
            raise ValueError(f"MART SQL dry-run failed ({name}, {sql_path}): {exc}") from exc


def validate_sql_dry_run(cfg, *, year: int, layers: list[str]) -> None:
    # Oggi il check copre solo CLEAN e MART. cross_year resta fuori perche'
    # ha un contratto di input diverso e richiede una validazione dedicata.
    if not any(layer in {"clean", "mart"} for layer in layers):
        return

    con = duckdb.connect(":memory:")
    try:
        _build_clean_preview(cfg, year=year, con=con)
        if "mart" in layers:
            _validate_mart_sql(cfg, year=year, con=con)
    finally:
        con.close()
