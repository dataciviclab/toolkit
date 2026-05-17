"""Shared helpers for MCP schema operations.

Internal utilities used by the MCP tool functions in schema_ops.py.
Not part of the public API — no stability guarantee.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb

from lab_connectors.mcp.errors import ErrorCode
from toolkit.mcp.errors import ToolkitClientError


def _sql_literal(value: str) -> str:
    """Escape a string for safe use inside a SQL single-quoted literal."""
    return value.replace("'", "''")


def _schema_from_parquet(parquet_path: Path) -> dict[str, Any]:
    """Return schema (columns + count) of a parquet file via DuckDB.

    Raises:
        ToolkitClientError: if the file doesn't exist or can't be read.
    """
    if not parquet_path.exists():
        raise ToolkitClientError(f"Parquet non trovato: {parquet_path}", code=ErrorCode.PARQUET_NOT_FOUND)
    relation = f"read_parquet('{_sql_literal(str(parquet_path))}')"
    try:
        with duckdb.connect(database=":memory:") as conn:
            conn.execute("PRAGMA disable_progress_bar")
            describe_rows = conn.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
    except Exception as exc:
        raise ToolkitClientError(
            f"Lettura schema parquet fallita per {parquet_path}: {exc}",
            code=ErrorCode.DUCKDB_ERROR,
        ) from exc

    columns = [{"name": row[0], "type": row[1]} for row in describe_rows]
    return {"path": str(parquet_path), "column_count": len(columns), "columns": columns}


def _read_parquet_row_count(parquet_path: Path) -> int | None:
    """Return row count of a parquet file, or None if unreadable."""
    if not parquet_path.exists():
        return None
    try:
        with duckdb.connect(database=":memory:") as conn:
            conn.execute("PRAGMA disable_progress_bar")
            result = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{_sql_literal(str(parquet_path))}')"
            ).fetchone()
            return int(result[0]) if result else None
    except Exception:
        return None


def _read_parquet_preview(parquet_path: Path, limit: int = 10) -> dict[str, Any]:
    """Return schema + row preview of a parquet file via DuckDB.

    Returns:
        dict with keys: path, column_count, columns (list of {name, type}),
        row_count, preview (list of dicts), truncated (bool).

    Raises:
        ToolkitClientError: if the file doesn't exist or can't be read.
    """
    if not parquet_path.exists():
        raise ToolkitClientError(f"Parquet non trovato: {parquet_path}", code=ErrorCode.PARQUET_NOT_FOUND)

    rel = f"read_parquet('{_sql_literal(str(parquet_path))}')"
    try:
        with duckdb.connect(database=":memory:") as conn:
            conn.execute("PRAGMA disable_progress_bar")

            # schema
            describe = conn.execute(f"DESCRIBE SELECT * FROM {rel}").fetchall()
            columns = [{"name": row[0], "type": row[1]} for row in describe]

            # row count
            count_row = conn.execute(f"SELECT COUNT(*) FROM {rel}").fetchone()
            row_count = int(count_row[0]) if count_row else None

            # preview
            preview_rows = conn.execute(
                f"SELECT * FROM {rel} LIMIT {int(limit)}"
            ).fetchall()
            col_names = [c["name"] for c in columns]
            preview = [dict(zip(col_names, row)) for row in preview_rows]

            truncated = bool(row_count and row_count > limit)

            return {
                "path": str(parquet_path),
                "column_count": len(columns),
                "columns": columns,
                "row_count": row_count,
                "preview": preview,
                "truncated": truncated,
            }
    except Exception as exc:
        raise ToolkitClientError(
            f"Lettura parquet fallita per {parquet_path}: {exc}",
            code=ErrorCode.DUCKDB_ERROR,
        ) from exc


def _exists(path: str | None) -> bool:
    """Return True if path is a real file/directory."""
    if not path:
        return False
    return Path(path).exists()


def _read_validation_content(path: str | None) -> dict[str, Any] | None:
    """Read a validation JSON file and return its content, or None if missing."""
    if not path or not _exists(path):
        return None
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return None


def _check_run_record_coherence(
    run_record: dict[str, Any] | None,
    layers: dict[str, Any],
) -> list[dict[str, str]]:
    """Verifica che i layer marcati SUCCESS nel run record abbiano output reali.

    Ritorna una lista di hint (dict con code, severity, message).
    Usata da review_readiness().
    """
    hints: list[dict[str, str]] = []
    if not run_record:
        return hints

    layers_map = run_record.get("layers") or {}
    for layer_name, layer_detail in layers_map.items():
        layer_status = (
            layer_detail.get("status") if isinstance(layer_detail, dict) else layer_detail
        )
        if layer_status != "SUCCESS":
            continue

        layer_info = layers.get(layer_name, {})

        if layer_name == "clean" and not layer_info.get("output_exists"):
            hints.append({
                "code": "run_says_clean_success_but_output_missing",
                "severity": "blocker",
                "message": "run record dice clean SUCCESS ma output file manca",
            })
        elif layer_name == "mart":
            out_count = layer_info.get("output_count", 0) or 0
            exists_count = layer_info.get("output_exists_count", 0) or 0
            if exists_count == 0 and out_count > 0:
                hints.append({
                    "code": "run_says_mart_success_but_outputs_missing",
                    "severity": "blocker",
                    "message": "run record dice mart SUCCESS ma nessun output file presente",
                })

    return hints


def _validation_summary_for_layer(
    layer_dir: Path, validation_filename: str
) -> dict[str, Any] | None:
    """Extract summary from a layer's validation JSON.

    Adds: ok, errors_count, warnings_count, row_count, col_count,
    raw_row_count, clean_row_count.
    Reads row/col counts from summary.stats (clean) or summary.row_counts (mart).
    Falls back to sections.stats for layers that use that path.
    Returns None if the validation file does not exist.
    """
    validation_path = layer_dir / validation_filename
    content = _read_validation_content(str(validation_path))
    if not content:
        return None

    result = {
        "ok": content.get("ok"),
        "errors_count": len(content.get("errors", [])),
        "warnings_count": len(content.get("warnings", [])),
        "row_count": None,
        "col_count": None,
    }

    # Extract stats from summary (clean layer: summary.stats.clean_rows/clean_cols)
    summary = content.get("summary", {})
    stats = summary.get("stats", {})
    result["row_count"] = stats.get("clean_rows") or stats.get("row_count")
    result["col_count"] = stats.get("clean_cols")

    # Fallback: sections.stats (mart layer uses sections differently)
    sections = content.get("sections", {})
    if result["row_count"] is None and "stats" in sections:
        result["row_count"] = sections["stats"].get("row_count")
        result["col_count"] = sections["stats"].get("col_count")

    # Extract transition metadata (clean validation)
    if "transition" in sections:
        t = sections["transition"]
        if "clean_cols" in t:
            result["col_count"] = t.get("clean_cols")
        if "raw_row_count" in t:
            result["raw_row_count"] = t.get("raw_row_count")
        if "clean_row_count" in t:
            result["clean_row_count"] = t.get("clean_row_count")

    # Extract row_counts from mart summary (mart layer)
    if result["row_count"] is None:
        row_counts = summary.get("row_counts", {})
        if row_counts:
            first_key = next(iter(row_counts), None)
            if first_key:
                result["row_count"] = row_counts[first_key]

    return result
