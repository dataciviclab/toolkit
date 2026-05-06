from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb

from toolkit.core.sql_utils import q_ident


def profile_relation(con: duckdb.DuckDBPyConnection, relation_name: str) -> dict[str, Any]:
    q_relation = q_ident(relation_name)
    row_count = int(con.execute(f"SELECT COUNT(*) FROM {q_relation}").fetchone()[0])
    described = con.execute(f"DESCRIBE {q_relation}").fetchall()
    columns = [{"name": row[0], "type": row[1]} for row in described]
    return {
        "row_count": row_count,
        "columns": columns,
    }


def profile_parquet_files(files: list[Path]) -> dict[str, Any]:
    if not files:
        raise ValueError("Cannot profile empty parquet file list")

    con = duckdb.connect(":memory:")
    try:
        if len(files) == 1:
            con.execute(
                f"CREATE VIEW profiled_input AS SELECT * FROM read_parquet('{files[0].as_posix()}')"
            )
        else:
            paths = ",".join(f"'{path.as_posix()}'" for path in files)
            con.execute(
                f"CREATE VIEW profiled_input AS SELECT * FROM read_parquet([{paths}])"
            )
        return profile_relation(con, "profiled_input")
    finally:
        con.close()


def compare_layer_profiles(
    source: dict[str, Any] | None,
    target: dict[str, Any] | None,
    *,
    source_layer: str,
    target_layer: str,
    target_name: str | None = None,
) -> dict[str, Any] | None:
    if source is None or target is None:
        return None

    source_columns = {item["name"]: item["type"] for item in source.get("columns", [])}
    target_columns = {item["name"]: item["type"] for item in target.get("columns", [])}

    # Build UPPER-mapped views for case-insensitive comparison.
    # This handles SQL renames like "ANNOCORSO AS annocorso" where raw has
    # uppercase names and clean has lowercase — they are the same column.
    source_upper_to_orig = {name.upper(): name for name in source_columns}
    target_upper_to_orig = {name.upper(): name for name in target_columns}

    source_upper = set(source_upper_to_orig.keys())
    target_upper = set(target_upper_to_orig.keys())

    # Columns present in both (case-insensitive match).
    # Use target's casing as the canonical name.
    shared_upper = sorted(source_upper & target_upper)

    # Added: in target but not in source (case-insensitive).
    # Map back to original target casing.
    added_upper = sorted(target_upper - source_upper)
    added_columns = [target_upper_to_orig[u] for u in added_upper]

    # Removed: in source but not in target (case-insensitive).
    # Map back to original source casing.
    removed_upper = sorted(source_upper - target_upper)
    removed_columns = [source_upper_to_orig[u] for u in removed_upper]

    type_changes = []
    for name_upper in shared_upper:
        src_name = source_upper_to_orig[name_upper]
        tgt_name = target_upper_to_orig[name_upper]
        if source_columns[src_name] != target_columns[tgt_name]:
            type_changes.append(
                {
                    "column": tgt_name,  # use target's casing as canonical
                    "from": source_columns[src_name],
                    "to": target_columns[tgt_name],
                }
            )

    payload = {
        "from": source_layer,
        "to": target_layer,
        "source_row_count": source.get("row_count"),
        "target_row_count": target.get("row_count"),
        "row_count_delta": (target.get("row_count") or 0) - (source.get("row_count") or 0),
        "added_columns": added_columns,
        "removed_columns": removed_columns,
        "type_changes": type_changes,
    }
    if target_name is not None:
        payload["target_name"] = target_name
    return payload
