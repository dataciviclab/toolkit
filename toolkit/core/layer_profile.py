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

    source_names = set(source_columns.keys())
    target_names = set(target_columns.keys())
    shared_names = sorted(source_names & target_names)

    type_changes = []
    for name in shared_names:
        if source_columns[name] != target_columns[name]:
            type_changes.append(
                {
                    "column": name,
                    "from": source_columns[name],
                    "to": target_columns[name],
                }
            )

    payload = {
        "from": source_layer,
        "to": target_layer,
        "source_row_count": source.get("row_count"),
        "target_row_count": target.get("row_count"),
        "row_count_delta": (target.get("row_count") or 0) - (source.get("row_count") or 0),
        "added_columns": sorted(target_names - source_names),
        "removed_columns": sorted(source_names - target_names),
        "type_changes": type_changes,
    }
    if target_name is not None:
        payload["target_name"] = target_name
    return payload
