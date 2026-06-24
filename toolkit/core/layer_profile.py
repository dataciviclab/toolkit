from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import duckdb
from lab_connectors.duckdb import safe_connect

from toolkit.core.sql_utils import q_ident, sql_path


def _normalize_key(name: str) -> str:
    """Normalizza un nome colonna per confronto cross-layer.

    APPLICA:
      - .upper() per case-insensitive
      - sostituisce [^A-Z0-9] con underscore
      - comprime underscore multipli

    Copre i casi:
      'Tipo ufficio' → 'TIPO_UFFICIO'
      'Definiti - totale' → 'DEFINITI_TOTALE'
      'ANNOCORSO' → 'ANNOCORSO'
      'Raccolta differenziata (%)' → 'RACCOLTA_DIFFERENZIATA_'
    """
    s = re.sub(r"[^A-Z0-9]+", "_", name.upper())
    return re.sub(r"_+", "_", s).strip("_")


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

    with safe_connect() as con:
        if len(files) == 1:
            con.execute(
                f"CREATE VIEW profiled_input AS SELECT * FROM read_parquet('{sql_path(files[0])}')"
            )
        else:
            paths = ",".join(f"'{sql_path(p)}'" for p in files)
            con.execute(f"CREATE VIEW profiled_input AS SELECT * FROM read_parquet([{paths}])")
        return profile_relation(con, "profiled_input")


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

    # Build normalised-key views for case+separator-insensitive comparison.
    # Handles renames like 'Tipo ufficio' → 'tipo_ufficio', 'Definiti - totale' → 'definiti_totale',
    # 'Raccolta differenziata (%)' → 'raccolta_differenziata'.
    source_norm_to_orig = {_normalize_key(name): name for name in source_columns}
    target_norm_to_orig = {_normalize_key(name): name for name in target_columns}

    source_norm = set(source_norm_to_orig.keys())
    target_norm = set(target_norm_to_orig.keys())

    # Columns present in both (normalised match).
    # Use target's casing as the canonical name.
    shared_norm = sorted(source_norm & target_norm)

    # Added: in target but not in source (normalised).
    # Map back to original target casing.
    added_norm = sorted(target_norm - source_norm)
    added_columns = [target_norm_to_orig[u] for u in added_norm]

    # Removed: in source but not in target (normalised).
    # Map back to original source casing.
    removed_norm = sorted(source_norm - target_norm)
    removed_columns = [source_norm_to_orig[u] for u in removed_norm]

    type_changes = []
    for name_norm in shared_norm:
        src_name = source_norm_to_orig[name_norm]
        tgt_name = target_norm_to_orig[name_norm]
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
