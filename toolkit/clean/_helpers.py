"""Internal helpers for clean layer validation.

Not part of the public API — internal utility module.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb

from toolkit.clean.duckdb_read import read_raw_to_relation
from toolkit.core.layer_profile import profile_relation


def _input_files_from_clean_metadata(raw_dir: Path, clean_metadata: dict[str, Any]) -> list[Path]:
    input_files = clean_metadata.get("input_files") or []
    return [raw_dir / str(name) for name in input_files]


def _profile_raw_input(
    input_files: list[Path],
    read_cfg: dict[str, Any],
    read_mode: str,
    logger,
) -> dict[str, Any]:
    con = duckdb.connect(":memory:")
    try:
        read_raw_to_relation(con, input_files, read_cfg, read_mode, logger)
        return profile_relation(con, "raw_input")
    finally:
        con.close()
