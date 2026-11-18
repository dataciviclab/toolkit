"""Visibilità dello stato del profilo RAW nel metadata e nel validation gate.

`profile_raw` non solleva quando DuckDB non riesce a leggere il file: cattura,
scrive i warning in `_profile/raw_profile.json` e restituisce zero colonne.
Senza uno stato esplicito il run resta verde e il layer CLEAN legge con i soli
hint dello sniff leggero — misurato sul workspace: 219 profili vuoti su 1480.

Questi test proteggono il contratto: un profilo vuoto è uno stato dichiarato
(`profile.status = failed` + warning nel gate), non un silenzio.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from toolkit.core.paths import METADATA, RAW_PROFILE, RAW_PROFILE_DIR, layer_year_dir
from toolkit.profile.raw import load_raw_profile_status
from toolkit.raw import run as raw_run
from toolkit.raw.validate import run_raw_validation

LOGGER = logging.getLogger("test.raw.profile")


def _raw_cfg(tmp_path: Path, csv_text: str, *, read_mode: str = "fallback") -> dict:
    src = tmp_path / "input.csv"
    src.write_text(csv_text, encoding="utf-8")
    return {
        "sources": [{"name": "src", "type": "local_file", "args": {"path": str(src)}}],
        "output_policy": "overwrite",
    }, read_mode


def _run(tmp_path: Path, csv_text: str, *, read_mode: str = "fallback") -> dict:
    raw_cfg, mode = _raw_cfg(tmp_path, csv_text, read_mode=read_mode)
    clean_cfg = {"read_mode": mode, "read": {"delim": ";"}}
    raw_run.run_raw(
        "demo",
        2024,
        str(tmp_path),
        raw_cfg,
        LOGGER,
        clean_cfg=clean_cfg,
    )
    out_dir = layer_year_dir(str(tmp_path), "raw", "demo", 2024)
    return json.loads((out_dir / METADATA).read_text())


def test_metadata_records_profile_ok(tmp_path: Path):
    metadata = _run(tmp_path, "id;valore\n1;10\n2;20\n")

    assert metadata["profile"]["status"] == "ok"
    assert metadata["profile"]["columns"] == 2
    assert metadata["profile"]["row_count"] == 2


def test_metadata_records_profile_failed_and_validation_warns(tmp_path: Path):
    """File che la read config dichiarata non riesce a leggere in strict."""
    metadata = _run(tmp_path, "col1;col2\n1;2;3\n4;5\n", read_mode="strict")

    assert metadata["profile"]["status"] == "failed"
    assert metadata["profile"]["columns"] == 0
    assert any("profile_failed" in w for w in metadata["profile"]["warnings"])

    summary = run_raw_validation(str(tmp_path), "demo", 2024, LOGGER)
    assert any("raw_profile_empty" in w for w in summary["warnings"]), summary["warnings"]
    assert summary["summary"]["profile_status"] == "failed"


def test_load_raw_profile_status_missing_profile(tmp_path: Path):
    state = load_raw_profile_status(tmp_path)

    assert state["status"] == "missing"
    assert state["columns"] == 0


@pytest.mark.parametrize(
    ("columns", "expected"),
    [(["a", "b"], "ok"), ([], "failed")],
)
def test_load_raw_profile_status_reads_artifact(tmp_path: Path, columns, expected):
    profile_dir = tmp_path / RAW_PROFILE_DIR
    profile_dir.mkdir(parents=True)
    (profile_dir / RAW_PROFILE).write_text(
        json.dumps({"file_used": "x.csv", "columns_raw": columns, "row_count": 3, "warnings": []}),
        encoding="utf-8",
    )

    state = load_raw_profile_status(tmp_path)

    assert state["status"] == expected
    assert state["columns"] == len(columns)
