from __future__ import annotations

import json
import shutil
from pathlib import Path

from toolkit.mcp.toolkit_client import inspect_paths, run_state, show_schema, summary


def test_mcp_toolkit_client_works_from_repo_layout(tmp_path: Path, monkeypatch) -> None:
    src = Path("project-example")
    dst = tmp_path / "project-example"
    shutil.copytree(src, dst)
    config_path = dst / "dataset.yml"

    monkeypatch.setenv("DATACIVICLAB_WORKSPACE", str(tmp_path))

    paths_payload = inspect_paths(str(config_path), 2022)
    assert paths_payload["dataset"] == "project_example"
    assert paths_payload["year"] == 2022
    assert paths_payload["paths"]["clean"]["output"].endswith("project_example_2022_clean.parquet")

    raw_schema = show_schema(str(config_path), "raw", 2022)
    assert raw_schema["layer"] == "raw"
    assert raw_schema["dataset"] == "project_example"

    state_payload = run_state(str(config_path), 2022)
    assert state_payload["dataset"] == "project_example"
    assert Path(state_payload["run_dir"]).parts[-2:] == ("project_example", "2022")

    # Arrange raw manifest without creating the primary output file.
    raw_dir = dst / "_smoke_out" / "data" / "raw" / "project_example" / "2022"
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / "manifest.json").write_text(
        json.dumps({"primary_output_file": "missing.csv"}), encoding="utf-8"
    )

    summary_payload = summary(str(config_path), 2022)
    warnings = summary_payload["warnings"]
    assert "raw_output_missing" in warnings
    assert "clean_output_missing" in warnings
    assert "mart_outputs_missing" in warnings
