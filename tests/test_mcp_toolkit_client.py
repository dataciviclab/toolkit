from __future__ import annotations

import shutil
from pathlib import Path

from toolkit.mcp.toolkit_client import inspect_paths, run_state, show_schema


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
    assert state_payload["run_dir"].endswith("project_example\\2022")
