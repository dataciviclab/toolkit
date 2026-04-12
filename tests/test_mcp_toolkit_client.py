from __future__ import annotations

import json
import shutil
from pathlib import Path

from toolkit.mcp.toolkit_client import (
    blocker_hints,
    inspect_paths,
    review_readiness,
    run_state,
    show_schema,
    summary,
)


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


def test_mcp_blocker_hints_detects_missing_outputs(tmp_path: Path, monkeypatch) -> None:
    src = Path("project-example")
    dst = tmp_path / "project-example"
    shutil.copytree(src, dst)
    config_path = dst / "dataset.yml"

    monkeypatch.setenv("DATACIVICLAB_WORKSPACE", str(tmp_path))

    # Arrange: solo raw manifest, nessun output reale
    raw_dir = dst / "_smoke_out" / "data" / "raw" / "project_example" / "2022"
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / "manifest.json").write_text(
        json.dumps({"primary_output_file": "missing.csv"}), encoding="utf-8"
    )

    hints_payload = blocker_hints(str(config_path), 2022)
    assert hints_payload["hint_count"] > 0
    codes = {h["code"] for h in hints_payload["hints"]}
    assert "raw_output_missing" in codes
    assert "clean_output_missing" in codes
    assert hints_payload["blocker_count"] >= 2


def test_mcp_blocker_hints_empty_when_all_present(tmp_path: Path, monkeypatch) -> None:
    src = Path("project-example")
    dst = tmp_path / "project-example"
    shutil.copytree(src, dst)
    config_path = dst / "dataset.yml"

    monkeypatch.setenv("DATACIVICLAB_WORKSPACE", str(tmp_path))

    # Arrange: crea output fittizi per tutti i layer
    raw_dir = dst / "_smoke_out" / "data" / "raw" / "project_example" / "2022"
    raw_dir.mkdir(parents=True, exist_ok=True)
    # Il filename risolto dal config e' "ispra_dettaglio_comunale_2022.csv"
    (raw_dir / "ispra_dettaglio_comunale_2022.csv").write_bytes(b"a;b\n1;2\n")

    clean_dir = dst / "_smoke_out" / "data" / "clean" / "project_example" / "2022"
    clean_dir.mkdir(parents=True, exist_ok=True)
    (clean_dir / "project_example_2022_clean.parquet").write_bytes(b"")

    mart_dir = dst / "_smoke_out" / "data" / "mart" / "project_example" / "2022"
    mart_dir.mkdir(parents=True, exist_ok=True)
    # Il config dichiara 2 tabelle mart
    (mart_dir / "rd_by_regione.parquet").write_bytes(b"")
    (mart_dir / "rd_by_provincia.parquet").write_bytes(b"")

    hints_payload = blocker_hints(str(config_path), 2022)
    assert hints_payload["hint_count"] == 0
    assert hints_payload["blocker_count"] == 0
    assert hints_payload["warning_count"] == 0


def test_mcp_blocker_hints_run_says_clean_success_but_output_missing(
    tmp_path: Path, monkeypatch
) -> None:
    src = Path("project-example")
    dst = tmp_path / "project-example"
    shutil.copytree(src, dst)
    config_path = dst / "dataset.yml"

    monkeypatch.setenv("DATACIVICLAB_WORKSPACE", str(tmp_path))

    # Arrange: raw output presente, clean dir esiste ma output manca
    raw_dir = dst / "_smoke_out" / "data" / "raw" / "project_example" / "2022"
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / "ispra_dettaglio_comunale_2022.csv").write_bytes(b"a;b\n1;2\n")
    (raw_dir / "manifest.json").write_text(
        json.dumps({"primary_output_file": "ispra_dettaglio_comunale_2022.csv"}),
        encoding="utf-8",
    )

    clean_dir = dst / "_smoke_out" / "data" / "clean" / "project_example" / "2022"
    clean_dir.mkdir(parents=True, exist_ok=True)
    # clean output NON creato di proposito

    # Run record che dice clean SUCCESS ma il file non c'e'
    # Rimuovo eventuali run record preesistenti copiati dal project-example
    run_dir = dst / "_smoke_out" / "data" / "_runs" / "project_example" / "2022"
    if run_dir.exists():
        for f in run_dir.glob("*.json"):
            f.unlink()
    run_dir.mkdir(parents=True, exist_ok=True)
    run_record = {
        "dataset": "project_example",
        "year": 2022,
        "run_id": "20260101T000000Z_abc123",
        "status": "FAILED",
        "layers": {
            "raw": {"status": "SUCCESS"},
            "clean": {
                "status": "SUCCESS",
                "started_at": "2026-01-01T00:00:00Z",
                "finished_at": "2026-01-01T00:00:01Z",
            },
        },
    }
    (run_dir / "20260101T000000Z_abc123.json").write_text(json.dumps(run_record), encoding="utf-8")

    hints_payload = blocker_hints(str(config_path), 2022)
    codes = {h["code"] for h in hints_payload["hints"]}
    assert "run_says_clean_success_but_output_missing" in codes
    blockers = [h for h in hints_payload["hints"] if h["severity"] == "blocker"]
    assert len(blockers) >= 1


def test_review_readiness_incomplete_when_no_outputs(tmp_path: Path, monkeypatch) -> None:
    src = Path("project-example")
    dst = tmp_path / "project-example"
    shutil.copytree(src, dst)
    config_path = dst / "dataset.yml"

    monkeypatch.setenv("DATACIVICLAB_WORKSPACE", str(tmp_path))

    # Nessun output creato
    payload = review_readiness(str(config_path), 2022)
    assert payload["readiness"] == "incomplete"
    assert payload["fail_count"] >= 2  # raw, clean, mart tutti mancanti
    check_names = {c["check"] for c in payload["checks"]}
    assert "config_valid" in check_names
    assert "raw_output_present" in check_names
    assert "clean_output_readable" in check_names
    assert "mart_outputs_readable" in check_names
    assert "run_record_coherent" in check_names


def test_review_readiness_ready_when_all_layers_present(tmp_path: Path, monkeypatch) -> None:
    src = Path("project-example")
    dst = tmp_path / "project-example"
    shutil.copytree(src, dst)
    config_path = dst / "dataset.yml"

    monkeypatch.setenv("DATACIVICLAB_WORKSPACE", str(tmp_path))

    # Crea output fittizi per tutti i layer
    raw_dir = dst / "_smoke_out" / "data" / "raw" / "project_example" / "2022"
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / "ispra_dettaglio_comunale_2022.csv").write_bytes(b"a;b\n1;2\n")

    clean_dir = dst / "_smoke_out" / "data" / "clean" / "project_example" / "2022"
    clean_dir.mkdir(parents=True, exist_ok=True)

    # Crea un parquet minimale leggibile
    import duckdb

    clean_parquet = clean_dir / "project_example_2022_clean.parquet"
    with duckdb.connect(":memory:") as conn:
        conn.execute("CREATE TABLE t AS SELECT 1 AS x")
        conn.execute(f"COPY t TO '{clean_parquet}' (FORMAT PARQUET)")

    mart_dir = dst / "_smoke_out" / "data" / "mart" / "project_example" / "2022"
    mart_dir.mkdir(parents=True, exist_ok=True)
    for name in ("rd_by_regione.parquet", "rd_by_provincia.parquet"):
        with duckdb.connect(":memory:") as conn:
            conn.execute("CREATE TABLE t AS SELECT 1 AS x")
            conn.execute(f"COPY t TO '{mart_dir / name}' (FORMAT PARQUET)")

    payload = review_readiness(str(config_path), 2022)
    assert payload["readiness"] == "ready"
    assert payload["fail_count"] == 0
    assert payload["ok_count"] == len(payload["checks"])
