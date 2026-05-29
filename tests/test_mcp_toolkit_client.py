from __future__ import annotations

import json
import shutil
from pathlib import Path

import duckdb
import pytest

from toolkit.mcp.toolkit_client import (
    inspect_paths,
    list_runs,
    review_readiness,
    run_state,
    run_summary,
    show_schema,
    summary,
)

pytestmark = pytest.mark.contract


def _write_real_parquet(path: Path) -> None:
    """Write a minimal real parquet file via DuckDB."""
    conn = duckdb.connect()
    conn.execute(f"COPY (SELECT 1 AS id) TO '{path}' (FORMAT PARQUET)")
    conn.close()


def test_mcp_toolkit_client_works_from_repo_layout(tmp_path: Path, monkeypatch) -> None:
    src = Path("project-example")
    dst = tmp_path / "project-example"
    shutil.copytree(src, dst, ignore=shutil.ignore_patterns("_smoke_out"))
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

    # Arrange raw metadata with a missing primary output file.
    raw_dir = dst / "_smoke_out" / "data" / "raw" / "project_example" / "2022"
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / "metadata.json").write_text(
        json.dumps({"primary_output_file": "missing.csv"}), encoding="utf-8"
    )

    summary_payload = summary(str(config_path), 2022)
    warnings = summary_payload["warnings"]
    assert "raw_output_missing" in warnings
    assert "clean_output_missing" in warnings
    assert "mart_outputs_missing" in warnings



def test_review_readiness_incomplete_when_no_outputs(tmp_path: Path, monkeypatch) -> None:
    src = Path("project-example")
    dst = tmp_path / "project-example"
    shutil.copytree(src, dst, ignore=shutil.ignore_patterns("_smoke_out"))
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
    shutil.copytree(src, dst, ignore=shutil.ignore_patterns("_smoke_out"))
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


def test_review_readiness_enriched_layers_shape(tmp_path: Path, monkeypatch) -> None:
    """review_readiness()['layers'] deve contenere validation, validation_msgs, profile, transition."""
    src = Path("project-example")
    dst = tmp_path / "project-example"
    shutil.copytree(src, dst, ignore=shutil.ignore_patterns("_smoke_out"))
    config_path = dst / "dataset.yml"
    monkeypatch.setenv("DATACIVICLAB_WORKSPACE", str(tmp_path))
    import duckdb

    # Crea raw output + raw_validation.json (raw NON usa _validate/)
    raw_dir = dst / "_smoke_out" / "data" / "raw" / "project_example" / "2022"
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / "data.csv").write_bytes(b"a;b\n1;2\n")
    (raw_dir / "raw_validation.json").write_text(
        '{"ok":true,"errors":[],"warnings":["test warning raw"],"summary":{}}',
        encoding="utf-8",
    )

    # Crea clean output + clean validation con messaggi
    clean_dir = dst / "_smoke_out" / "data" / "clean" / "project_example" / "2022"
    clean_dir.mkdir(parents=True, exist_ok=True)
    clean_parquet = clean_dir / "project_example_2022_clean.parquet"
    with duckdb.connect(":memory:") as conn:
        conn.execute("CREATE TABLE t AS SELECT 1 AS x")
        conn.execute(f"COPY t TO '{clean_parquet}' (FORMAT PARQUET)")
    clean_val_dir = clean_dir / "_validate"
    clean_val_dir.mkdir(parents=True, exist_ok=True)
    (clean_val_dir / "clean_validation.json").write_text(
        json.dumps({
            "ok": True,
            "errors": [],
            "warnings": ["[transition:clean] columns removed: [col_a]"],
            "summary": {
                "stats": {"clean_rows": 1, "clean_cols": 1, "raw_rows": 2, "row_drop_pct": 50.0},
            },
            "sections": {
                "transition": {
                    "raw_row_count": 2,
                    "clean_row_count": 1,
                },
            },
        }),
        encoding="utf-8",
    )

    # Crea mart output + mart validation con messaggi
    mart_dir = dst / "_smoke_out" / "data" / "mart" / "project_example" / "2022"
    mart_dir.mkdir(parents=True, exist_ok=True)
    mart_p = mart_dir / "mart_t.parquet"
    with duckdb.connect(":memory:") as conn:
        conn.execute("CREATE TABLE t AS SELECT 1 AS x")
        conn.execute(f"COPY t TO '{mart_p}' (FORMAT PARQUET)")
    mart_val_dir = mart_dir / "_validate"
    mart_val_dir.mkdir(parents=True, exist_ok=True)
    (mart_val_dir / "mart_validation.json").write_text(
        json.dumps({
            "ok": False,
            "errors": ["[mart_t] row_count too small"],
            "warnings": [],
            "summary": {"row_counts": {"mart_t": 1}},
        }),
        encoding="utf-8",
    )

    payload = review_readiness(str(config_path), 2022)
    layers = payload.get("layers", {})

    # --- Asserts sulla struttura layers ---
    assert "raw" in layers
    assert "clean" in layers
    assert "mart" in layers

    # Raw: deve leggere raw_validation.json (non _validate/)
    raw_msgs = layers["raw"].get("validation_msgs", {})
    assert "test warning raw" in raw_msgs.get("warnings", []), (
        f"Raw validation messages non trovate: {raw_msgs}"
    )

    # Clean: validation_msgs deve contenere il warning di transizione
    clean_msgs = layers["clean"].get("validation_msgs", {})
    assert len(clean_msgs.get("warnings", [])) == 1
    assert "columns removed" in clean_msgs["warnings"][0]

    # Clean: transition stats
    trans = layers["clean"].get("transition", {})
    assert trans.get("row_drop_pct") == 50.0

    # Mart: validation_msgs deve contenere l'errore
    mart_msgs = layers["mart"].get("validation_msgs", {})
    assert len(mart_msgs.get("errors", [])) == 1
    assert "row_count too small" in mart_msgs["errors"][0]

    # Mart: ok=False si riflette nel validation
    assert layers["mart"].get("validation", {}).get("ok") is False

    # Raw profile hints (encoding/delim da suggested_read se presente)
    raw_profile = layers["raw"].get("profile", {})
    assert isinstance(raw_profile, dict)


def test_review_readiness_needs_review_with_single_failure(tmp_path: Path, monkeypatch) -> None:
    src = Path("project-example")
    dst = tmp_path / "project-example"
    shutil.copytree(src, dst, ignore=shutil.ignore_patterns("_smoke_out"))
    config_path = dst / "dataset.yml"

    monkeypatch.setenv("DATACIVICLAB_WORKSPACE", str(tmp_path))

    # raw presente
    raw_dir = dst / "_smoke_out" / "data" / "raw" / "project_example" / "2022"
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / "ispra_dettaglio_comunale_2022.csv").write_bytes(b"a;b\n1;2\n")

    # clean presente e leggibile
    clean_dir = dst / "_smoke_out" / "data" / "clean" / "project_example" / "2022"
    clean_dir.mkdir(parents=True, exist_ok=True)
    import duckdb

    clean_parquet = clean_dir / "project_example_2022_clean.parquet"
    with duckdb.connect(":memory:") as conn:
        conn.execute("CREATE TABLE t AS SELECT 1 AS x")
        conn.execute(f"COPY t TO '{clean_parquet}' (FORMAT PARQUET)")

    # mart volutamente mancante -> unico fail atteso
    payload = review_readiness(str(config_path), 2022)
    assert payload["readiness"] == "needs-review"
    assert payload["fail_count"] == 1


def test_mcp_raw_profile_handles_suggested_read_yaml(tmp_path: Path, monkeypatch) -> None:
    """raw_profile deve cadere su suggested_read.yml quando raw_profile.json non esiste."""
    src = Path("project-example")
    dst = tmp_path / "project-example"
    shutil.copytree(src, dst, ignore=shutil.ignore_patterns("_smoke_out"))
    config_path = dst / "dataset.yml"

    monkeypatch.setenv("DATACIVICLAB_WORKSPACE", str(tmp_path))

    # Crea solo suggested_read.yml, nessun raw_profile.json
    raw_dir = dst / "_smoke_out" / "data" / "raw" / "project_example" / "2022" / "_profile"
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / "suggested_read.yml").write_text(
        "clean:\n  read:\n    delim: ','\n    encoding: 'utf-8'\n",
        encoding="utf-8",
    )

    from toolkit.mcp.toolkit_client import raw_profile

    payload = raw_profile(str(config_path), 2022)
    assert payload["profile_exists"] is True
    assert payload["read_hints"]["delimiter"] == ","
    assert payload["read_hints"]["encoding"] == "utf-8"


def test_mcp_raw_profile_error_when_no_profile_file(tmp_path: Path, monkeypatch) -> None:
    """raw_profile deve fallire con errore chiaro quando non c'e' nessun profilo."""
    src = Path("project-example")
    dst = tmp_path / "project-example"
    shutil.copytree(src, dst, ignore=shutil.ignore_patterns("_smoke_out"))
    config_path = dst / "dataset.yml"

    monkeypatch.setenv("DATACIVICLAB_WORKSPACE", str(tmp_path))

    # Crea la dir _profile ma lasciala vuota
    raw_dir = dst / "_smoke_out" / "data" / "raw" / "project_example" / "2022" / "_profile"
    raw_dir.mkdir(parents=True, exist_ok=True)

    from toolkit.mcp.toolkit_client import raw_profile
    from toolkit.mcp.errors import ToolkitClientError

    with pytest.raises(ToolkitClientError, match="Nessun file raw_profile.json ne suggested_read.yml"):
        raw_profile(str(config_path), 2022)


def test_list_runs_accepts_naive_datetime_filter(tmp_path: Path, monkeypatch) -> None:
    """Naive datetime in since/until must be normalized to UTC, not crash with TypeError."""
    src = Path("project-example")
    dst = tmp_path / "project-example"
    shutil.copytree(src, dst, ignore=shutil.ignore_patterns("_smoke_out"))
    config_path = dst / "dataset.yml"

    monkeypatch.setenv("DATACIVICLAB_WORKSPACE", str(tmp_path))

    # Create a run record with UTC-aware started_at
    run_dir = dst / "_smoke_out" / "data" / "_runs" / "project_example" / "2022"
    run_dir.mkdir(parents=True, exist_ok=True)
    run_record = {
        "dataset": "project_example",
        "year": 2022,
        "run_id": "20260101T120000Z_abc123",
        "status": "SUCCESS",
        "started_at": "2026-01-01T12:00:00+00:00",
        "finished_at": "2026-01-01T12:01:00+00:00",
        "layers": {
            "raw": {"status": "SUCCESS"},
            "clean": {"status": "SUCCESS"},
        },
    }
    (run_dir / "20260101T120000Z_abc123.json").write_text(
        json.dumps(run_record), encoding="utf-8"
    )

    # Naive datetime filter — must NOT raise TypeError
    payload = list_runs(str(config_path), 2022, since="2025-12-01T00:00:00", limit=5)
    run_ids = [r["run_id"] for r in payload["runs"]]
    assert "20260101T120000Z_abc123" in run_ids

    # Naive until — filter should exclude this run
    payload2 = list_runs(str(config_path), 2022, until="2025-12-01T00:00:00", limit=5)
    run_ids2 = [r["run_id"] for r in payload2["runs"]]
    assert "20260101T120000Z_abc123" not in run_ids2

    # Aware datetime still works
    payload3 = list_runs(str(config_path), 2022, since="2025-12-01T00:00:00+00:00", limit=5)
    run_ids3 = [r["run_id"] for r in payload3["runs"]]
    assert "20260101T120000Z_abc123" in run_ids3


def test_run_summary_accepts_since_until_filters(tmp_path: Path, monkeypatch) -> None:
    """run_summary with since/until filters must normalize naive datetimes to UTC."""
    src = Path("project-example")
    dst = tmp_path / "project-example"
    shutil.copytree(src, dst, ignore=shutil.ignore_patterns("_smoke_out"))
    config_path = dst / "dataset.yml"

    monkeypatch.setenv("DATACIVICLAB_WORKSPACE", str(tmp_path))

    # Clear any pre-existing run records
    run_dir = dst / "_smoke_out" / "data" / "_runs" / "project_example" / "2022"
    run_dir.mkdir(parents=True, exist_ok=True)
    for f in run_dir.glob("*.json"):
        f.unlink()

    # Create runs: Oct 10, Oct 20, Oct 25 — 2 SUCCESS, 1 FAILED
    records = [
        {"started_at": "2025-10-10T12:00:00+00:00", "status": "SUCCESS"},
        {"started_at": "2025-10-20T12:00:00+00:00", "status": "SUCCESS"},
        {"started_at": "2025-10-25T12:00:00+00:00", "status": "FAILED"},
    ]
    for i, rec in enumerate(records):
        run_id = f"run_{i+1}"
        run_record = {
            "dataset": "project_example",
            "year": 2022,
            "run_id": run_id,
            "status": rec["status"],
            "started_at": rec["started_at"],
            "finished_at": rec["started_at"].replace("T12:00", "T12:01"),
            "duration_seconds": 60.0,
            "layers": {"raw": {"status": "SUCCESS"}, "clean": {"status": "SUCCESS"}},
        }
        (run_dir / f"{run_id}.json").write_text(json.dumps(run_record), encoding="utf-8")

    # Without filters — all 3 runs
    p = run_summary(str(config_path), 2022)
    assert p["total_runs"] == 3
    assert p["success_count"] == 2

    # Naive since — must not crash; Oct 16+ keeps Oct 20 and Oct 25 (2 runs, 1 SUCCESS)
    p2 = run_summary(str(config_path), 2022, since="2025-10-16T00:00:00")
    assert p2["total_runs"] == 2
    assert p2["success_count"] == 1
    assert p2["filters"]["since"] == "2025-10-16T00:00:00"

    # Naive until — Oct 25 00:00 excludes Oct 25 (12:00 > 00:00), keeps Oct 10 and Oct 20
    p3 = run_summary(str(config_path), 2022, until="2025-10-25T00:00:00")
    assert p3["total_runs"] == 2
    assert p3["success_count"] == 2
    assert p3["filters"]["until"] == "2025-10-25T00:00:00"

    # Aware datetime — same result as naive
    p4 = run_summary(str(config_path), 2022, since="2025-10-16T00:00:00+00:00")
    assert p4["total_runs"] == 2


@pytest.mark.policy
def test_inspect_paths_cli_mcp_contract_alignment(tmp_path: Path, monkeypatch) -> None:
    """CLI --json output must match the InspectPathsResult TypedDict contract.

    Run ``toolkit inspect paths --json`` via CLI runner and verify every key
    defined in ``InspectPathsResult`` is present in the output. This catches
    contract drift between CLI and MCP consumer code.
    """
    from typer.testing import CliRunner

    from toolkit.cli.app import app
    from toolkit.mcp.types import (
        CleanPaths,
        InspectPathsResult,
        LayerPaths,
        MartPaths,
        RawHints,
        RawPaths,
    )

    src = Path("project-example")
    dst = tmp_path / "project-example"
    shutil.copytree(src, dst, ignore=shutil.ignore_patterns("_smoke_out"))
    config_path = dst / "dataset.yml"

    monkeypatch.chdir(tmp_path)
    runner = CliRunner()

    result = runner.invoke(app, ["inspect", "paths", "--config", str(config_path), "--year", "2022", "--json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)

    # Verifica chiavi top-level del contratto
    for key in InspectPathsResult.__required_keys__:
        assert key in payload, f"InspectPathsResult: chiave '{key}' mancante nel CLI output"

    # Verifica sottostruttura paths
    for key in LayerPaths.__required_keys__:
        assert key in payload["paths"], f"LayerPaths: chiave '{key}' mancante"

    # Verifica raw paths
    for key in RawPaths.__required_keys__:
        assert key in payload["paths"]["raw"], f"RawPaths: chiave '{key}' mancante"

    # Verifica clean paths
    for key in CleanPaths.__required_keys__:
        assert key in payload["paths"]["clean"], f"CleanPaths: chiave '{key}' mancante"

    # Verifica mart paths
    for key in MartPaths.__required_keys__:
        assert key in payload["paths"]["mart"], f"MartPaths: chiave '{key}' mancante"

    # Verifica raw_hints ha almeno le chiavi required di RawHints
    for key in RawHints.__required_keys__:
        assert key in payload["raw_hints"], f"RawHints: chiave '{key}' mancante"


@pytest.mark.policy
def test_inspect_paths_multi_year_defaults_to_max_year(tmp_path: Path, monkeypatch) -> None:
    """inspect_paths(year=None) su dataset multi-year usa l'ultimo anno."""
    from toolkit.mcp.cli_adapter import inspect_paths

    yml = tmp_path / "dataset.yml"
    yml.write_text(
        "root: " + str(tmp_path) + "\n"
        "dataset:\n"
        "  name: test\n"
        "  years: [2022, 2023]\n"
    )
    monkeypatch.chdir(tmp_path)

    result = inspect_paths(str(yml))
    assert result["year"] == 2023  # max year come default
    assert "_year_resolution" in result
    assert result["_year_resolution"]["years_available"] == [2022, 2023]
    assert "Usato 2023" in result["_year_resolution"]["note"]


@pytest.mark.policy
@pytest.mark.policy
def test_schema_diff_cli_contract_alignment(tmp_path: Path, monkeypatch) -> None:
    """toolkit inspect schema-diff --json output matches SchemaDiffResult TypedDict."""
    from typer.testing import CliRunner

    from toolkit.cli.app import app
    from toolkit.mcp.types import (
        RawSchemaEntry,
        SchemaComparison,
        SchemaDiffResult,
    )

    src = Path("project-example")
    dst = tmp_path / "project-example"
    shutil.copytree(src, dst, ignore=shutil.ignore_patterns("_smoke_out"))
    config_path = dst / "dataset.yml"

    monkeypatch.chdir(tmp_path)
    runner = CliRunner()

    result = runner.invoke(app, ["inspect", "schema-diff", "--config", str(config_path), "--json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)

    for key in SchemaDiffResult.__required_keys__:
        assert key in payload, f"SchemaDiffResult: chiave '{key}' mancante"
    for entry in payload.get("entries", []):
        for key in RawSchemaEntry.__required_keys__:
            assert key in entry, f"RawSchemaEntry: chiave '{key}' mancante in anno {entry.get('year')}"
    for comp in payload.get("comparisons", []):
        for key in SchemaComparison.__required_keys__:
            assert key in comp, f"SchemaComparison: chiave '{key}' mancante"


@pytest.mark.policy
def test_review_readiness_mcp_contract_shape(tmp_path: Path, monkeypatch) -> None:
    """review_readiness() output matches ReviewReadinessResult TypedDict."""
    from toolkit.mcp.types import ReadinessCheck, ReviewReadinessResult
    from toolkit.mcp.toolkit_client import review_readiness

    src = Path("project-example")
    dst = tmp_path / "project-example"
    shutil.copytree(src, dst, ignore=shutil.ignore_patterns("_smoke_out"))
    config_path = dst / "dataset.yml"

    monkeypatch.chdir(tmp_path)

    payload = review_readiness(str(config_path), 2022)

    for key in ReviewReadinessResult.__required_keys__:
        assert key in payload, f"ReviewReadinessResult: chiave '{key}' mancante"
    for check in payload.get("checks", []):
        for ck in ReadinessCheck.__required_keys__:
            assert ck in check, f"ReadinessCheck: chiave '{ck}' mancante in {check.get('check')}"


def test_clean_preview_with_real_parquet(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """clean_preview deve leggere un parquet reale e restituire schema + preview."""
    src = Path("project-example")
    dst = tmp_path / "project-example"
    shutil.copytree(src, dst, ignore=shutil.ignore_patterns("_smoke_out"))
    config_path = dst / "dataset.yml"

    monkeypatch.setenv("DATACIVICLAB_WORKSPACE", str(tmp_path))

    # Crea un parquet clean reale
    clean_dir = dst / "_smoke_out" / "data" / "clean" / "project_example" / "2022"
    clean_dir.mkdir(parents=True, exist_ok=True)
    _write_real_parquet(clean_dir / "project_example_2022_clean.parquet")

    from toolkit.mcp.toolkit_client import clean_preview

    result = clean_preview(str(config_path), layer="clean", year=2022, limit=5)

    assert result["dataset"] == "project_example"
    assert result["layer"] == "clean"
    assert result["column_count"] >= 1
    assert len(result["preview"]) >= 1
    assert "row_count" in result
    assert "truncated" in result


def test_clean_preview_mart_with_index(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """clean_preview(layer='mart') deve leggere il mart_index richiesto."""
    src = Path("project-example")
    dst = tmp_path / "project-example"
    shutil.copytree(src, dst, ignore=shutil.ignore_patterns("_smoke_out"))
    config_path = dst / "dataset.yml"

    monkeypatch.setenv("DATACIVICLAB_WORKSPACE", str(tmp_path))

    # Crea output mart (due table)
    mart_dir = dst / "_smoke_out" / "data" / "mart" / "project_example" / "2022"
    mart_dir.mkdir(parents=True, exist_ok=True)
    _write_real_parquet(mart_dir / "rd_by_regione.parquet")
    _write_real_parquet(mart_dir / "rd_by_provincia.parquet")

    from toolkit.mcp.toolkit_client import clean_preview

    result = clean_preview(str(config_path), layer="mart", mart_index=1, year=2022, limit=5)

    assert result["layer"] == "mart"
    assert result["mart_name"] == "rd_by_provincia"
    assert result["column_count"] >= 1


def test_clean_preview_mart_index_negative(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """clean_preview con mart_index < 0 deve alzare errore."""
    src = Path("project-example")
    dst = tmp_path / "project-example"
    shutil.copytree(src, dst, ignore=shutil.ignore_patterns("_smoke_out"))
    config_path = dst / "dataset.yml"

    monkeypatch.setenv("DATACIVICLAB_WORKSPACE", str(tmp_path))

    # Crea output mart (due table)
    mart_dir = dst / "_smoke_out" / "data" / "mart" / "project_example" / "2022"
    mart_dir.mkdir(parents=True, exist_ok=True)
    _write_real_parquet(mart_dir / "rd_by_regione.parquet")
    _write_real_parquet(mart_dir / "rd_by_provincia.parquet")

    from toolkit.mcp.errors import ToolkitClientError
    from toolkit.mcp.toolkit_client import clean_preview

    with pytest.raises(ToolkitClientError, match="Indice mart"):
        clean_preview(str(config_path), layer="mart", mart_index=-1, year=2022, limit=5)


def test_raw_preview_with_real_csv(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """raw_preview deve leggere un CSV reale dal raw dir."""
    src = Path("project-example")
    dst = tmp_path / "project-example"
    shutil.copytree(src, dst, ignore=shutil.ignore_patterns("_smoke_out"))
    config_path = dst / "dataset.yml"

    monkeypatch.setenv("DATACIVICLAB_WORKSPACE", str(tmp_path))

    raw_dir = dst / "_smoke_out" / "data" / "raw" / "project_example" / "2022"
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / "metadata.json").write_text(
        json.dumps({"primary_output_file": "ispra_dettaglio_comunale_2022.csv"}), encoding="utf-8"
    )
    (raw_dir / "ispra_dettaglio_comunale_2022.csv").write_bytes(b"a;b\n1;2\n3;4\n")

    from toolkit.mcp.toolkit_client import raw_preview

    result = raw_preview(str(config_path), year=2022, limit=5)

    assert result["path"].endswith("ispra_dettaglio_comunale_2022.csv")
    assert result["column_count"] >= 2
    assert len(result["preview"]) >= 2


def test_dataset_info_from_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """dataset_info deve estrarre campi da dataset.yml reale senza eseguire pipeline."""
    src = Path("project-example")
    dst = tmp_path / "project-example"
    shutil.copytree(src, dst, ignore=shutil.ignore_patterns("_smoke_out"))
    config_path = dst / "dataset.yml"

    monkeypatch.setenv("DATACIVICLAB_WORKSPACE", str(tmp_path))

    from toolkit.mcp.toolkit_client import dataset_info

    result = dataset_info(str(config_path))

    assert result["dataset"] == "project_example"
    assert result["years"] == [2022]
    assert "source_urls" in result
    assert "has_clean" in result
    assert "has_mart" in result
    assert "raw_sources_count" in result
    assert "mart_tables" in result
    assert result["raw_sources_count"] >= 1


def test_list_candidates_with_minimal_candidate(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """list_candidates deve trovare candidate creati nel workspace."""
    from toolkit.mcp import discovery as _discovery_mod
    monkeypatch.setattr(_discovery_mod, "WORKSPACE_ROOT", tmp_path)

    # Crea un candidate minimale
    cand_dir = tmp_path / "dataset-incubator" / "candidates" / "test-candidate"
    cand_dir.mkdir(parents=True, exist_ok=True)
    (cand_dir / "dataset.yml").write_text(
        "dataset:\n  name: test-candidate\n  years: [2024]\n",
        encoding="utf-8",
    )

    from toolkit.mcp.discovery import list_candidates

    result = list_candidates(stage="all")

    assert isinstance(result, list)
    slugs = [c["slug"] for c in result]
    assert "test-candidate" in slugs
    for item in result:
        if item["slug"] == "test-candidate":
            assert item["stage"] == "candidates"
            assert item["years"] == [2024]
            break


def test_list_candidates_returns_sorted_list(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """list_candidates deve restituire lista ordinata per slug."""
    from toolkit.mcp import discovery as _discmod
    monkeypatch.setattr(_discmod, "WORKSPACE_ROOT", tmp_path)

    for name in ("b-dataset", "a-dataset", "c-dataset"):
        cand_dir = tmp_path / "dataset-incubator" / "candidates" / name
        cand_dir.mkdir(parents=True, exist_ok=True)
        (cand_dir / "dataset.yml").write_text(
            f"dataset:\n  name: {name}\n  years: [2024]\n",
            encoding="utf-8",
        )

    from toolkit.mcp.discovery import list_candidates

    result = list_candidates(stage="candidates")
    slugs = [c["slug"] for c in result]

    assert slugs == sorted(slugs)
    assert "a-dataset" in slugs
    assert "b-dataset" in slugs
    assert "c-dataset" in slugs


def test_safe_path_absolute_not_found_raises_clean_error(tmp_path: Path) -> None:
    """_safe_path con path assoluto inesistente deve alzare ToolkitClientError,
    non RecursionError (regressione recursion loop)."""
    from toolkit.mcp.path_safety import _safe_path
    from toolkit.mcp.errors import ToolkitClientError

    nonexistent = tmp_path / "nonexistent" / "dataset.yml"

    with pytest.raises(ToolkitClientError, match="Config non trovata"):
        _safe_path(str(nonexistent))


def test_safe_path_slug_not_found_raises_clean_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """_safe_path con slug inesistente deve alzare ToolkitClientError,
    non RecursionError (regressione recursion loop)."""
    from toolkit.mcp import path_safety as _ps_mod
    from toolkit.mcp.path_safety import _safe_path
    from toolkit.mcp.errors import ToolkitClientError

    monkeypatch.setattr(_ps_mod, "WORKSPACE_ROOT", tmp_path)

    with pytest.raises(ToolkitClientError, match="Config non trovata"):
        _safe_path("totally-nonexistent-slug")


def test_safe_path_directory_resolves_to_dataset_yml(tmp_path: Path) -> None:
    """_safe_path con path directory deve restituire directory/dataset.yml se esiste."""
    from toolkit.mcp.path_safety import _safe_path

    # Crea directory con dataset.yml dentro
    d = tmp_path / "candidates" / "test-dataset"
    d.mkdir(parents=True)
    yml = d / "dataset.yml"
    yml.write_text("dataset:\n  name: test\n")

    result = _safe_path(str(d))
    assert result == yml.resolve(), (
        f"Dovrebbe restituire {yml.resolve()}, invece ha restituito {result}"
    )
    assert result.suffix in (".yml", ".yaml")


def test_safe_path_directory_without_dataset_yml_raises_error(tmp_path: Path) -> None:
    """_safe_path con directory senza dataset.yml deve alzare ToolkitClientError,
    non restituire la directory."""
    from toolkit.mcp.path_safety import _safe_path
    from toolkit.mcp.errors import ToolkitClientError

    empty_dir = tmp_path / "empty-dir"
    empty_dir.mkdir()

    with pytest.raises(ToolkitClientError, match="non contiene dataset"):
        _safe_path(str(empty_dir))


def test_safe_path_directory_without_dataset_yml_falls_back_to_slug(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """_safe_path con directory senza dataset.yml deve tentare risoluzione slug
    prima di alzare errore."""
    from toolkit.mcp import path_safety as _ps_mod
    from toolkit.mcp.path_safety import _safe_path

    # Crea slug risolvibile: dataset-incubator/candidates/{slug}/dataset.yml
    slug = "test-slug-resolved"
    candidate_dir = tmp_path / "dataset-incubator" / "candidates" / slug
    candidate_dir.mkdir(parents=True)
    yml = candidate_dir / "dataset.yml"
    yml.write_text("dataset:\n  name: resolved\n")

    monkeypatch.setattr(_ps_mod, "WORKSPACE_ROOT", tmp_path)

    # Una directory senza dataset.yml con lo stesso nome dello slug
    some_dir = tmp_path / "some-other-dir"
    some_dir.mkdir()

    # _safe_path("some-other-dir") non dovrebbe risolversi
    # _safe_path(slug) dovrebbe risolversi via slug
    result = _safe_path(slug)
    assert result == yml.resolve()


def test_list_candidates_status_filter(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """list_candidates(status_filter='SUCCESS') deve filtrare per last_run_status."""
    from toolkit.mcp import discovery as _discmod
    monkeypatch.setattr(_discmod, "WORKSPACE_ROOT", tmp_path)

    # Crea due candidate: uno senza run (status=None), l'altro mocka run SUCCESS
    for name in ("candidate-a", "candidate-b"):
        cand_dir = tmp_path / "dataset-incubator" / "candidates" / name
        cand_dir.mkdir(parents=True, exist_ok=True)
        (cand_dir / "dataset.yml").write_text(
            f"dataset:\n  name: {name}\n  years: [2024]\n",
            encoding="utf-8",
        )

    # Crea run record per candidate-b: status=SUCCESS
    runs_dir = tmp_path / "out" / "data" / "_runs" / "candidate-b" / "2024"
    runs_dir.mkdir(parents=True, exist_ok=True)
    import json
    (runs_dir / "run_success.json").write_text(
        json.dumps({
            "dataset": "candidate-b",
            "year": 2024,
            "run_id": "run_001",
            "status": "SUCCESS",
            "started_at": "2026-01-01T12:00:00+00:00",
            "finished_at": "2026-01-01T12:01:00+00:00",
            "layers": {"raw": {"status": "SUCCESS"}},
        }),
        encoding="utf-8",
    )

    from toolkit.mcp.discovery import list_candidates

    # Senza filtro -> tutti e due
    all_results = list_candidates(stage="candidates")
    assert len(all_results) == 2

    # Filtro SUCCESS -> solo candidate-b
    success_results = list_candidates(stage="candidates", status_filter="SUCCESS")
    assert len(success_results) == 1
    assert success_results[0]["slug"] == "candidate-b"

    # Filtro FAILED -> nessuno
    failed_results = list_candidates(stage="candidates", status_filter="FAILED")
    assert len(failed_results) == 0


def test_list_candidates_status_filter_invalid(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """list_candidates con status_filter non valido deve alzare errore."""
    from toolkit.mcp import discovery as _discmod
    from toolkit.mcp.errors import ToolkitClientError
    monkeypatch.setattr(_discmod, "WORKSPACE_ROOT", tmp_path)

    from toolkit.mcp.discovery import list_candidates

    with pytest.raises(ToolkitClientError, match="status_filter"):
        list_candidates(stage="candidates", status_filter="INVALID")


def test_list_candidates_stage_invalid(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """list_candidates con stage non valido deve alzare errore."""
    from toolkit.mcp import discovery as _discmod
    from toolkit.mcp.errors import ToolkitClientError
    monkeypatch.setattr(_discmod, "WORKSPACE_ROOT", tmp_path)

    from toolkit.mcp.discovery import list_candidates

    with pytest.raises(ToolkitClientError, match="stage deve essere"):
        list_candidates(stage="bogus")
