"""Test del MCP client layer — funzioni chiamate dal server MCP.

Setup centralizzato in fixture ``mcp_project_example`` (conftest.py).
Helper ``_make_project_smoke`` per test che necessitano di output fittizi.
"""

from __future__ import annotations

import json
from pathlib import Path

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


# ── Helper ───────────────────────────────────────────────────────────


def _write_parquet(path: Path, sql: str = "SELECT 1 AS id") -> None:
    """Scrive un parquet minimale."""
    from lab_connectors.duckdb import safe_connect

    with safe_connect() as conn:
        conn.execute(f"COPY ({sql}) TO '{path}' (FORMAT PARQUET)")


def _make_project_smoke(
    tmp_path: Path, slug: str = "project_example", year: int = 2022
) -> tuple[Path, str, int]:
    """Crea output fittizi clean + mart per project-example.

    Returns:
        (config_path, slug, year).
    """
    src = Path("project-example")
    dst = tmp_path / "project-example"
    import shutil

    shutil.copytree(src, dst, ignore=shutil.ignore_patterns("_smoke_out"))
    config_path = dst / "dataset.yml"

    root = dst / "_smoke_out"
    clean_dir = root / "data" / "clean" / slug / str(year)
    clean_dir.mkdir(parents=True, exist_ok=True)
    _write_parquet(clean_dir / f"{slug}_{year}_clean.parquet", "SELECT 'a' AS cat, 1 AS val")

    mart_dir = root / "data" / "mart" / slug / str(year)
    mart_dir.mkdir(parents=True, exist_ok=True)
    _write_parquet(mart_dir / "rd_by_regione.parquet", "SELECT 'x' AS k, 10 AS v")

    return config_path, slug, year


# ── Test ─────────────────────────────────────────────────────────────


def test_mcp_toolkit_client_works_from_repo_layout(
    mcp_project_example: tuple[Path, str, int],
) -> None:
    """Funzioni core: inspect_paths, show_schema, run_state, summary."""
    config_path, dataset, year = mcp_project_example

    paths_payload = inspect_paths(str(config_path), year)
    assert paths_payload["dataset"] == dataset
    assert paths_payload["paths"]["clean"]["output"].endswith(f"{dataset}_{year}_clean.parquet")

    raw_schema = show_schema(str(config_path), "raw", year)
    assert raw_schema["layer"] == "raw"
    assert raw_schema["dataset"] == dataset

    state_payload = run_state(str(config_path), year)
    assert state_payload["dataset"] == dataset
    assert Path(state_payload["run_dir"]).parts[-2:] == (dataset, str(year))

    # summary con primary_output_file mancante
    base = Path(config_path).parent / "_smoke_out"
    raw_dir = base / "data" / "raw" / dataset / str(year)
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / "metadata.json").write_text(
        json.dumps({"primary_output_file": "missing.csv"}), encoding="utf-8"
    )
    summary_payload = summary(str(config_path), year)
    warnings = summary_payload["warnings"]
    assert "raw_output_missing" in warnings
    assert "clean_output_missing" in warnings
    assert "mart_outputs_missing" in warnings


def test_review_readiness_incomplete_when_no_outputs(
    mcp_project_example: tuple[Path, str, int],
) -> None:
    """Nessun output → readiness=incomplete."""
    config_path, _dataset, year = mcp_project_example
    payload = review_readiness(str(config_path), year)
    assert payload["readiness"] == "incomplete"
    assert payload["fail_count"] >= 2
    check_names = {c["check"] for c in payload["checks"]}
    for name in (
        "config_valid",
        "raw_output_present",
        "clean_output_readable",
        "mart_outputs_readable",
        "run_record_coherent",
    ):
        assert name in check_names


def _make_fake_outputs(
    base_dir: Path,
    slug: str,
    year: int,
    *,
    raw_name: str | None = None,
    mart_tables: list[str] | None = None,
) -> None:
    """Crea output fittizi raw + clean + mart per test di readiness."""
    tables = mart_tables or ["rd_by_regione"]

    # raw (CSV) — il nome deve matchare dataset.yml
    raw_dir = base_dir / "data" / "raw" / slug / str(year)
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_name = raw_name or f"{slug}_{year}.csv"
    (raw_dir / raw_name).write_bytes(b"a;b\n1;2\n")

    # clean parquet
    clean_dir = base_dir / "data" / "clean" / slug / str(year)
    clean_dir.mkdir(parents=True, exist_ok=True)
    _write_parquet(clean_dir / f"{slug}_{year}_clean.parquet")

    # mart parquet(s)
    mart_dir = base_dir / "data" / "mart" / slug / str(year)
    mart_dir.mkdir(parents=True, exist_ok=True)
    for t in tables:
        _write_parquet(mart_dir / f"{t}.parquet")


@pytest.mark.parametrize(
    "readiness,missing_mart",
    [
        ("ready", False),
        ("needs-review", True),
    ],
)
def test_review_readiness_levels(
    mcp_project_example: tuple[Path, str, int],
    readiness: str,
    missing_mart: bool,
) -> None:
    """readiness=ready con tutti gli output, needs-review con mart mancante."""
    config_path, dataset, year = mcp_project_example
    root = Path(config_path).parent / "_smoke_out"
    mart_tables = [] if missing_mart else ["rd_by_regione", "rd_by_provincia"]
    _make_fake_outputs(
        root,
        dataset,
        year,
        raw_name=f"ispra_dettaglio_comunale_{year}.csv",
        mart_tables=mart_tables,
    )
    payload = review_readiness(str(config_path), year)
    assert payload["readiness"] == readiness
    assert payload["fail_count"] == (1 if missing_mart else 0)


def test_review_readiness_enriched_layers_shape(
    mcp_project_example: tuple[Path, str, int],
) -> None:
    """review_readiness()['layers'] con validation, validation_msgs, profile, transition."""
    config_path, dataset, year = mcp_project_example
    root = Path(config_path).parent / "_smoke_out"

    raw_dir = root / "data" / "raw" / dataset / str(year)
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / "data.csv").write_bytes(b"a;b\n1;2\n")
    (raw_dir / "raw_validation.json").write_text(
        '{"ok":true,"errors":[],"warnings":["test warning raw"],"summary":{}}', encoding="utf-8"
    )

    clean_dir = root / "data" / "clean" / dataset / str(year)
    clean_dir.mkdir(parents=True, exist_ok=True)
    _write_parquet(clean_dir / f"{dataset}_{year}_clean.parquet")
    clean_val_dir = clean_dir / "_validate"
    clean_val_dir.mkdir(parents=True, exist_ok=True)
    (clean_val_dir / "clean_validation.json").write_text(
        json.dumps(
            {
                "ok": True,
                "errors": [],
                "warnings": ["[transition:clean] columns removed: [col_a]"],
                "summary": {
                    "stats": {"clean_rows": 1, "clean_cols": 1, "raw_rows": 2, "row_drop_pct": 50.0}
                },
                "sections": {"transition": {"raw_row_count": 2, "clean_row_count": 1}},
            }
        ),
        encoding="utf-8",
    )

    mart_dir = root / "data" / "mart" / dataset / str(year)
    mart_dir.mkdir(parents=True, exist_ok=True)
    _write_parquet(mart_dir / "mart_t.parquet")
    mart_val_dir = mart_dir / "_validate"
    mart_val_dir.mkdir(parents=True, exist_ok=True)
    (mart_val_dir / "mart_validation.json").write_text(
        json.dumps(
            {
                "ok": False,
                "errors": ["[mart_t] row_count too small"],
                "warnings": [],
                "summary": {"row_counts": {"mart_t": 1}},
            }
        ),
        encoding="utf-8",
    )

    payload = review_readiness(str(config_path), year)
    layers = payload.get("layers", {})
    assert "raw" in layers and "clean" in layers and "mart" in layers

    raw_msgs = layers["raw"].get("validation_msgs", {})
    assert "test warning raw" in raw_msgs.get("warnings", [])

    clean_msgs = layers["clean"].get("validation_msgs", {})
    assert len(clean_msgs.get("warnings", [])) == 1
    assert "columns removed" in clean_msgs["warnings"][0]

    trans = layers["clean"].get("transition", {})
    assert trans.get("row_drop_pct") == 50.0

    mart_msgs = layers["mart"].get("validation_msgs", {})
    assert len(mart_msgs.get("errors", [])) == 1
    assert "row_count too small" in mart_msgs["errors"][0]

    assert layers["mart"].get("validation", {}).get("ok") is False
    assert isinstance(layers["raw"].get("profile", {}), dict)


@pytest.mark.parametrize("has_suggested", [True, False])
def test_mcp_raw_profile(
    mcp_project_example: tuple[Path, str, int],
    has_suggested: bool,
) -> None:
    """raw_profile: con suggested_read.yml → read_hints, senza → errore."""
    from toolkit.mcp.toolkit_client import raw_profile
    from toolkit.mcp.errors import ToolkitClientError

    config_path, dataset, year = mcp_project_example
    root = Path(config_path).parent / "_smoke_out"
    profile_dir = root / "data" / "raw" / dataset / str(year) / "_profile"
    profile_dir.mkdir(parents=True, exist_ok=True)

    if has_suggested:
        (profile_dir / "suggested_read.yml").write_text(
            "clean:\n  read:\n    delim: ','\n    encoding: 'utf-8'\n", encoding="utf-8"
        )
        payload = raw_profile(str(config_path), year)
        assert payload["profile_exists"] is True
        assert payload["read_hints"]["delimiter"] == ","
        assert payload["read_hints"]["encoding"] == "utf-8"
    else:
        with pytest.raises(ToolkitClientError, match="Nessun file raw_profile.json"):
            raw_profile(str(config_path), year)


def test_list_runs_accepts_naive_datetime_filter(
    mcp_project_example: tuple[Path, str, int],
) -> None:
    """Naive datetime in since/until viene normalizzato a UTC."""
    config_path, dataset, year = mcp_project_example
    root = Path(config_path).parent / "_smoke_out"
    run_dir = root / "data" / "_runs" / dataset / str(year)
    run_dir.mkdir(parents=True, exist_ok=True)

    run_record = {
        "dataset": dataset,
        "year": year,
        "run_id": "20260101T120000Z_abc123",
        "status": "SUCCESS",
        "started_at": "2026-01-01T12:00:00+00:00",
        "finished_at": "2026-01-01T12:01:00+00:00",
        "layers": {"raw": {"status": "SUCCESS"}, "clean": {"status": "SUCCESS"}},
    }
    (run_dir / "20260101T120000Z_abc123.json").write_text(json.dumps(run_record), encoding="utf-8")

    payload = list_runs(str(config_path), year, since="2025-12-01T00:00:00", limit=5)
    assert "20260101T120000Z_abc123" in [r["run_id"] for r in payload["runs"]]

    payload2 = list_runs(str(config_path), year, until="2025-12-01T00:00:00", limit=5)
    assert "20260101T120000Z_abc123" not in [r["run_id"] for r in payload2["runs"]]

    payload3 = list_runs(str(config_path), year, since="2025-12-01T00:00:00+00:00", limit=5)
    assert "20260101T120000Z_abc123" in [r["run_id"] for r in payload3["runs"]]


def test_run_summary_accepts_since_until_filters(
    mcp_project_example: tuple[Path, str, int],
) -> None:
    """run_summary filtra per since/until."""
    config_path, dataset, year = mcp_project_example
    run_dir = Path(config_path).parent / "_smoke_out" / "data" / "_runs" / dataset / str(year)
    run_dir.mkdir(parents=True, exist_ok=True)
    for f in run_dir.glob("*.json"):
        f.unlink()

    records = [
        {"started_at": "2025-10-10T12:00:00+00:00", "status": "SUCCESS"},
        {"started_at": "2025-10-20T12:00:00+00:00", "status": "SUCCESS"},
        {"started_at": "2025-10-25T12:00:00+00:00", "status": "FAILED"},
    ]
    for i, rec in enumerate(records):
        run_id = f"run_{i + 1}"
        (run_dir / f"{run_id}.json").write_text(
            json.dumps(
                {
                    "dataset": dataset,
                    "year": year,
                    "run_id": run_id,
                    "status": rec["status"],
                    "started_at": rec["started_at"],
                    "finished_at": rec["started_at"].replace("T12:00", "T12:01"),
                    "duration_seconds": 60.0,
                    "layers": {"raw": {"status": "SUCCESS"}, "clean": {"status": "SUCCESS"}},
                }
            ),
            encoding="utf-8",
        )

    p = run_summary(str(config_path), year)
    assert p["total_runs"] == 3 and p["success_count"] == 2

    p2 = run_summary(str(config_path), year, since="2025-10-16T00:00:00")
    assert p2["total_runs"] == 2 and p2["success_count"] == 1

    p3 = run_summary(str(config_path), year, until="2025-10-25T00:00:00")
    assert p3["total_runs"] == 2 and p3["success_count"] == 2

    p4 = run_summary(str(config_path), year, since="2025-10-16T00:00:00+00:00")
    assert p4["total_runs"] == 2


@pytest.mark.policy
def test_inspect_paths_cli_mcp_contract_alignment(
    mcp_project_example: tuple[Path, str, int],
    monkeypatch,
    tmp_path,
) -> None:
    """CLI --json output matches InspectPathsResult TypedDict."""
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

    config_path, _dataset, year = mcp_project_example
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(
        app, ["inspect", "paths", "--config", str(config_path), "--year", str(year), "--json"]
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)

    for key in InspectPathsResult.__required_keys__:
        assert key in payload
    for key in LayerPaths.__required_keys__:
        assert key in payload["paths"]
    for key, container in [(RawPaths, "raw"), (CleanPaths, "clean"), (MartPaths, "mart")]:
        for k in key.__required_keys__:
            assert k in payload["paths"][container], f"{key.__name__}: {k} mancante"
    for key in RawHints.__required_keys__:
        assert key in payload["raw_hints"]


def test_inspect_paths_multi_year_defaults_to_max_year(tmp_path, monkeypatch):
    """inspect_paths(year=None) su dataset multi-year usa l'ultimo anno."""
    from toolkit.mcp.cli_adapter import inspect_paths

    yml = tmp_path / "dataset.yml"
    yml.write_text(f"root: {tmp_path}\ndataset:\n  name: test\n  years: [2022, 2023]\n")
    monkeypatch.chdir(tmp_path)

    result = inspect_paths(str(yml))
    assert result["year"] == 2023
    assert "_year_resolution" in result
    assert result["_year_resolution"]["years_available"] == [2022, 2023]


@pytest.mark.policy
def test_schema_diff_cli_contract_alignment(
    mcp_project_example: tuple[Path, str, int],
    monkeypatch,
    tmp_path,
) -> None:
    """toolkit inspect config --diff --json matches SchemaDiffResult TypedDict."""
    from typer.testing import CliRunner
    from toolkit.cli.app import app
    from toolkit.mcp.types import RawSchemaEntry, SchemaComparison, SchemaDiffResult

    config_path, _dataset, _year = mcp_project_example
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(
        app, ["inspect", "config", "--diff", "--config", str(config_path), "--json"]
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)

    for key in SchemaDiffResult.__required_keys__:
        assert key in payload
    for entry in payload.get("entries", []):
        for key in RawSchemaEntry.__required_keys__:
            assert key in entry
    for comp in payload.get("comparisons", []):
        for key in SchemaComparison.__required_keys__:
            assert key in comp


@pytest.mark.policy
def test_review_readiness_mcp_contract_shape(
    mcp_project_example: tuple[Path, str, int],
) -> None:
    """review_readiness() matches ReviewReadinessResult TypedDict."""
    from toolkit.mcp.types import ReadinessCheck, ReviewReadinessResult

    config_path, _dataset, year = mcp_project_example
    payload = review_readiness(str(config_path), year)

    for key in ReviewReadinessResult.__required_keys__:
        assert key in payload
    for check in payload.get("checks", []):
        for ck in ReadinessCheck.__required_keys__:
            assert ck in check


@pytest.mark.parametrize(
    "layer,mart_index,expect_error",
    [
        ("clean", None, False),
        ("mart", 1, False),
        ("mart", -1, True),
    ],
)
def test_clean_preview(
    mcp_project_example: tuple[Path, str, int],
    layer: str,
    mart_index: int | None,
    expect_error: bool,
) -> None:
    """clean_preview su layer clean e mart."""
    from toolkit.mcp.toolkit_client import clean_preview
    from toolkit.mcp.errors import ToolkitClientError

    config_path, dataset, year = mcp_project_example
    root = Path(config_path).parent / "_smoke_out"

    clean_dir = root / "data" / "clean" / dataset / str(year)
    clean_dir.mkdir(parents=True, exist_ok=True)
    _write_parquet(clean_dir / f"{dataset}_{year}_clean.parquet")

    mart_dir = root / "data" / "mart" / dataset / str(year)
    mart_dir.mkdir(parents=True, exist_ok=True)
    _write_parquet(mart_dir / "rd_by_regione.parquet")
    _write_parquet(mart_dir / "rd_by_provincia.parquet")

    kwargs = {"year": year, "limit": 5}
    if layer == "mart":
        kwargs["layer"] = "mart"
        if mart_index is not None:
            kwargs["mart_index"] = mart_index
    else:
        kwargs["layer"] = "clean"

    if expect_error:
        with pytest.raises(ToolkitClientError, match="Indice mart"):
            clean_preview(str(config_path), **kwargs)
    else:
        result = clean_preview(str(config_path), **kwargs)
        assert result["dataset"] == dataset
        assert result["layer"] == layer
        assert result["column_count"] >= 1
        assert len(result.get("preview", [])) >= 1
        assert "row_count" in result
        assert "truncated" in result
        if layer == "mart" and mart_index == 1:
            assert result["mart_name"] == "rd_by_provincia"


def test_raw_preview_with_real_csv(
    mcp_project_example: tuple[Path, str, int],
) -> None:
    """raw_preview legge CSV reale dal raw dir."""
    from toolkit.mcp.toolkit_client import raw_preview

    config_path, dataset, year = mcp_project_example
    raw_dir = Path(config_path).parent / "_smoke_out" / "data" / "raw" / dataset / str(year)
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / "metadata.json").write_text(
        json.dumps({"primary_output_file": "test_data.csv"}), encoding="utf-8"
    )
    (raw_dir / "test_data.csv").write_bytes(b"a;b\n1;2\n3;4\n")

    result = raw_preview(str(config_path), year=year, limit=5)
    assert result["path"].endswith("test_data.csv")
    assert result["column_count"] >= 2
    assert len(result["preview"]) >= 2


def test_dataset_info_from_config(
    mcp_project_example: tuple[Path, str, int],
) -> None:
    """dataset_info estrae campi da dataset.yml senza eseguire pipeline."""
    from toolkit.mcp.toolkit_client import dataset_info

    config_path, _dataset, _year = mcp_project_example
    result = dataset_info(str(config_path))
    assert result["dataset"] == "project_example"
    assert result["years"] == [2022]
    assert "source_urls" in result
    assert "has_clean" in result and "has_mart" in result
    assert "raw_sources_count" in result and "mart_tables" in result
    assert result["raw_sources_count"] >= 1


# ── list_candidates ─────────────────────────────────────────────────


def _make_candidate(workspace: Path, name: str) -> None:
    """Crea un candidate minimale."""
    cand_dir = workspace / "dataset-incubator" / "candidates" / name
    cand_dir.mkdir(parents=True, exist_ok=True)
    (cand_dir / "dataset.yml").write_text(
        f"dataset:\n  name: {name}\n  years: [2024]\n", encoding="utf-8"
    )


def _make_candidate_run(
    workspace: Path, name: str, status: str = "SUCCESS", year: int = 2024, root: str | None = None
) -> None:
    """Crea run record per un candidate."""
    base = Path(root) if root else workspace / "out"
    runs_dir = base / "data" / "_runs" / name / str(year)
    runs_dir.mkdir(parents=True, exist_ok=True)
    (runs_dir / "run.json").write_text(
        json.dumps(
            {
                "dataset": name,
                "year": year,
                "run_id": "run_001",
                "status": status,
                "started_at": "2026-01-01T12:00:00+00:00",
                "finished_at": "2026-01-01T12:01:00+00:00",
                "layers": {"raw": {"status": "SUCCESS"}},
            }
        ),
        encoding="utf-8",
    )


@pytest.mark.parametrize(
    "names,sorted_check",
    [
        (["test-candidate"], False),
        (["b-dataset", "a-dataset", "c-dataset"], True),
    ],
)
def test_list_candidates_sorting(tmp_path, monkeypatch, names, sorted_check):
    """list_candidates: discover e ordinamento."""
    from toolkit.mcp import discovery as _discmod

    monkeypatch.setattr(_discmod, "WORKSPACE_ROOT", tmp_path)

    for name in names:
        _make_candidate(tmp_path, name)

    from toolkit.mcp.discovery import list_candidates

    result = list_candidates(stage="all")
    slugs = [c["slug"] for c in result]
    for name in names:
        assert name in slugs
        item = [c for c in result if c["slug"] == name][0]
        assert item["stage"] == "candidates"
        assert item["years"] == [2024]
    if sorted_check:
        assert slugs == sorted(slugs)


def test_list_candidates_resolves_custom_root(tmp_path, monkeypatch):
    """list_candidates risolve root custom da dataset.yml."""
    from toolkit.mcp import discovery as _discmod

    monkeypatch.setattr(_discmod, "WORKSPACE_ROOT", tmp_path)

    name = "test-root-candidate"
    cand_dir = tmp_path / "dataset-incubator" / "candidates" / name
    cand_dir.mkdir(parents=True, exist_ok=True)
    (cand_dir / "dataset.yml").write_text(
        f'root: "../../custom_out"\nschema_version: 1\ndataset:\n  name: {name}\n  years: [2024]\n',
        encoding="utf-8",
    )

    custom_root = tmp_path / "dataset-incubator" / "custom_out"
    clean_dir = custom_root / "data" / "clean" / name / "2024"
    clean_dir.mkdir(parents=True, exist_ok=True)
    _write_parquet(clean_dir / f"{name}_2024_clean.parquet")

    mart_dir = custom_root / "data" / "mart" / name / "2024"
    mart_dir.mkdir(parents=True, exist_ok=True)
    _write_parquet(mart_dir / f"mart_{name}.parquet")

    _make_candidate_run(tmp_path, name, root=custom_root)

    from toolkit.mcp.discovery import list_candidates

    items = [c for c in list_candidates(stage="all") if c["slug"] == name]
    assert len(items) == 1
    item = items[0]
    assert item["has_clean"] is True
    assert item["has_mart"] is True
    assert item["last_run_status"] == "SUCCESS"
    assert not (tmp_path / "out" / "data" / "clean" / name).exists()


@pytest.mark.parametrize(
    "status_filter,expected_count",
    [
        (None, 2),
        ("SUCCESS", 1),
        ("FAILED", 0),
    ],
)
def test_list_candidates_status_filter(tmp_path, monkeypatch, status_filter, expected_count):
    """Filtro per last_run_status."""
    from toolkit.mcp import discovery as _discmod

    monkeypatch.setattr(_discmod, "WORKSPACE_ROOT", tmp_path)

    _make_candidate(tmp_path, "candidate-a")
    _make_candidate(tmp_path, "candidate-b")
    _make_candidate_run(tmp_path, "candidate-b")  # SUCCESS

    from toolkit.mcp.discovery import list_candidates

    kwargs = {"stage": "candidates"}
    if status_filter:
        kwargs["status_filter"] = status_filter
    result = list_candidates(**kwargs)
    assert len(result) == expected_count


@pytest.mark.parametrize(
    "invalid,kwargs,error_match",
    [
        ("status", {"status_filter": "INVALID"}, "status_filter"),
        ("stage", {"stage": "bogus"}, "stage deve essere"),
    ],
)
def test_list_candidates_invalid_params(tmp_path, monkeypatch, invalid, kwargs, error_match):
    """Parametri invalidi → errore."""
    from toolkit.mcp import discovery as _discmod
    from toolkit.mcp.errors import ToolkitClientError

    monkeypatch.setattr(_discmod, "WORKSPACE_ROOT", tmp_path)

    from toolkit.mcp.discovery import list_candidates

    with pytest.raises(ToolkitClientError, match=error_match):
        list_candidates(**kwargs)


# ── _safe_path ──────────────────────────────────────────────────────


def test_safe_path_not_found(tmp_path):
    """Path/slug inesistente → ToolkitClientError, non RecursionError."""
    from toolkit.mcp.path_safety import _safe_path
    from toolkit.mcp.errors import ToolkitClientError

    with pytest.raises(ToolkitClientError, match="Config non trovata"):
        _safe_path(str(tmp_path / "nonexistent" / "dataset.yml"))

    from toolkit.mcp import path_safety as _ps_mod

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(_ps_mod, "WORKSPACE_ROOT", tmp_path)
    with pytest.raises(ToolkitClientError, match="Config non trovata"):
        _safe_path("totally-nonexistent-slug")
    monkeypatch.undo()


def test_safe_path_directory_resolves(tmp_path):
    """Directory con dataset.yml → restituisce il file."""
    from toolkit.mcp.path_safety import _safe_path

    d = tmp_path / "candidates" / "test-dataset"
    d.mkdir(parents=True)
    yml = d / "dataset.yml"
    yml.write_text("dataset:\n  name: test\n")

    result = _safe_path(str(d))
    assert result == yml.resolve()
    assert result.suffix in (".yml", ".yaml")


def test_safe_path_directory_without_dataset_yml(tmp_path):
    """Directory senza dataset.yml → errore."""
    from toolkit.mcp.path_safety import _safe_path
    from toolkit.mcp.errors import ToolkitClientError

    empty_dir = tmp_path / "empty-dir"
    empty_dir.mkdir()
    with pytest.raises(ToolkitClientError, match="non contiene dataset"):
        _safe_path(str(empty_dir))


# ── Aggregate ops: layer_query + dataset_status ─────────────────────


@pytest.mark.parametrize(
    "mode,sql",
    [
        ("schema", None),
        ("preview", None),
        ("sql", "SELECT cat, SUM(val) AS tot FROM data GROUP BY cat"),
    ],
)
def test_layer_query_modes(tmp_path, monkeypatch, mode, sql):
    """layer_query: schema, preview, sql."""
    from toolkit.mcp.aggregate_ops import layer_query

    config_path, _dataset, _year = _make_project_smoke(tmp_path)
    monkeypatch.setenv("DATACIVICLAB_WORKSPACE", str(tmp_path))

    kwargs = {"layer": "clean", "mode": mode, "limit": 5}
    if sql:
        kwargs["sql"] = sql
    result = layer_query(str(config_path), **kwargs)
    assert result["layer"] == "clean"
    assert result["column_count"] >= 1
    if mode == "schema":
        assert "columns" in result
    elif mode == "preview":
        assert len(result.get("preview", [])) >= 1
        assert result.get("row_count", 0) >= 1
    elif mode == "sql":
        assert result.get("mode") == mode
        assert result["column_count"] >= 2
        assert result.get("sql") is not None


def test_layer_query_mart_preview(tmp_path, monkeypatch):
    """layer_query su layer=mart."""
    from toolkit.mcp.aggregate_ops import layer_query

    config_path, _dataset, _year = _make_project_smoke(tmp_path)
    monkeypatch.setenv("DATACIVICLAB_WORKSPACE", str(tmp_path))

    result = layer_query(str(config_path), layer="mart", mode="preview", limit=3)
    assert result["layer"] == "mart"
    assert result["column_count"] >= 1


@pytest.mark.parametrize(
    "layer,mode,error_match",
    [
        ("clean", "profile", "mode=profile"),
        ("clean", "invalid", "mode deve essere"),
    ],
)
def test_layer_query_invalid_args(tmp_path, monkeypatch, layer, mode, error_match):
    """layer_query con argomenti invalidi → errore."""
    from toolkit.mcp.aggregate_ops import layer_query
    from toolkit.mcp.errors import ToolkitClientError

    config_path, _dataset, _year = _make_project_smoke(tmp_path)
    monkeypatch.setenv("DATACIVICLAB_WORKSPACE", str(tmp_path))

    with pytest.raises(ToolkitClientError, match=error_match):
        layer_query(str(config_path), layer=layer, mode=mode)


def test_dataset_status_shape(tmp_path, monkeypatch):
    """dataset_status: 5 sezioni."""
    from toolkit.mcp.aggregate_ops import dataset_status

    config_path, dataset = _make_project_smoke(tmp_path)[:2]
    monkeypatch.setenv("DATACIVICLAB_WORKSPACE", str(tmp_path))

    result = dataset_status(str(config_path))
    for section in ("paths_info", "summary", "readiness", "run_stats", "info"):
        assert section in result
    assert result["info"]["dataset"] == dataset
