"""Tests for ``toolkit run --dry-run``."""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path

import duckdb
import pytest

from toolkit.cli.app import app
from toolkit.cli.cmd_run import run_year
from toolkit.core.config import load_config
from tests.helpers import make_dataset_yml, make_standard_sql


# ── Basic patterns ──────────────────────────────────────────────────────────


@pytest.mark.policy
def test_run_dry_run_prints_plan_and_creates_only_run_record(
    tmp_path: Path,
    runner,
) -> None:
    make_standard_sql(tmp_path)
    config_path = make_dataset_yml(
        tmp_path / "dataset.yml",
        mart_tables=[("mart_example", "sql/mart/mart_example.sql")],
    )
    root_dir = tmp_path / "out"

    result = runner.invoke(app, ["run", "all", "--config", str(config_path), "--dry-run"])

    assert result.exit_code == 0
    assert "Execution Plan" in result.output
    assert "status: DRY_RUN" in result.output
    assert "steps: raw, clean, mart" in result.output
    assert "sql_validation: OK" in result.output

    runs_dir = root_dir / "data" / "_runs" / "demo_ds" / "2022"
    records = list(runs_dir.glob("*.json"))
    assert len(records) == 1

    record = json.loads(records[0].read_text(encoding="utf-8"))
    assert record["status"] == "DRY_RUN"
    assert record["layers"]["raw"]["status"] == "PENDING"

    assert not (root_dir / "data" / "raw" / "demo_ds" / "2022").exists()
    assert not (root_dir / "data" / "clean" / "demo_ds" / "2022").exists()
    assert not (root_dir / "data" / "mart" / "demo_ds" / "2022").exists()


@pytest.mark.policy
def test_run_dry_run_fails_on_clean_sql_syntax_error(tmp_path: Path, runner) -> None:
    make_standard_sql(tmp_path)
    # Replace clean.sql with syntax error
    (tmp_path / "sql" / "clean.sql").write_text("select from raw_input", encoding="utf-8")

    config_path = make_dataset_yml(
        tmp_path / "dataset.yml",
        mart_tables=[("mart_example", "sql/mart/mart_example.sql")],
    )

    result = runner.invoke(app, ["run", "all", "--config", str(config_path), "--dry-run"])

    assert result.exit_code != 0
    assert "CLEAN SQL dry-run failed" in str(result.exception)


@pytest.mark.policy
def test_run_dry_run_fails_on_mart_sql_binding_error(tmp_path: Path, runner) -> None:
    make_standard_sql(tmp_path)
    (tmp_path / "sql" / "clean.sql").write_text(
        'select "x" as value from raw_input',
        encoding="utf-8",
    )
    (tmp_path / "sql" / "mart" / "mart_example.sql").write_text(
        "select missing_col from clean_input",
        encoding="utf-8",
    )

    config_path = make_dataset_yml(
        tmp_path / "dataset.yml",
        mart_tables=[("mart_example", "sql/mart/mart_example.sql")],
        extra='  read:\n    columns:\n      x: "VARCHAR"',
    )

    result = runner.invoke(app, ["run", "all", "--config", str(config_path), "--dry-run"])

    assert result.exit_code != 0
    assert "MART SQL dry-run failed" in str(result.exception)


@pytest.mark.policy
def test_run_dry_run_accepts_unquoted_raw_columns_without_read_columns(
    tmp_path: Path,
    runner,
) -> None:
    make_standard_sql(tmp_path)
    (tmp_path / "sql" / "clean.sql").write_text(
        "select x as value from raw_input",
        encoding="utf-8",
    )

    config_path = make_dataset_yml(
        tmp_path / "dataset.yml",
        mart_tables=[("mart_example", "sql/mart/mart_example.sql")],
    )

    result = runner.invoke(app, ["run", "all", "--config", str(config_path), "--dry-run"])

    assert result.exit_code == 0
    assert "sql_validation: OK" in result.output


@pytest.mark.policy
def test_run_dry_run_accepts_mart_sql_with_root_posix_placeholder(
    tmp_path: Path,
    runner,
) -> None:
    make_standard_sql(tmp_path)
    root_dir = tmp_path / "out"
    lookup_path = root_dir / "lookup" / "mart_lookup_2022.parquet"
    lookup_path.parent.mkdir(parents=True, exist_ok=True)
    duckdb.execute(
        f"COPY (SELECT 1 AS lookup_value) TO '{lookup_path.as_posix()}' (FORMAT PARQUET)"
    )
    (tmp_path / "sql" / "mart" / "mart_example.sql").write_text(
        "select * from read_parquet('{root_posix}/lookup/mart_lookup_2022.parquet')",
        encoding="utf-8",
    )

    config_path = make_dataset_yml(
        tmp_path / "dataset.yml",
        root=root_dir,
        mart_tables=[("mart_example", "sql/mart/mart_example.sql")],
    )

    result = runner.invoke(app, ["run", "all", "--config", str(config_path), "--dry-run"])

    assert result.exit_code == 0
    assert "sql_validation: OK" in result.output


# ── Support datasets ────────────────────────────────────────────────────────


def _make_support_config(
    support_root: Path, support_config: Path, tables: list[tuple[str, str]]
) -> Path:
    """Write a support dataset.yml and return its path."""
    make_dataset_yml(
        support_config,
        root=support_root,
        name="lookup_ds",
        years=[2024],
        clean_sql="sql/clean.sql",
        mart_tables=tables,
    )
    return support_config


@pytest.mark.policy
def test_run_dry_run_accepts_mart_sql_with_support_placeholder(
    tmp_path: Path,
    runner,
) -> None:
    make_standard_sql(tmp_path)
    root_dir = tmp_path / "out"
    support_root = tmp_path / "support_out"

    support_output = support_root / "data" / "mart" / "lookup_ds" / "2024" / "lookup_table.parquet"
    support_output.parent.mkdir(parents=True, exist_ok=True)
    duckdb.execute(
        f"COPY (SELECT 7 AS lookup_value) TO '{support_output.as_posix()}' (FORMAT PARQUET)"
    )

    support_config = _make_support_config(
        support_root,
        tmp_path / "support_dataset.yml",
        [("lookup_table", "sql/lookup.sql")],
    )

    (tmp_path / "sql" / "mart" / "mart_example.sql").write_text(
        "select * from read_parquet('{support.lookup.mart}')",
        encoding="utf-8",
    )

    config_path = make_dataset_yml(
        tmp_path / "dataset.yml",
        root=root_dir,
        mart_tables=[("mart_example", "sql/mart/mart_example.sql")],
        extra=(
            "support:\n"
            '  - name: "lookup"\n'
            f'    config: "{support_config.as_posix()}"\n'
            "    years: [2024]"
        ),
    )

    result = runner.invoke(app, ["run", "all", "--config", str(config_path), "--dry-run"])

    assert result.exit_code == 0
    assert "sql_validation: OK" in result.output


@pytest.mark.policy
def test_run_dry_run_fails_when_support_output_is_missing(
    tmp_path: Path,
    runner,
) -> None:
    make_standard_sql(tmp_path)
    root_dir = tmp_path / "out"
    support_root = tmp_path / "support_out"

    support_config = _make_support_config(
        support_root,
        tmp_path / "support_dataset.yml",
        [("lookup_table", "sql/lookup.sql")],
    )

    (tmp_path / "sql" / "mart" / "mart_example.sql").write_text(
        "select * from read_parquet('{support.lookup.mart}')",
        encoding="utf-8",
    )

    config_path = make_dataset_yml(
        tmp_path / "dataset.yml",
        root=root_dir,
        mart_tables=[("mart_example", "sql/mart/mart_example.sql")],
        extra=(
            "support:\n"
            '  - name: "lookup"\n'
            f'    config: "{support_config.as_posix()}"\n'
            "    years: [2024]"
        ),
    )

    result = runner.invoke(app, ["run", "all", "--config", str(config_path), "--dry-run"])

    assert result.exit_code == 0, result.output
    assert "DRY_RUN" in result.output


@pytest.mark.policy
def test_run_dry_run_fails_when_support_outputs_are_only_partially_present(
    tmp_path: Path,
    runner,
) -> None:
    make_standard_sql(tmp_path)
    root_dir = tmp_path / "out"
    support_root = tmp_path / "support_out"

    support_output = support_root / "data" / "mart" / "lookup_ds" / "2024" / "lookup_a.parquet"
    support_output.parent.mkdir(parents=True, exist_ok=True)
    duckdb.execute(
        f"COPY (SELECT 7 AS lookup_value) TO '{support_output.as_posix()}' (FORMAT PARQUET)"
    )

    support_config = _make_support_config(
        support_root,
        tmp_path / "support_dataset.yml",
        [("lookup_a", "sql/lookup_a.sql"), ("lookup_b", "sql/lookup_b.sql")],
    )

    (tmp_path / "sql" / "mart" / "mart_example.sql").write_text(
        "select * from read_parquet('{support.lookup.mart}')",
        encoding="utf-8",
    )

    config_path = make_dataset_yml(
        tmp_path / "dataset.yml",
        root=root_dir,
        mart_tables=[("mart_example", "sql/mart/mart_example.sql")],
        extra=(
            "support:\n"
            '  - name: "lookup"\n'
            f'    config: "{support_config.as_posix()}"\n'
            "    years: [2024]"
        ),
    )

    result = runner.invoke(app, ["run", "all", "--config", str(config_path), "--dry-run"])

    assert result.exit_code == 0, result.output
    assert "DRY_RUN" in result.output


# ── Logger context ──────────────────────────────────────────────────────────


@pytest.mark.policy
def test_run_year_logs_effective_root_context(tmp_path: Path, caplog) -> None:
    make_standard_sql(tmp_path)
    config_path = make_dataset_yml(
        tmp_path / "dataset.yml",
        mart_tables=[("mart_example", "sql/mart/mart_example.sql")],
    )
    root_dir = tmp_path / "out"

    cfg = load_config(config_path)
    logger = logging.getLogger("test.run_dry_run")
    logger.handlers = [caplog.handler]
    logger.propagate = False
    logger.setLevel(logging.INFO)

    with caplog.at_level(logging.INFO, logger="test.run_dry_run"):
        run_year(cfg, 2022, step="all", dry_run=True, logger=logger)

    assert "RUN context | dataset=demo_ds year=2022" in caplog.text
    assert f"base_dir={tmp_path}" in caplog.text
    assert f"effective_root={root_dir}" in caplog.text
    assert "root_source=yml" in caplog.text


# ── Mart-only / compose ─────────────────────────────────────────────────────


@pytest.mark.policy
def test_run_dry_run_accepts_mart_only_config(tmp_path: Path, runner) -> None:
    mart_sql = tmp_path / "compose" / "sql"
    mart_sql.mkdir(parents=True, exist_ok=True)
    source_path = tmp_path / "external_source.parquet"
    duckdb.execute("COPY (SELECT 1 AS value) TO ? (FORMAT PARQUET)", [str(source_path)])
    (mart_sql / "mart_example.sql").write_text(
        f"select * from read_parquet('{source_path.as_posix()}')",
        encoding="utf-8",
    )

    config_path = make_dataset_yml(
        tmp_path / "compose" / "dataset.yml",
        name="compose_demo",
        clean_sql=None,
        mart_tables=[("mart_example", "sql/mart_example.sql")],
    )

    result = runner.invoke(app, ["run", "mart", "--config", str(config_path), "--dry-run"])

    assert result.exit_code == 0
    assert "Execution Plan" in result.output
    assert "steps: mart" in result.output
    assert "sql_validation: OK" in result.output


@pytest.mark.policy
def test_run_dry_run_all_fails_readably_on_mart_only_config(
    tmp_path: Path,
    runner,
) -> None:
    mart_sql = tmp_path / "compose" / "sql"
    mart_sql.mkdir(parents=True, exist_ok=True)
    (mart_sql / "mart_example.sql").write_text("select 1 as value", encoding="utf-8")

    config_path = make_dataset_yml(
        tmp_path / "compose" / "dataset.yml",
        name="compose_demo",
        clean_sql=None,
        mart_tables=[("mart_example", "sql/mart_example.sql")],
    )

    result = runner.invoke(app, ["run", "all", "--config", str(config_path), "--dry-run"])

    assert result.exit_code != 0
    assert "run all is not supported for mart-only / compose-only configs" in str(result.exception)


@pytest.mark.policy
def test_run_mart_executes_mart_only_config(tmp_path: Path, runner) -> None:
    mart_sql = tmp_path / "compose" / "sql"
    mart_sql.mkdir(parents=True, exist_ok=True)
    source_path = tmp_path / "external_source.parquet"
    duckdb.execute("COPY (SELECT 1 AS value) TO ? (FORMAT PARQUET)", [str(source_path)])
    (mart_sql / "mart_example.sql").write_text(
        f"select * from read_parquet('{source_path.as_posix()}')",
        encoding="utf-8",
    )

    root_dir = tmp_path / "out"
    config_path = make_dataset_yml(
        tmp_path / "compose" / "dataset.yml",
        name="compose_demo",
        root=root_dir,
        clean_sql=None,
        mart_tables=[("mart_example", "sql/mart_example.sql")],
    )

    result = runner.invoke(app, ["run", "mart", "--config", str(config_path)])

    assert result.exit_code == 0
    mart_dir = root_dir / "data" / "mart" / "compose_demo" / "2022"
    assert (mart_dir / "mart_example.parquet").exists()
    assert (mart_dir / "metadata.json").exists()
    assert not (root_dir / "data" / "clean" / "compose_demo" / "2022").exists()


@pytest.mark.policy
def test_run_mart_mart_only_ignores_stale_clean_dir(tmp_path: Path, runner) -> None:
    mart_sql = tmp_path / "compose" / "sql"
    mart_sql.mkdir(parents=True, exist_ok=True)
    root_dir = tmp_path / "out"
    stale_clean = root_dir / "data" / "clean" / "compose_demo" / "2022"
    stale_clean.mkdir(parents=True, exist_ok=True)
    stale_parquet = stale_clean / "compose_demo_2022_clean.parquet"
    duckdb.execute(
        f"COPY (SELECT 1 AS stale_value) TO '{stale_parquet.as_posix()}' (FORMAT PARQUET)"
    )
    (mart_sql / "mart_example.sql").write_text(
        "select stale_value from clean_input",
        encoding="utf-8",
    )

    config_path = make_dataset_yml(
        tmp_path / "compose" / "dataset.yml",
        name="compose_demo",
        clean_sql=None,
        mart_tables=[("mart_example", "sql/mart_example.sql")],
    )

    result = runner.invoke(app, ["run", "mart", "--config", str(config_path)])

    assert result.exit_code != 0
    assert "clean_input" in str(result.exception)
    assert not (
        root_dir / "data" / "mart" / "compose_demo" / "2022" / "mart_example.parquet"
    ).exists()


@pytest.mark.policy
def test_run_all_fails_readably_on_mart_only_config(tmp_path: Path, runner) -> None:
    mart_sql = tmp_path / "compose" / "sql"
    mart_sql.mkdir(parents=True, exist_ok=True)
    (mart_sql / "mart_example.sql").write_text("select 1 as value", encoding="utf-8")

    config_path = make_dataset_yml(
        tmp_path / "compose" / "dataset.yml",
        name="compose_demo",
        clean_sql=None,
        mart_tables=[("mart_example", "sql/mart_example.sql")],
    )

    result = runner.invoke(app, ["run", "all", "--config", str(config_path)])

    assert result.exit_code != 0
    assert "run all is not supported for mart-only / compose-only configs" in str(result.exception)


# ── Raw sources ─────────────────────────────────────────────────────────────


@pytest.mark.policy
def test_run_all_fails_with_bootstrap_hint_when_clean_sql_missing(
    tmp_path: Path,
    runner,
) -> None:
    raw_dir = tmp_path / "data" / "raw" / "demo_ds" / "2022"
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / "demo_ds_2022.csv").write_text("col1;col2\nval1;val2\n", encoding="utf-8")

    config_path = make_dataset_yml(
        tmp_path / "dataset.yml",
        name="demo_ds",
        extra=(
            "raw:\n"
            "  sources:\n"
            "    - type: local_file\n"
            "      args:\n"
            "        path: data/raw/demo_ds/2022/demo_ds_2022.csv\n"
        ),
        clean_sql="sql/clean.sql",  # file does not exist
    )

    result = runner.invoke(app, ["run", "all", "--config", str(config_path)])

    assert result.exit_code != 0
    exc_text = str(result.exception)
    assert "CLEAN SQL file not found" in exc_text
    assert "toolkit run raw" in exc_text


# ── Probe step contract tests ────────────────────────────────────────────────


def _make_probe_cfg(tmp_path: Path) -> tuple:
    """Helper: create a minimal config and return (cfg, year)."""
    from tests.helpers import make_dataset_yml, make_standard_sql

    make_standard_sql(tmp_path)
    config_path = make_dataset_yml(
        tmp_path / "dataset.yml",
        mart_tables=[("mart_example", "sql/mart/mart_example.sql")],
    )
    return load_config(config_path), 2022


class _FakeRawConfig:
    """Minimal RawConfig-like object for probe test."""

    def __init__(self, sources: list):
        self.sources = sources


class _FakeCfg:
    """Minimal config mock for probe tests (ToolkitConfig e' frozen)."""

    def __init__(self, raw_sources: list):
        self.raw = _FakeRawConfig(raw_sources)
        self.base_dir = None


@pytest.mark.contract
def test_probe_calls_probe_url_headers_for_http_source(monkeypatch) -> None:
    """Probe step calls scout probe for http_file sources."""
    calls = []
    monkeypatch.setattr(
        "toolkit.scout.http.probe_url_headers",
        lambda url, timeout=5, client=None: (
            calls.append(url) or {"status_code": 200, "content_type": "text/csv"}
        ),
    )
    from toolkit.cli.cmd_run import _run_probe

    _run_probe(
        _FakeCfg(
            [{"name": "s1", "type": "http_file", "args": {"url": "https://example.com/data.csv"}}]
        ),
        2024,
        logging.getLogger("t"),
    )

    assert len(calls) == 1
    assert "example.com" in calls[0]


@pytest.mark.contract
def test_probe_skips_local_file(monkeypatch) -> None:
    """Probe step does NOT call probe_url_headers for local_file sources."""
    calls = []
    monkeypatch.setattr(
        "toolkit.scout.http.probe_url_headers",
        lambda url, timeout=5: calls.append(url) or {"status_code": 200},
    )
    from toolkit.cli.cmd_run import _run_probe

    _run_probe(
        _FakeCfg([{"name": "s1", "type": "local_file", "args": {"path": "data/file.csv"}}]),
        2024,
        logging.getLogger("t"),
    )

    assert calls == [], "probe_url_headers should NOT be called for local_file"


@pytest.mark.contract
def test_probe_does_not_block_on_error(monkeypatch) -> None:
    """Probe step logs warning but does NOT raise on unreachable source."""
    monkeypatch.setattr(
        "toolkit.scout.http.probe_url_headers",
        lambda url, timeout=5, client=None: (_ for _ in ()).throw(RuntimeError("ConnectionError")),
    )
    from toolkit.cli.cmd_run import _run_probe

    _run_probe(
        _FakeCfg(
            [{"name": "s1", "type": "http_file", "args": {"url": "https://dead.test/data.csv"}}]
        ),
        2024,
        logging.getLogger("t"),
    )


@pytest.mark.contract
def test_probe_logs_ckan_portal(monkeypatch) -> None:
    """Probe step probes CKAN portal_url (API base, not homepage)."""
    calls = []
    monkeypatch.setattr(
        "toolkit.scout.http.probe_url_headers",
        lambda url, timeout=5, client=None: calls.append(url) or {"status_code": 200},
    )
    from toolkit.cli.cmd_run import _run_probe

    _run_probe(
        _FakeCfg(
            [
                {
                    "name": "s1",
                    "type": "ckan",
                    "args": {"portal_url": "https://ckan.test/api/3/action"},
                }
            ]
        ),
        2024,
        logging.getLogger("t"),
    )

    assert len(calls) == 1
    assert "ckan.test/api/3/action" in calls[0], (
        "should probe the full portal_url, not just scheme://host"
    )


@pytest.mark.policy
def test_probe_parallel_execution(monkeypatch) -> None:
    """Probe step esegue fonti multiple in parallelo, non in serie.

    Con 3 fonti che impiegano 0.5s ciascuna, il tempo totale deve
    essere minore di 3 * 0.5 = 1.5s (prova di parallelismo).
    """
    DELAY = 0.5

    def _delayed_probe(url, timeout=5, client=None):
        time.sleep(DELAY)
        return {"status_code": 200, "content_type": "text/csv"}

    monkeypatch.setattr(
        "toolkit.scout.http.probe_url_headers",
        _delayed_probe,
    )
    from toolkit.cli.cmd_run import _run_probe

    sources = [
        {"name": "s1", "type": "http_file", "args": {"url": f"https://src{i}.test/data.csv"}}
        for i in range(3)
    ]

    start = time.perf_counter()
    _run_probe(_FakeCfg(sources), 2024, logging.getLogger("t"))
    elapsed = time.perf_counter() - start

    # Se fosse seriale: 3 * 0.5 = 1.5s + overhead
    # Con parallelismo: ~0.5s + overhead
    assert elapsed < 1.2, (
        f"Troppo lento ({elapsed:.2f}s): le probe sembrano sequenziali. "
        f"Atteso < 1.2s per 3 fonti da {DELAY}s ciascuna."
    )
