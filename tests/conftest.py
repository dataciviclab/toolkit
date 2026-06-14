from __future__ import annotations

from pathlib import Path

import pytest


# ---- Test policy markers (Lab-wide) ----------------------------------------
# Marker are declared in pytest.ini. See lab-ops/operations/test-policy.md.


# ---- Existing Lab markers ---------------------------------------------------

CORE_TESTS = {
    "test_cli_inspect_paths.py",
    "test_cli_query.py",
    "test_duckdb_shape.py",
    "test_cli_path_contract.py",
    "test_cli_resume.py",
    "test_cli_scout_url.py",
    "test_cli_status.py",
    "test_config_helpers.py",
    "test_config_legacy.py",
    "test_config_loading.py",
    "test_metadata_hash.py",
    "test_paths.py",
    "test_project_example_e2e.py",
    "test_run_context.py",
    "test_run_dry_run.py",
    "test_run_validation_gate.py",
    "test_smoke_tiny_e2e.py",
    "test_validate_layers.py",
    "test_validate_rules.py",
}

ADVANCED_TESTS = {
    "test_clean_csv_columns.py",
    "test_clean_duckdb_read.py",
    "test_clean_input_selection.py",
    "test_extractors.py",
    "test_logging_context.py",
    "test_profile_sniff.py",
    "test_raw_ext_inference.py",
    "test_raw_profile_hints.py",
    "test_registry.py",
}

COMPAT_TESTS: set[str] = set()


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        name = item.path.name
        if name in CORE_TESTS:
            item.add_marker(pytest.mark.core)
        elif name in ADVANCED_TESTS:
            item.add_marker(pytest.mark.advanced)
        elif name in COMPAT_TESTS:
            item.add_marker(pytest.mark.compat)


# ------------------------------------------------------------------
# Shared fixtures
# ------------------------------------------------------------------


@pytest.fixture
def runner():
    """CLI runner for Typer/Click commands.

    Usage::

        def test_something(runner):
            result = runner.invoke(app, ["run", "--help"])
            assert result.exit_code == 0
    """
    from typer.testing import CliRunner

    return CliRunner()


@pytest.fixture
def project_example(tmp_path: Path) -> Path:
    """Copy ``project-example/`` to a temp directory.

    Returns the path to the copy. The ``_smoke_out`` directory is
    excluded to avoid stale artifacts in CI.
    """
    import shutil

    src = Path("project-example")
    dst = tmp_path / "project-example"
    shutil.copytree(src, dst, ignore=shutil.ignore_patterns("_smoke_out"))
    return dst


# ------------------------------------------------------------------
# Smoke template fixtures (parametrizzate)
# ------------------------------------------------------------------


def _discover_smoke_fixtures(offline_only: bool = True) -> list[pytest.param]:
    """Raccoglie gli smoke e restituisce parametri pytest pronti per l'uso."""
    from _smoke_registry import discover_offline_smokes, discover_smokes

    smokes = discover_offline_smokes() if offline_only else discover_smokes()
    return [pytest.param(s, id=s.name, marks=[]) for s in smokes]


@pytest.fixture(params=_discover_smoke_fixtures(offline_only=True))
def smoke_offline(tmp_path: Path, request: pytest.FixtureRequest) -> Path:
    """Copia uno smoke offline (local_file) in tmp_path e ritorna il path.

    Parametrizzato automaticamente su tutti gli smoke che non richiedono rete.
    """
    import shutil

    from _smoke_registry import SmokeTemplate

    smoke: SmokeTemplate = request.param
    dst = tmp_path / smoke.name
    shutil.copytree(smoke.path, dst, ignore=shutil.ignore_patterns("_smoke_out", "README.md"))
    return dst


@pytest.fixture
def smoke_offline_dir(smoke_offline: Path) -> Path:
    """Convenienza: come ``smoke_offline`` ma ritorna direttamente lo smoke copiato.

    Utile quando un test ha bisogno di più fixtures e vuole un nome esplicito.
    """
    return smoke_offline


@pytest.fixture
def chdir_tmp(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Chdir to a clean temp directory for the duration of the test.

    Equivalent to ``monkeypatch.chdir(tmp_path)`` — saves a line in
    every test that needs a clean working directory.
    """
    monkeypatch.chdir(tmp_path)
    return tmp_path
