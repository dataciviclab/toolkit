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
    "test_project_example_metadata.py",
    "test_run_context.py",
    "test_smoke_templates_golden_path.py",
    "test_smoke_templates_contract_years.py",
    "test_run_dry_run.py",
    "test_run_validation_gate.py",
    "test_smoke_e2e_flow.py",
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


def _discover_smoke_fixtures() -> list[pytest.param]:
    """Raccoglie gli smoke testabili offline: nativi ``local_file`` + quelli
    con ``dataset.offline.yml`` (source_type ``http_file`` con server locale)."""
    from _smoke_registry import discover_testable_offline_smokes

    smokes = discover_testable_offline_smokes()
    return [pytest.param(s, id=s.name, marks=[]) for s in smokes]


SMOKE_PORT_PLACEHOLDER = "{SMOKE_PORT}"


@pytest.fixture(scope="session")
def smoke_http_server():
    """Avvia un server HTTP locale su una porta libera per servirire i
    fixture degli smoke offline che preservano il source_type ``http_file``.

    Il server serve i file da ``smoke/`` con directory fissa, immune da
    ``chdir`` nei test.
    """
    import http.server
    import socket
    import threading

    toolkit_root = Path(__file__).resolve().parent.parent
    smoke_dir = toolkit_root / "smoke"

    # Handler che serve sempre dalla directory smoke/, immune da future chdir
    class _SmokeHTTPHandler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(smoke_dir), **kwargs)

    # Porta libera
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()

    server = http.server.HTTPServer(("127.0.0.1", port), _SmokeHTTPHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield port
    finally:
        server.shutdown()


@pytest.fixture(params=_discover_smoke_fixtures())
def smoke_offline(tmp_path: Path, request: pytest.FixtureRequest) -> Path:
    """Copia uno smoke testabile offline in tmp_path e ritorna il path.

    Parametrizzato su 3 template: 2 nativi ``local_file`` + 1 con
    ``http_file`` servito da server HTTP locale.

    Se lo smoke usa ``{SMOKE_PORT}`` nel config, sostituisce il placeholder
    con la porta reale del server.
    """
    import shutil

    from _smoke_registry import SmokeTemplate, OFFLINE_CONFIG_NAME, ONLINE_CONFIG_NAME

    smoke: SmokeTemplate = request.param
    dst = tmp_path / smoke.name
    shutil.copytree(smoke.path, dst, ignore=shutil.ignore_patterns("_smoke_out", "README.md"))

    # Determina quale file config usare
    offline_src = dst / OFFLINE_CONFIG_NAME
    online_dst = dst / ONLINE_CONFIG_NAME
    use_offline = offline_src.exists() and smoke._config_name == OFFLINE_CONFIG_NAME

    config_file = offline_src if use_offline else online_dst

    # Se il config contiene {SMOKE_PORT}, avvia il server e sostituisci
    if config_file.exists() and SMOKE_PORT_PLACEHOLDER in config_file.read_text(encoding="utf-8"):
        port = request.getfixturevalue("smoke_http_server")
        content = config_file.read_text(encoding="utf-8")
        content = content.replace(SMOKE_PORT_PLACEHOLDER, str(port))
        config_file.write_text(content, encoding="utf-8")

    # Rinomina dataset.offline.yml → dataset.yml per trasparenza ai test
    if use_offline:
        offline_src.rename(online_dst)

    return dst


@pytest.fixture
def chdir_tmp(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Chdir to a clean temp directory for the duration of the test.

    Equivalent to ``monkeypatch.chdir(tmp_path)`` — saves a line in
    every test that needs a clean working directory.
    """
    monkeypatch.chdir(tmp_path)
    return tmp_path
