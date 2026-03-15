from __future__ import annotations

import pytest


CORE_TESTS = {
    "test_cli_all_commands.py",
    "test_cli_inspect_paths.py",
    "test_cli_path_contract.py",
    "test_cli_resume.py",
    "test_cli_scout_url.py",
    "test_cli_status.py",
    "test_config.py",
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
    "test_artifacts_policy.py",
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

COMPAT_TESTS = {
    "test_deprecated_shims.py",
}


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        name = item.path.name
        if name in CORE_TESTS:
            item.add_marker(pytest.mark.core)
        elif name in ADVANCED_TESTS:
            item.add_marker(pytest.mark.advanced)
        elif name in COMPAT_TESTS:
            item.add_marker(pytest.mark.compat)
