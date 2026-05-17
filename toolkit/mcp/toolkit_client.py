"""MCP toolkit client — public API surface.

This module is a thin facade. The actual implementation lives in dedicated sub-modules:

- errors: ToolkitClientError
- path_safety: _safe_path, _load_cfg
- cli_adapter: inspect_paths (direct call, no subprocess)
- schema_ops: list_runs, show_schema, raw_profile, run_state, summary, review_readiness

Note: run_state is kept here for internal use (tests) but is no longer a registered MCP tool.
Use inspect_paths (with run_file_count, years_seen) or summary (with latest_run_record) instead.
"""

from __future__ import annotations

# ruff: noqa: F401
from toolkit.mcp.errors import ToolkitClientError
from toolkit.mcp.cli_adapter import inspect_paths
from toolkit.mcp.schema_ops import (
    csv_preview,
    list_runs,
    raw_profile,
    review_readiness,
    run_state,
    run_summary,
    schema_diff,
    show_schema,
    summary,
)
