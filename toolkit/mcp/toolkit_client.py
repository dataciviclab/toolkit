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
from toolkit.mcp.discovery import list_candidates
from toolkit.mcp.errors import ToolkitClientError
from toolkit.mcp.cli_adapter import inspect_paths
from toolkit.mcp.schema_ops import (
    clean_preview,
    csv_preview,
    dataset_info,
    list_runs,
    raw_preview,
    raw_profile,
    review_readiness,
    run_state,
    run_summary,
    schema_diff,
    show_schema,
    summary,
)
from toolkit.mcp.scout_ops import (
    mcp_ckan_package_show,
    mcp_html_extract_links,
    mcp_infer_topic,
    mcp_probe_url,
    mcp_probe_url_routed,
    mcp_sparql_query,
)
