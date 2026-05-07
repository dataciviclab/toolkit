"""TypedDict contracts for CLI --json output consumed by MCP adapters.

Each function in ``cli_adapter.py`` that invokes a ``toolkit <command> --json``
subprocess MUST validate the result against the corresponding contract here.
This ensures CLI output shape stays aligned with what MCP consumers expect.
"""

from __future__ import annotations

from typing import Any, TypedDict


class RawPaths(TypedDict):
    dir: str
    metadata: str
    manifest: str
    validation: str


class CleanPaths(TypedDict):
    dir: str
    output: str
    metadata: str
    manifest: str
    validation: str


class MartPaths(TypedDict):
    dir: str
    outputs: list[str]
    metadata: str
    manifest: str
    validation: str


class LayerPaths(TypedDict):
    raw: RawPaths
    clean: CleanPaths
    mart: MartPaths
    support: list[Any]
    run_dir: str


class RawHints(TypedDict, total=False):
    primary_output_file: str | None
    suggested_read_path: str
    suggested_read_exists: bool
    encoding: str | None
    delim: str | None
    decimal: str | None
    skip: int | None
    warnings: list[str]


class LatestRun(TypedDict, total=False):
    run_id: str | None
    status: str | None
    started_at: str | None
    path: str


class InspectPathsResult(TypedDict):
    dataset: str
    year: int
    config_path: str
    root: str
    paths: LayerPaths
    run_file_count: int
    years_seen: list[int]
    raw_hints: RawHints
    layer_profiles: dict[str, Any]
    latest_run: LatestRun | None


# --- blocker-hints ---

class Hint(TypedDict):
    code: str
    severity: str  # "blocker" | "warning"
    message: str


class BlockerHintsResult(TypedDict, total=False):
    dataset: str | None
    config_path: str
    year: str | None
    hint_count: int
    hints: list[Hint]
    blocker_count: int
    warning_count: int


# --- review-readiness (MCP only, no CLI) ---

class MartOutputCheck(TypedDict):
    name: str
    exists: bool
    readable: bool | None
    rows: int | None


class ReadinessCheck(TypedDict):
    check: str
    ok: bool
    detail: str | list[MartOutputCheck]


class ReviewReadinessResult(TypedDict, total=False):
    dataset: str | None
    config_path: str
    year: str | None
    readiness: str  # "ready" | "needs-review" | "incomplete"
    check_count: int
    ok_count: int
    fail_count: int
    checks: list[ReadinessCheck]


# --- schema-diff ---

class RawSchemaEntry(TypedDict, total=False):
    year: int
    raw_dir: str
    raw_exists: bool
    primary_output_file: str | None
    file_used: str | None
    profile_source: str | None
    is_binary_file: bool | None
    encoding: str | None
    delim: str | None
    decimal: str | None
    skip: int | None
    header_line: str | None
    columns_count: int
    columns_preview: list[str]
    warnings: list[str]


class SchemaComparison(TypedDict, total=False):
    from_year: int
    to_year: int
    from_columns_count: int
    to_columns_count: int
    added_columns: list[str]
    removed_columns: list[str]
    changed: bool


class SchemaDiffResult(TypedDict, total=False):
    dataset: str
    config_path: str
    years: list[int]
    entries: list[RawSchemaEntry]
    comparisons: list[SchemaComparison]
