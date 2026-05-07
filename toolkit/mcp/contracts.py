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
