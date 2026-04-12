from __future__ import annotations

from typing import Any

from mcp.server.fastmcp import FastMCP

from .toolkit_client import (
    ToolkitClientError,
    blocker_hints as blocker_hints_impl,
    inspect_paths as inspect_paths_impl,
    review_readiness as review_readiness_impl,
    run_state as run_state_impl,
    summary as summary_impl,
    show_schema as show_schema_impl,
)


mcp = FastMCP(
    name="toolkit",
    instructions=(
        "Server MCP locale, read-only, per ispezionare path risolti, schemi e stato run del toolkit."
    ),
)


def _guard(fn, *args, **kwargs) -> dict[str, Any]:
    try:
        return fn(*args, **kwargs)
    except ToolkitClientError as exc:
        return {"error": str(exc)}


@mcp.tool(
    description="Mostra il path contract risolto per un dataset config.", structured_output=True
)
def toolkit_inspect_paths(config_path: str, year: int = 0) -> dict[str, Any]:
    return _guard(inspect_paths_impl, config_path, year or None)


@mcp.tool(description="Mostra lo schema di raw, clean o mart.", structured_output=True)
def toolkit_show_schema(config_path: str, layer: str = "clean", year: int = 0) -> dict[str, Any]:
    return _guard(show_schema_impl, config_path, layer, year or None)


@mcp.tool(
    description="Mostra lo stato minimo dei run per un dataset config.", structured_output=True
)
def toolkit_run_state(config_path: str, year: int = 0) -> dict[str, Any]:
    return _guard(run_state_impl, config_path, year or None)


@mcp.tool(
    description="Mostra un riepilogo diagnostico minimo per un dataset config.",
    structured_output=True,
)
def toolkit_summary(config_path: str, year: int = 0) -> dict[str, Any]:
    return _guard(summary_impl, config_path, year or None)


@mcp.tool(
    description="Segnala blocker e warning leggeri confrontando config dichiarata e output reali.",
    structured_output=True,
)
def toolkit_blocker_hints(config_path: str, year: int = 0) -> dict[str, Any]:
    return _guard(blocker_hints_impl, config_path, year or None)


@mcp.tool(
    description="Check di readiness per review candidate: config, layer, output e coerenza run record.",
    structured_output=True,
)
def toolkit_review_readiness(config_path: str, year: int = 0) -> dict[str, Any]:
    return _guard(review_readiness_impl, config_path, year or None)


if __name__ == "__main__":
    mcp.run()
