from __future__ import annotations

from typing import Any

from mcp.server.fastmcp import FastMCP

from .toolkit_client import (
    ToolkitClientError,
    blocker_hints as blocker_hints_impl,
    inspect_paths as inspect_paths_impl,
    list_runs as list_runs_impl,
    raw_profile as raw_profile_impl,
    review_readiness as review_readiness_impl,
    run_summary as run_summary_impl,
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
    description="Mostra il profilo raw: encoding, delimiter, colonne, missingness e mapping suggestions.",
    structured_output=True,
)
def toolkit_raw_profile(config_path: str, year: int = 0) -> dict[str, Any]:
    return _guard(raw_profile_impl, config_path, year or None)


@mcp.tool(
    description="Statistiche aggregate dei run: totali, successi, fallimenti, durata media.",
    structured_output=True,
)
def toolkit_run_summary(
    config_path: str,
    year: int = 0,
    *,
    since: str | None = None,
    until: str | None = None,
) -> dict[str, Any]:
    return _guard(run_summary_impl, config_path, year or None, since=since, until=until)


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


@mcp.tool(
    description="Lista run records con filtri opzionali. Ritorna record completi (non solo metadata).",
    structured_output=True,
)
def toolkit_list_runs(
    config_path: str,
    year: int = 0,
    *,
    since: str | None = None,
    until: str | None = None,
    status: str | None = None,
    limit: int | None = None,
    cross_year: bool = False,
) -> dict[str, Any]:
    return _guard(list_runs_impl, config_path, year or None, since=since, until=until, status=status, limit=limit, cross_year=cross_year)


if __name__ == "__main__":
    mcp.run()
