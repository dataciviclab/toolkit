"""Toolkit MCP server.

Espone 9 tool read-only per ispezione della pipeline toolkit.
Usa ``lab_connectors.mcp`` per init standardizzato, error handling e logging.
"""

from __future__ import annotations

from typing import Any

from lab_connectors.mcp import create_mcp_server, guard

from .toolkit_client import (
    csv_preview as csv_preview_impl,
    inspect_paths as inspect_paths_impl,
    list_runs as list_runs_impl,
    raw_profile as raw_profile_impl,
    review_readiness as review_readiness_impl,
    run_summary as run_summary_impl,
    schema_diff as schema_diff_impl,
    summary as summary_impl,
    show_schema as show_schema_impl,
)

mcp = create_mcp_server(
    name="toolkit",
    instructions=(
        "Server MCP locale, read-only, per ispezionare path risolti, "
        "schemi e stato run del toolkit."
    ),
)


@mcp.tool(
    description="Mostra il path contract risolto per un dataset config.", structured_output=True
)
def toolkit_inspect_paths(config_path: str, year: int = 0) -> dict[str, Any]:
    return guard(inspect_paths_impl, config_path, year or None)


@mcp.tool(description="Mostra lo schema di raw, clean o mart.", structured_output=True)
def toolkit_show_schema(config_path: str, layer: str = "clean", year: int = 0) -> dict[str, Any]:
    return guard(show_schema_impl, config_path, layer, year or None)


@mcp.tool(
    description="Mostra il profilo raw: encoding, delimiter, colonne, missingness e mapping suggestions.",
    structured_output=True,
)
def toolkit_raw_profile(config_path: str, year: int = 0) -> dict[str, Any]:
    return guard(raw_profile_impl, config_path, year or None)


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
    return guard(run_summary_impl, config_path, year or None, since=since, until=until)


@mcp.tool(
    description="Mostra un riepilogo diagnostico minimo per un dataset config.",
    structured_output=True,
)
def toolkit_summary(config_path: str, year: int = 0) -> dict[str, Any]:
    return guard(summary_impl, config_path, year or None)


@mcp.tool(
    description="Check di readiness per review candidate: config, layer, output e coerenza run record.",
    structured_output=True,
)
def toolkit_review_readiness(config_path: str, year: int = 0) -> dict[str, Any]:
    return guard(review_readiness_impl, config_path, year or None)


@mcp.tool(
    description="Confronta i segnali di schema raw (encoding, colonne, ecc.) tra gli anni configurati per un dataset.",
    structured_output=True,
)
def toolkit_schema_diff(config_path: str) -> dict[str, Any]:
    return guard(schema_diff_impl, config_path)


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
    return guard(list_runs_impl, config_path, year or None, since=since, until=until, status=status, limit=limit, cross_year=cross_year)


@mcp.tool(
    description="Legge un CSV usando la stessa pipeline di profile_raw (sniff_source_file + profile_with_read_cfg). "
    "Restituisce schema, preview, mapping_suggestions e parametri sniff (delim, encoding, decimal, skip). "
    "Utile per ispezionare rapidamente il contenuto di un file raw senza runnare la pipeline.",
    structured_output=True,
)
def toolkit_csv_preview(csv_path: str, limit: int = 20) -> dict[str, Any]:
    return guard(csv_preview_impl, csv_path, limit)


if __name__ == "__main__":
    mcp.run()
