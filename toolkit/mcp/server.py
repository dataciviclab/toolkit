"""Toolkit MCP server.

Espone 13 tool read-only per ispezione della pipeline toolkit:
- ispezione standard: inspect_paths, show_schema, raw_profile, summary
- diagnostica: review_readiness, schema_diff, run_summary
- run history: list_runs
- discovery: list_candidates, dataset_info
- preview: csv_preview, clean_preview, raw_preview

Usa ``lab_connectors.mcp`` per init standardizzato, error handling e logging.
"""

from __future__ import annotations

from typing import Any

from lab_connectors.mcp import create_mcp_server, guard_timed

from .toolkit_client import (
    clean_preview as clean_preview_impl,
    csv_preview as csv_preview_impl,
    dataset_info as dataset_info_impl,
    inspect_paths as inspect_paths_impl,
    list_candidates as list_candidates_impl,
    list_runs as list_runs_impl,
    raw_preview as raw_preview_impl,
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
        "schemi, stato run e preview dati del toolkit. "
        "Supporta slug dataset (es. 'terna-electricity-by-source') "
        "al posto del path assoluto a dataset.yml."
    ),
)


@mcp.tool(
    description="Mostra il path contract risolto per un dataset config.", structured_output=True
)
def toolkit_inspect_paths(config_path: str, year: int = 0) -> dict[str, Any]:
    return guard_timed(inspect_paths_impl, "toolkit_inspect_paths", config_path, year or None)


@mcp.tool(description="Mostra lo schema di raw, clean o mart.", structured_output=True)
def toolkit_show_schema(config_path: str, layer: str = "clean", year: int = 0) -> dict[str, Any]:
    return guard_timed(show_schema_impl, "toolkit_show_schema", config_path, layer, year or None)


@mcp.tool(
    description="Mostra il profilo raw: encoding, delimiter, colonne, missingness e mapping suggestions.",
    structured_output=True,
)
def toolkit_raw_profile(config_path: str, year: int = 0) -> dict[str, Any]:
    return guard_timed(raw_profile_impl, "toolkit_raw_profile", config_path, year or None)


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
    return guard_timed(run_summary_impl, "toolkit_run_summary", config_path, year or None, since=since, until=until)


@mcp.tool(
    description="Mostra un riepilogo diagnostico minimo per un dataset config.",
    structured_output=True,
)
def toolkit_summary(config_path: str, year: int = 0) -> dict[str, Any]:
    return guard_timed(summary_impl, "toolkit_summary", config_path, year or None)


@mcp.tool(
    description="Check di readiness per review candidate: config, layer, output e coerenza run record.",
    structured_output=True,
)
def toolkit_review_readiness(config_path: str, year: int = 0) -> dict[str, Any]:
    return guard_timed(review_readiness_impl, "toolkit_review_readiness", config_path, year or None)


@mcp.tool(
    description="Elenca tutti i dataset disponibili in dataset-incubator (candidates e support_datasets). Opzionalmente filtra per last_run_status.",
    structured_output=True,
)
def toolkit_list_candidates(
    stage: str = "all",
    status_filter: str | None = None,
) -> dict[str, Any]:
    return guard_timed(list_candidates_impl, "toolkit_list_candidates", stage, status_filter)


@mcp.tool(
    description="Info di base da un dataset.yml: fonte, URL, anni, tabelle mart, support datasets.",
    structured_output=True,
)
def toolkit_dataset_info(config_path: str) -> dict[str, Any]:
    return guard_timed(dataset_info_impl, "toolkit_dataset_info", config_path)


@mcp.tool(
    description="Preview dei dati puliti (clean parquet) o mart. Mostra schema + prime N righe.",
    structured_output=True,
)
def toolkit_clean_preview(
    config_path: str,
    layer: str = "clean",
    mart_index: int = 0,
    year: int = 0,
    limit: int = 10,
) -> dict[str, Any]:
    return guard_timed(clean_preview_impl, "toolkit_clean_preview", config_path, layer, mart_index, year or None, limit)


@mcp.tool(
    description="Preview del raw file primario (CSV) di un dataset. Wrapper su csv_preview + inspect_paths.",
    structured_output=True,
)
def toolkit_raw_preview(
    config_path: str,
    year: int = 0,
    limit: int = 20,
) -> dict[str, Any]:
    return guard_timed(raw_preview_impl, "toolkit_raw_preview", config_path, year or None, limit)


@mcp.tool(
    description="Confronta i segnali di schema raw (encoding, colonne, ecc.) tra gli anni configurati per un dataset.",
    structured_output=True,
)
def toolkit_schema_diff(config_path: str) -> dict[str, Any]:
    return guard_timed(schema_diff_impl, "toolkit_schema_diff", config_path)


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
    return guard_timed(list_runs_impl, "toolkit_list_runs", config_path, year or None, since=since, until=until, status=status, limit=limit, cross_year=cross_year)


@mcp.tool(
    description="Legge un CSV usando la stessa pipeline di profile_raw (sniff_source_file + profile_with_read_cfg). "
    "Restituisce schema, preview, mapping_suggestions e parametri sniff (delim, encoding, decimal, skip). "
    "Utile per ispezionare rapidamente il contenuto di un file raw senza runnare la pipeline.",
    structured_output=True,
)
def toolkit_csv_preview(csv_path: str, limit: int = 20) -> dict[str, Any]:
    return guard_timed(csv_preview_impl, "toolkit_csv_preview", csv_path, limit)


if __name__ == "__main__":
    mcp.run()
