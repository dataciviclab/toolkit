"""Schema inspection and readiness diagnostics for the MCP toolkit client.

Provides read-only diagnostics on config, layers, and run records:
- show_schema: schema of a raw/clean/mart layer
- raw_profile: content of _profile/raw_profile.json
- run_state: run directory state and latest run record
- summary: layer-level overview with existence checks
- review_readiness: readiness check for candidate review
- schema_diff: compare RAW schema signals across configured years
- csv_preview: schema + preview of a CSV file via DuckDB auto-detect
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from lab_connectors.mcp.errors import ErrorCode

from toolkit.mcp.errors import ToolkitClientError
from toolkit.mcp.path_safety import _load_cfg, _safe_path
from toolkit.core.run_records import get_run_dir_dataset, list_runs as _list_runs_records


def show_schema(config_path: str, layer: str = "clean", year: int | None = None) -> dict[str, Any]:
    """Mostra lo schema (colonne + tipi) di raw, clean o mart.

    Thin wrapper MCP: delega a ``toolkit.cli.inspect.schema_ops.show_schema``.
    """
    from toolkit.domain.schema import show_schema as _cli_show_schema

    try:
        return _cli_show_schema(config_path, layer=layer, year=year)
    except ValueError as exc:
        raise ToolkitClientError(str(exc), code=ErrorCode.INVALID_PARAMS) from exc
    except FileNotFoundError as exc:
        raise ToolkitClientError(str(exc), code=ErrorCode.PARQUET_NOT_FOUND) from exc


def raw_profile(config_path: str, year: int | None = None) -> dict[str, Any]:
    """Restituisce il contenuto di _profile/raw_profile.json (o suggested_read.yml come fallback).

    Thin wrapper MCP: delega a ``toolkit.cli.layer_ops.raw_profile``.
    """
    from toolkit.domain.layer import raw_profile as _cli_raw_profile

    try:
        return _cli_raw_profile(str(_safe_path(config_path)), year=year)
    except FileNotFoundError as exc:
        raise ToolkitClientError(str(exc), code=ErrorCode.ARTIFACT_NOT_FOUND) from exc


def run_state(config_path: str, year: int | None = None) -> dict[str, Any]:
    """Stato della run directory.

    Thin wrapper MCP: delega a ``toolkit.cli.inspect.readiness_ops.run_state``.
    """
    from toolkit.domain.readiness import run_state as _cli_run_state

    try:
        return _cli_run_state(str(_safe_path(str(config_path))), year=year)
    except FileNotFoundError as exc:
        raise ToolkitClientError(str(exc), code=ErrorCode.CONFIG_NOT_FOUND) from exc


def list_runs(
    config_path: str,
    year: int | None = None,
    *,
    since: str | None = None,
    until: str | None = None,
    status: str | None = None,
    limit: int | None = None,
    cross_year: bool = False,
) -> dict[str, Any]:
    """List run records with optional filters.

    Args:
        config_path: path to dataset.yml
        year: filter to specific year (default: all years)
        since: ISO datetime string — only runs started after this moment
        until: ISO datetime string — only runs started before this moment
        status: filter by status (SUCCESS, FAILED, RUNNING, DRY_RUN)
        limit: max records to return (default 20, None for all)
        cross_year: if True, list runs across all years for this dataset
    """
    from datetime import datetime, timezone

    _, cfg = _load_cfg(str(config_path))
    root = cfg.root

    if cross_year:
        run_dir = get_run_dir_dataset(Path(root), cfg.dataset)
    else:
        if year is None:
            year = cfg.years[0] if cfg.years else 0
        run_dir = Path(root) / "data" / "_runs" / cfg.dataset / str(year)

    since_dt = None
    if since:
        try:
            raw = since.replace("Z", "+00:00")
            since_dt = datetime.fromisoformat(raw)
            if since_dt.tzinfo is None:
                since_dt = since_dt.replace(tzinfo=timezone.utc)
        except ValueError as exc:
            raise ToolkitClientError(
                f"since must be a valid ISO datetime, got: {since}", code=ErrorCode.INVALID_PARAMS
            ) from exc

    until_dt = None
    if until:
        try:
            raw = until.replace("Z", "+00:00")
            until_dt = datetime.fromisoformat(raw)
            if until_dt.tzinfo is None:
                until_dt = until_dt.replace(tzinfo=timezone.utc)
        except ValueError as exc:
            raise ToolkitClientError(
                f"until must be a valid ISO datetime, got: {until}", code=ErrorCode.INVALID_PARAMS
            ) from exc

    valid_statuses = {"SUCCESS", "FAILED", "RUNNING", "DRY_RUN"}
    if status and status not in valid_statuses:
        raise ToolkitClientError(
            f"status must be one of: {', '.join(sorted(valid_statuses))}",
            code=ErrorCode.INVALID_PARAMS,
        )

    limit = limit if limit is not None else 20

    records = _list_runs_records(
        run_dir,
        since=since_dt,
        until=until_dt,
        status=status if status else None,  # type: ignore[arg-type]
        limit=limit,
    )

    return {
        "dataset": cfg.dataset,
        "config_path": str(config_path),
        "requested_year": year,
        "all_years": cross_year,
        "filters": {
            "since": since,
            "until": until,
            "status": status,
            "limit": limit,
        },
        "run_dir": str(run_dir),
        "total_matches": len(records),
        "runs": records,
    }


def run_summary(
    config_path: str,
    year: int | None = None,
    *,
    since: str | None = None,
    until: str | None = None,
) -> dict[str, Any]:
    """Aggregated run statistics for a dataset/year.

    Args:
        config_path: path to dataset.yml
        year: filter to specific year (default: first year in config)
        since: ISO datetime string — only runs started after this moment
        until: ISO datetime string — only runs started before this moment

    Returns: total_runs, success_count, failed_count, run_rate,
    avg_duration_seconds, last_30d_runs, status_breakdown.
    """
    config = _safe_path(config_path)
    _, cfg = _load_cfg(config)
    root = cfg.root

    from datetime import datetime, timezone, timedelta
    from toolkit.core.run_records import get_run_dir, list_runs

    if year is None:
        year = cfg.years[0] if cfg.years else 0
    run_dir = get_run_dir(Path(root), cfg.dataset, year)

    since_dt: datetime | None = None
    if since:
        try:
            raw = since.replace("Z", "+00:00")
            since_dt = datetime.fromisoformat(raw)
            if since_dt.tzinfo is None:
                since_dt = since_dt.replace(tzinfo=timezone.utc)
        except ValueError as exc:
            raise ToolkitClientError(
                f"since must be a valid ISO datetime, got: {since}", code=ErrorCode.INVALID_PARAMS
            ) from exc

    until_dt: datetime | None = None
    if until:
        try:
            raw = until.replace("Z", "+00:00")
            until_dt = datetime.fromisoformat(raw)
            if until_dt.tzinfo is None:
                until_dt = until_dt.replace(tzinfo=timezone.utc)
        except ValueError as exc:
            raise ToolkitClientError(
                f"until must be a valid ISO datetime, got: {until}", code=ErrorCode.INVALID_PARAMS
            ) from exc

    all_records = list_runs(run_dir, since=since_dt, until=until_dt, limit=None)

    if not all_records:
        return {
            "dataset": cfg.dataset,
            "year": year,
            "run_dir": str(run_dir),
            "total_runs": 0,
            "success_count": 0,
            "failed_count": 0,
            "run_rate": None,
            "avg_duration_seconds": None,
            "last_30d_runs": 0,
            "status_breakdown": {},
        }

    total = len(all_records)
    success = sum(1 for r in all_records if r.get("status") == "SUCCESS")
    failed = sum(1 for r in all_records if r.get("status") == "FAILED")
    durations = [
        r.get("duration_seconds") for r in all_records if r.get("duration_seconds") is not None
    ]
    avg_duration = (
        round(sum(d for d in durations if d is not None) / len(durations), 1) if durations else None
    )

    thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)
    last_30d = 0
    for r in all_records:
        started = r.get("started_at", "")
        if started:
            try:
                dt = datetime.fromisoformat(started.replace("Z", "+00:00"))
                if dt >= thirty_days_ago:
                    last_30d += 1
            except ValueError:
                pass

    status_breakdown: dict[str, int] = {}
    for r in all_records:
        s = r.get("status", "UNKNOWN")
        status_breakdown[s] = status_breakdown.get(s, 0) + 1

    return {
        "dataset": cfg.dataset,
        "year": year,
        "run_dir": str(run_dir),
        "filters": {"since": since, "until": until},
        "total_runs": total,
        "success_count": success,
        "failed_count": failed,
        "run_rate": round(success / total * 100, 1) if total > 0 else None,
        "avg_duration_seconds": avg_duration,
        "last_30d_runs": last_30d,
        "status_breakdown": status_breakdown,
    }


def summary(config_path: str, year: int | None = None) -> dict[str, Any]:
    """Layer-level overview with existence checks.

    Thin wrapper MCP: delega a ``toolkit.cli.inspect.readiness_ops.summary``.
    """
    from toolkit.domain.readiness import summary as _cli_summary

    try:
        return _cli_summary(str(_safe_path(str(config_path))), year=year)
    except FileNotFoundError as exc:
        raise ToolkitClientError(str(exc), code=ErrorCode.CONFIG_NOT_FOUND) from exc


def review_readiness(config_path: str, year: int | None = None) -> dict[str, Any]:
    """Check minimale di readiness per review di intake/run candidate.

    Thin wrapper MCP: delega a ``toolkit.cli.inspect.readiness_ops.review_readiness``.
    """
    from toolkit.domain.readiness import review_readiness as _cli_review_readiness

    try:
        return _cli_review_readiness(str(_safe_path(str(config_path))), year=year)
    except FileNotFoundError as exc:
        raise ToolkitClientError(str(exc), code=ErrorCode.CONFIG_NOT_FOUND) from exc


def schema_diff(config_path: str) -> dict[str, Any]:
    """Compare RAW schema signals across the years configured for a dataset.

    Thin wrapper MCP: delega a ``toolkit.cli.inspect.schema_diff_ops.schema_diff_payload``.
    """
    from toolkit.domain.schema_diff import schema_diff_payload as _payload

    try:
        return _payload(config_path)
    except (ValueError, FileNotFoundError) as exc:
        from toolkit.mcp.errors import ToolkitClientError
        from lab_connectors.mcp.errors import ErrorCode

        raise ToolkitClientError(str(exc), code=ErrorCode.INVALID_PARAMS) from exc


def clean_preview(
    config_path: str,
    layer: str = "clean",
    mart_index: int = 0,
    year: int | None = None,
    limit: int = 10,
) -> dict[str, Any]:
    """Preview dati da un parquet clean o mart.

    Thin wrapper MCP: delega a ``toolkit.cli.layer_ops.clean_preview``.
    """
    from toolkit.domain.layer import clean_preview as _cli_clean_preview

    try:
        return _cli_clean_preview(
            str(_safe_path(config_path)),
            layer=layer,
            mart_index=mart_index,
            year=year,
            limit=limit,
        )
    except ValueError as exc:
        raise ToolkitClientError(str(exc), code=ErrorCode.INVALID_PARAMS) from exc
    except FileNotFoundError as exc:
        raise ToolkitClientError(str(exc), code=ErrorCode.PARQUET_NOT_FOUND) from exc


def raw_preview(
    config_path: str,
    year: int | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    """Preview del raw file primario di un dataset.

    Thin wrapper MCP: delega a ``toolkit.cli.layer_ops.raw_preview``.
    """
    from toolkit.domain.layer import raw_preview as _cli_raw_preview

    try:
        return _cli_raw_preview(str(_safe_path(config_path)), year=year, limit=limit)
    except FileNotFoundError as exc:
        raise ToolkitClientError(str(exc), code=ErrorCode.ARTIFACT_NOT_FOUND) from exc


def dataset_info(config_path: str) -> dict[str, Any]:
    """Restituisce informazioni di base da un dataset.yml.

    Legge la configurazione del dataset e ne estrae i campi significativi
    senza eseguire la pipeline.

    Args:
        config_path: path a dataset.yml o slug del dataset.

    Returns:
        Dict con: dataset, years, time_coverage, source_urls (da raw.sources),
        has_clean, has_mart, mart_tables, support_datasets, raw_sources_count.
    """
    config, cfg = _load_cfg(config_path)

    # Estrai URL fonti da raw.sources
    source_urls: list[str] = []
    for src in cfg.raw.sources:
        url = src.args.get("url") or src.args.get("data_url") or src.args.get("endpoint")
        if url:
            source_urls.append(str(url))

    # Mart tables names
    mart_tables: list[str] = []
    for t in cfg.mart.tables:
        mart_tables.append(t.name)

    # Support datasets
    support_list: list[dict[str, str]] = []
    for sd in cfg.support:
        support_list.append({"name": str(sd.name), "config": str(sd.config or "")})

    # Presenza layer su disco
    out_root = cfg.root / "data" if hasattr(cfg, "root") else None
    slug = cfg.dataset if hasattr(cfg, "dataset") else None
    has_clean = bool(out_root and (out_root / "clean" / slug).exists()) if slug else False
    has_mart = bool(out_root and (out_root / "mart" / slug).exists()) if slug else False

    time_cov = None
    if hasattr(cfg, "time_coverage") and cfg.time_coverage:
        time_cov = {
            "start_year": cfg.time_coverage.start_year,
            "end_year": cfg.time_coverage.end_year,
        }

    return {
        "dataset": cfg.dataset if hasattr(cfg, "dataset") else None,
        "config_path": str(config),
        "source_id": cfg.source_id if hasattr(cfg, "source_id") else None,
        "years": list(cfg.years) if hasattr(cfg, "years") else [],
        "time_coverage": time_cov,
        "source_urls": source_urls,
        "raw_sources_count": len(cfg.raw.sources),
        "has_clean": has_clean,
        "has_mart": has_mart,
        "mart_tables": mart_tables,
        "support_datasets": support_list,
        "tags": list(cfg.tags) if hasattr(cfg, "tags") else [],
        "category": cfg.category if hasattr(cfg, "category") else None,
    }


def csv_preview(csv_path: str, limit: int = 20) -> dict[str, Any]:
    """Read a CSV file using the same profiling pipeline as ``profile_raw``.

    Thin wrapper: la logica è in ``toolkit.cli.inspect.profile_ops.csv_preview``.
    MCP aggiunge solo path safety (``_safe_path``) e wrapping errori in
    ``ToolkitClientError``.

    Args:
        csv_path: path to the CSV file (absolute or relative to workspace root)
        limit: max rows to return in preview (default 20)

    Returns:
        dict with keys: path, column_count, columns (name + inferred_type),
        row_count_estimate, preview (list of rows), mapping_suggestions,
        delim_suggested, encoding_suggested, decimal_suggested, skip_suggested,
        robust_read_suggested
    """
    from toolkit.domain.profile import csv_preview as _csv_preview_cli
    from toolkit.mcp.path_safety import _safe_path

    path = _safe_path(csv_path)
    if not path.exists():
        raise ToolkitClientError(f"CSV non trovato: {path}", code=ErrorCode.ARTIFACT_NOT_FOUND)
    try:
        result = _csv_preview_cli(str(path), limit=limit)
        result["note"] = (
            "type inference via DuckDB with explicit sniff parameters; "
            "mapping_suggestions use the same pipeline as profile_raw"
        )
        return result
    except Exception as exc:
        raise ToolkitClientError(
            f"Lettura CSV fallita per {path}: {exc}", code=ErrorCode.ARTIFACT_UNREADABLE
        ) from exc
