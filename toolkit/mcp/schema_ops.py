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

import json
from pathlib import Path
from typing import Any

from lab_connectors.mcp.errors import ErrorCode

from toolkit.mcp.errors import ToolkitClientError
from toolkit.mcp.path_safety import _load_cfg, _safe_path
from toolkit.core.paths import RAW_PROFILE, RAW_SUGGESTED_READ
from toolkit.core.run_records import get_run_dir_dataset, list_runs as _list_runs_records


def _inspect_paths(*args: Any, **kwargs: Any) -> Any:
    """Lazy import to avoid circular dependency with cli_adapter."""
    from toolkit.mcp.cli_adapter import inspect_paths as _impl

    return _impl(*args, **kwargs)


def _raw_schema_payload(*args: Any, **kwargs: Any) -> Any:
    """Lazy import to avoid circular dependency with cli/inspect."""
    from toolkit.cli.inspect._helpers import _raw_schema_payload as _impl

    return _impl(*args, **kwargs)


def _compare_schema_entries(*args: Any, **kwargs: Any) -> Any:
    """Lazy import to avoid circular dependency with cli/inspect."""
    from toolkit.cli.inspect._helpers import _compare_schema_entries as _impl

    return _impl(*args, **kwargs)


def show_schema(config_path: str, layer: str = "clean", year: int | None = None) -> dict[str, Any]:
    """Mostra lo schema (colonne + tipi) di raw, clean o mart.

    Thin wrapper MCP: delega a ``toolkit.cli.inspect.schema_ops.show_schema``.
    """
    from toolkit.cli.inspect.schema_ops import show_schema as _cli_show_schema

    try:
        return _cli_show_schema(config_path, layer=layer, year=year)
    except ValueError as exc:
        raise ToolkitClientError(str(exc), code=ErrorCode.INVALID_PARAMS) from exc
    except FileNotFoundError as exc:
        raise ToolkitClientError(str(exc), code=ErrorCode.PARQUET_NOT_FOUND) from exc


def raw_profile(config_path: str, year: int | None = None) -> dict[str, Any]:
    """Restituisce il contenuto di _profile/raw_profile.json (o suggested_read.yml come fallback).

    Il profilo contiene encoding, delimitatore, decimal suggestion, nomi colonna,
    sample rows, missingness e mapping suggestions per il layer raw.
    """
    config = _safe_path(config_path)
    paths = _inspect_paths(str(config), year)
    raw_dir = Path(paths["paths"]["raw"]["dir"])
    profile_path = raw_dir / "_profile"
    raw_profile_json = profile_path / RAW_PROFILE
    suggested_read_yml = profile_path / RAW_SUGGESTED_READ

    if raw_profile_json.exists():
        try:
            profile = json.loads(raw_profile_json.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ToolkitClientError(
                f"raw_profile.json malformato in {raw_profile_json}: {exc}",
                code=ErrorCode.ARTIFACT_UNREADABLE,
            ) from exc
    elif suggested_read_yml.exists():
        # Fallback: suggested_read.yml contains the same hints in YAML form
        import yaml

        try:
            raw_yaml = yaml.safe_load(suggested_read_yml.read_text(encoding="utf-8"))
        except yaml.YAMLError as exc:
            raise ToolkitClientError(
                f"suggested_read.yml non valido in {suggested_read_yml}: {exc}",
                code=ErrorCode.ARTIFACT_UNREADABLE,
            ) from exc
        clean_section = raw_yaml.get("clean", {}) if isinstance(raw_yaml, dict) else {}
        read_section = clean_section.get("read", {}) if isinstance(clean_section, dict) else {}
        profile = {
            "dataset": None,
            "year": None,
            "encoding_suggested": read_section.get("encoding"),
            "delim_suggested": read_section.get("delim"),
            "decimal_suggested": read_section.get("decimal"),
            "skip_suggested": read_section.get("skip"),
            "robust_read_suggested": None,
            "columns_raw": None,
            "columns_norm": None,
            "missingness_top": [],
            "mapping_suggestions": {},
            "warnings": [],
        }
    else:
        raise ToolkitClientError(
            f"Profilo raw non trovato in {profile_path}. "
            "Nessun file raw_profile.json ne suggested_read.yml.",
            code=ErrorCode.ARTIFACT_NOT_FOUND,
        )

    # Ritorna un sottoinsieme leggibile: evita di restituire sample_rows intere
    # se sono troppe (già incluse nel profilo per reference).
    return {
        "dataset": profile.get("dataset"),
        "year": profile.get("year"),
        "config_path": str(config_path),
        "profile_path": str(profile_path),
        "file_used": profile.get("file_used"),
        "read_hints": {
            "encoding": profile.get("encoding_suggested"),
            "delimiter": profile.get("delim_suggested"),
            "decimal": profile.get("decimal_suggested"),
            "skip": profile.get("skip_suggested"),
            "robust": profile.get("robust_read_suggested"),
        },
        "header_line": profile.get("header_line"),
        "columns": {
            "raw": profile.get("columns_raw") or [],
            "normalized": profile.get("columns_norm") or [],
            "count": len(profile.get("columns_raw") or []),
        },
        "missingness_top": profile.get("missingness_top", []),
        "mapping_suggestions": profile.get("mapping_suggestions", {}),
        "warnings": profile.get("warnings", []),
        "profile_exists": True,
    }


def run_state(config_path: str, year: int | None = None) -> dict[str, Any]:
    """Stato della run directory.

    Thin wrapper MCP: delega a ``toolkit.cli.inspect.readiness_ops.run_state``.
    """
    from toolkit.cli.inspect.readiness_ops import run_state as _cli_run_state

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
            raise ToolkitClientError(f"since must be a valid ISO datetime, got: {since}", code=ErrorCode.INVALID_PARAMS) from exc

    until_dt = None
    if until:
        try:
            raw = until.replace("Z", "+00:00")
            until_dt = datetime.fromisoformat(raw)
            if until_dt.tzinfo is None:
                until_dt = until_dt.replace(tzinfo=timezone.utc)
        except ValueError as exc:
            raise ToolkitClientError(f"until must be a valid ISO datetime, got: {until}", code=ErrorCode.INVALID_PARAMS) from exc

    valid_statuses = {"SUCCESS", "FAILED", "RUNNING", "DRY_RUN"}
    if status and status not in valid_statuses:
        raise ToolkitClientError(f"status must be one of: {', '.join(sorted(valid_statuses))}", code=ErrorCode.INVALID_PARAMS)

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
            raise ToolkitClientError(f"since must be a valid ISO datetime, got: {since}", code=ErrorCode.INVALID_PARAMS) from exc

    until_dt: datetime | None = None
    if until:
        try:
            raw = until.replace("Z", "+00:00")
            until_dt = datetime.fromisoformat(raw)
            if until_dt.tzinfo is None:
                until_dt = until_dt.replace(tzinfo=timezone.utc)
        except ValueError as exc:
            raise ToolkitClientError(f"until must be a valid ISO datetime, got: {until}", code=ErrorCode.INVALID_PARAMS) from exc

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
    durations = [r.get("duration_seconds") for r in all_records if r.get("duration_seconds") is not None]
    avg_duration = round(sum(d for d in durations if d is not None) / len(durations), 1) if durations else None

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
    from toolkit.cli.inspect.readiness_ops import summary as _cli_summary

    try:
        return _cli_summary(str(_safe_path(str(config_path))), year=year)
    except FileNotFoundError as exc:
        raise ToolkitClientError(str(exc), code=ErrorCode.CONFIG_NOT_FOUND) from exc



def review_readiness(config_path: str, year: int | None = None) -> dict[str, Any]:
    """Check minimale di readiness per review di intake/run candidate.

    Thin wrapper MCP: delega a ``toolkit.cli.inspect.readiness_ops.review_readiness``.
    """
    from toolkit.cli.inspect.readiness_ops import review_readiness as _cli_review_readiness

    try:
        return _cli_review_readiness(str(_safe_path(str(config_path))), year=year)
    except FileNotFoundError as exc:
        raise ToolkitClientError(str(exc), code=ErrorCode.CONFIG_NOT_FOUND) from exc


def schema_diff(config_path: str) -> dict[str, Any]:
    """Compare RAW schema signals across the years configured for a dataset.

    Returns entries per year (encoding, delim, columns, etc.) and pairwise
    comparisons showing added/removed columns between consecutive years.
    """
    config, cfg = _load_cfg(config_path)

    from toolkit.cli.common import iter_years

    years = iter_years(cfg, None)
    entries = [_raw_schema_payload(cfg, selected_year) for selected_year in years]
    comparisons = _compare_schema_entries(entries)

    return {
        "dataset": cfg.dataset,
        "config_path": str(config_path),
        "years": [entry["year"] for entry in entries],
        "entries": entries,
        "comparisons": comparisons,
    }


def clean_preview(
    config_path: str,
    layer: str = "clean",
    mart_index: int = 0,
    year: int | None = None,
    limit: int = 10,
) -> dict[str, Any]:
    """Preview dati da un parquet clean o mart.

    Args:
        config_path: path a dataset.yml o slug del dataset.
        layer: ``"clean"`` (default) o ``"mart"``.
        mart_index: indice dell'output mart (default 0), usato solo se layer=mart.
        year: anno del dataset. Per dataset multi-year, se omesso usa l'ultimo anno.
        limit: numero massimo di righe in preview (default 10).

    Returns:
        Schema + preview rows del parquet. Stessa struttura di
        ``_read_parquet_preview`` con campi aggiuntivi: dataset, year, layer.
    """
    config = _safe_path(config_path)
    paths = _inspect_paths(str(config), year)

    if layer == "clean":
        parquet_path_str = paths["paths"]["clean"].get("output")
        if not parquet_path_str:
            raise ToolkitClientError("Nessun output clean risolto", code=ErrorCode.PARQUET_NOT_FOUND)
        parquet_path = Path(parquet_path_str)
    elif layer == "mart":
        outputs = paths["paths"]["mart"].get("outputs") or []
        if not outputs:
            raise ToolkitClientError("Nessun output mart risolto", code=ErrorCode.PARQUET_NOT_FOUND)
        if mart_index < 0 or mart_index >= len(outputs):
            code = ErrorCode.INVALID_PARAMS
            raise ToolkitClientError(
                f"Indice mart {mart_index} non valido: {len(outputs)} output disponibili (indice 0-{len(outputs)-1})",
                code=code,
            )
        parquet_path = Path(outputs[mart_index])
    else:
        raise ToolkitClientError("layer deve essere 'clean' o 'mart'", code=ErrorCode.INVALID_PARAMS)

    # Verifica esistenza output
    if not parquet_path.exists():
        raise ToolkitClientError(
            f"Output {layer} non trovato: {parquet_path}",
            code=ErrorCode.PARQUET_NOT_FOUND,
        )

    # Verifica che sia un parquet
    if parquet_path.suffix not in (".parquet",):
        raise ToolkitClientError(
            f"Formato non supportato per preview: {parquet_path.suffix}. "
            "clean_preview supporta solo file .parquet.",
            code=ErrorCode.INVALID_PARAMS,
        )

    from toolkit.mcp._schema_utils import _read_parquet_preview

    result = _read_parquet_preview(parquet_path, limit=limit)
    result.update({
        "dataset": paths.get("dataset"),
        "year": paths.get("year"),
        "layer": layer,
        "config_path": str(config_path),
        "mart_name": parquet_path.stem if layer == "mart" else None,
    })
    return result


def raw_preview(
    config_path: str,
    year: int | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    """Preview del raw file primario di un dataset.

    Wrapper che risolve il raw file dal config_path e chiama ``csv_preview``
    se il file è un CSV. Per file binari (XLSX) restituisce un messaggio informativo.

    Args:
        config_path: path a dataset.yml o slug del dataset.
        year: anno del dataset. Per multi-year, se omesso usa l'ultimo anno.
        limit: righe massime in preview (default 20).

    Returns:
        Dict con path, formato, e preview (se CSV) o messaggio (se binario).
    """
    from toolkit.mcp._schema_utils import _exists

    config = _safe_path(config_path)
    paths = _inspect_paths(str(config), year)
    raw_dir = Path(paths["paths"]["raw"]["dir"])
    primary_file = (paths.get("raw_hints") or {}).get("primary_output_file")
    if not primary_file:
        raise ToolkitClientError(
            "Nessun primary_output_file nel manifest raw",
            code=ErrorCode.ARTIFACT_NOT_FOUND,
        )
    raw_file = raw_dir / primary_file
    if not _exists(str(raw_file)):
        raise ToolkitClientError(
            f"Raw file non trovato: {raw_file}",
            code=ErrorCode.ARTIFACT_NOT_FOUND,
        )

    suffix = raw_file.suffix.lower()
    if suffix in (".csv", ".tsv", ".txt"):
        return csv_preview(str(raw_file), limit=limit)
    elif suffix in (".xlsx", ".xls"):
        return {
            "path": str(raw_file),
            "format": "xlsx",
            "note": "File binario XLSX. Usa toolkit_inspect_schema(layer='raw') per lo schema delle colonne.",
            "dataset": paths.get("dataset"),
            "year": paths.get("year"),
        }
    else:
        return {
            "path": str(raw_file),
            "format": suffix.lstrip("."),
            "note": f"Formato '{suffix}' non supportato per preview raw. "
                    "Usa toolkit_inspect_schema(layer='raw') per lo schema.",
            "dataset": paths.get("dataset"),
            "year": paths.get("year"),
        }


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
    raw_dict = cfg.raw.get("sources") if hasattr(cfg, "raw") else []
    if isinstance(raw_dict, list):
        for src in raw_dict:
            if isinstance(src, dict):
                args = src.get("args") or {}
                url = args.get("url") or args.get("data_url") or args.get("endpoint")
                if url:
                    source_urls.append(str(url))

    # Mart tables names
    mart_tables: list[str] = []
    if hasattr(cfg, "mart"):
        mart_dict = cfg.mart.get("tables") if hasattr(cfg.mart, "get") else []
        if isinstance(mart_dict, list):
            for t in mart_dict:
                if isinstance(t, dict):
                    name = t.get("name")
                    if name:
                        mart_tables.append(str(name))

    # Support datasets
    support_list: list[dict[str, str]] = []
    if hasattr(cfg, "support"):
        raw_support = cfg.support if isinstance(cfg.support, list) else []
        for sd in raw_support:
            if hasattr(sd, "get"):
                sname = sd.get("name")
                if sname:
                    support_list.append({"name": str(sname), "config": str(sd.get("config", ""))})

    # Presenza layer su disco
    out_root = cfg.root / "data" if hasattr(cfg, "root") else None
    slug = cfg.dataset if hasattr(cfg, "dataset") else None
    has_clean = bool(out_root and (out_root / "clean" / slug).exists()) if slug else False
    has_mart = bool(out_root and (out_root / "mart" / slug).exists()) if slug else False

    time_cov = None
    if hasattr(cfg, "time_coverage") and cfg.time_coverage:
        time_cov = {"start_year": cfg.time_coverage.start_year, "end_year": cfg.time_coverage.end_year}

    return {
        "dataset": cfg.dataset if hasattr(cfg, "dataset") else None,
        "config_path": str(config),
        "years": list(cfg.years) if hasattr(cfg, "years") else [],
        "time_coverage": time_cov,
        "source_urls": source_urls,
        "raw_sources_count": len(raw_dict) if isinstance(raw_dict, list) else 0,
        "has_clean": has_clean,
        "has_mart": has_mart,
        "mart_tables": mart_tables,
        "support_datasets": support_list,
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
    from toolkit.cli.inspect.profile_ops import csv_preview as _csv_preview_cli
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
        raise ToolkitClientError(f"Lettura CSV fallita per {path}: {exc}", code=ErrorCode.ARTIFACT_UNREADABLE) from exc
