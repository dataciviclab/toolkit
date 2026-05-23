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

from toolkit.mcp._schema_utils import (
    _check_run_record_coherence,
    _exists,
    _read_parquet_row_count,
    _schema_from_parquet,
    _validation_summary_for_layer,
)
from lab_connectors.mcp.errors import ErrorCode

from toolkit.mcp.errors import ToolkitClientError
from toolkit.mcp.path_safety import _load_cfg, _safe_path
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
    config, _cfg = _load_cfg(config_path)
    safe_layer = (layer or "clean").strip().lower()
    if safe_layer not in {"raw", "clean", "mart"}:
        raise ToolkitClientError("layer deve essere uno tra: raw, clean, mart", code=ErrorCode.INVALID_PARAMS)

    if safe_layer == "raw":
        years = list(_cfg.years or [])
        entries = [_raw_schema_payload(_cfg, yr) for yr in years]
        entries_filtered = [e for e in entries if year is None or e.get("year") == year]
        return {
            "dataset": _cfg.dataset,
            "layer": "raw",
            "year": year,
            "entry_count": len(entries_filtered),
            "entries": entries_filtered,
        }

    paths = _inspect_paths(str(config), year)
    if safe_layer == "clean":
        parquet_path = Path(paths["paths"]["clean"]["output"])
        payload = _schema_from_parquet(parquet_path)
    else:
        outputs = paths["paths"]["mart"].get("outputs") or []
        if not outputs:
            raise ToolkitClientError("Nessun output mart risolto dal toolkit", code=ErrorCode.PARQUET_NOT_FOUND)
        parquet_path = Path(outputs[0])
        payload = _schema_from_parquet(parquet_path)
        payload["available_outputs"] = outputs
        if len(outputs) > 1:
            payload["warning"] = (
                "Sono presenti piu' output mart; lo schema mostrato riguarda solo il primo output."
            )

    payload.update(
        {
            "dataset": paths.get("dataset"),
            "year": paths.get("year"),
            "layer": safe_layer,
            "config_path": str(config_path),
        }
    )
    return payload


def raw_profile(config_path: str, year: int | None = None) -> dict[str, Any]:
    """Restituisce il contenuto di _profile/raw_profile.json (o suggested_read.yml come fallback).

    Il profilo contiene encoding, delimitatore, decimal suggestion, nomi colonna,
    sample rows, missingness e mapping suggestions per il layer raw.
    """
    config = _safe_path(config_path)
    paths = _inspect_paths(str(config), year)
    raw_dir = Path(paths["paths"]["raw"]["dir"])
    profile_path = raw_dir / "_profile"
    raw_profile_json = profile_path / "raw_profile.json"
    suggested_read_yml = profile_path / "suggested_read.yml"

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
    config = _safe_path(config_path)
    paths = _inspect_paths(str(config), year)
    run_dir = Path(paths["paths"]["run_dir"])
    run_files = sorted(run_dir.glob("*.json")) if run_dir.exists() else []
    latest_run = paths.get("latest_run")
    latest_payload = None
    if latest_run and latest_run.get("path"):
        latest_path = Path(latest_run["path"])
        if latest_path.exists():
            latest_payload = json.loads(latest_path.read_text(encoding="utf-8"))
    years_seen = (
        sorted({p.parent.name for p in run_dir.parent.glob("*/*.json") if p.parent.name.isdigit()})
        if run_dir.parent.exists()
        else []
    )
    return {
        "dataset": paths.get("dataset"),
        "config_path": str(config_path),
        "requested_year": year,
        "run_dir": str(run_dir),
        "run_dir_exists": run_dir.exists(),
        "run_file_count": len(run_files),
        "years_seen": years_seen,
        "latest_run": latest_run,
        "latest_run_record": latest_payload,
    }


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
        "cross_year": cross_year,  # backward compat
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
    config = _safe_path(config_path)
    paths = _inspect_paths(str(config), year)
    raw_paths = paths["paths"]["raw"]
    clean_paths = paths["paths"]["clean"]
    mart_paths = paths["paths"]["mart"]
    run_dir = Path(paths["paths"]["run_dir"])

    raw_dir = Path(raw_paths["dir"])
    clean_dir = Path(clean_paths["dir"])
    mart_dir = Path(mart_paths["dir"])
    mart_outputs = list(mart_paths.get("outputs") or [])
    missing_mart_outputs = [output for output in mart_outputs if not _exists(output)]

    primary_output_file = (paths.get("raw_hints") or {}).get("primary_output_file")
    primary_output_path = str(raw_dir / primary_output_file) if primary_output_file else None

    latest_run = paths.get("latest_run") or {}
    latest_run_path = latest_run.get("path")

    run_files = sorted(run_dir.glob("*.json")) if run_dir.exists() else []
    run_file_count = paths.get("run_file_count", len(run_files))
    years_seen = paths.get("years_seen", [])

    latest_run_record: dict[str, Any] | None = None
    if latest_run_path and Path(latest_run_path).exists():
        try:
            latest_run_record = json.loads(Path(latest_run_path).read_text(encoding="utf-8"))
        except Exception:
            pass

    warnings: list[str] = []
    if primary_output_file and not _exists(primary_output_path):
        warnings.append("raw_output_missing")
    if not _exists(clean_paths.get("output")):
        warnings.append("clean_output_missing")
    if mart_outputs and missing_mart_outputs:
        warnings.append("mart_outputs_missing")
    if latest_run_path and not _exists(latest_run_path):
        warnings.append("latest_run_record_missing")

    # Estrai layer run status dal run record (se presente)
    layer_run_statuses: dict[str, dict[str, Any]] = {}
    if latest_run_record:
        for layer_name in ("raw", "clean", "mart"):
            layer_info = (latest_run_record.get("layers") or {}).get(layer_name, {})
            layer_val = (latest_run_record.get("validations") or {}).get(layer_name, {})
            layer_run_statuses[layer_name] = {
                "status": layer_info.get("status", "PENDING"),
                "validation_passed": layer_val.get("passed"),
                "validation_errors": layer_val.get("errors_count", 0),
                "validation_warnings": layer_val.get("warnings_count", 0),
            }

    return {
        "dataset": paths.get("dataset"),
        "config_path": str(config_path),
        "year": paths.get("year"),
        "layers": {
            "raw": {
                "dir": str(raw_dir),
                "dir_exists": raw_dir.exists(),
                "manifest_exists": _exists(raw_paths.get("manifest")),
                "metadata_exists": _exists(raw_paths.get("metadata")),
                "primary_output_file": primary_output_file,
                "primary_output_exists": _exists(primary_output_path),
                "suggested_read_exists": (paths.get("raw_hints") or {}).get(
                    "suggested_read_exists"
                ),
                "encoding_suggested": (paths.get("raw_hints") or {}).get("encoding"),
                "delim_suggested": (paths.get("raw_hints") or {}).get("delim"),
                "decimal_suggested": (paths.get("raw_hints") or {}).get("decimal"),
                "skip_suggested": (paths.get("raw_hints") or {}).get("skip"),
                "raw_warnings": (paths.get("raw_hints") or {}).get("warnings", []),
                "validation": _validation_summary_for_layer(raw_dir, "_validate/raw_validation.json"),
                "run_status": layer_run_statuses.get("raw"),
            },
            "clean": {
                "dir": str(clean_dir),
                "dir_exists": clean_dir.exists(),
                "output": clean_paths.get("output"),
                "output_exists": _exists(clean_paths.get("output")),
                "manifest_exists": _exists(clean_paths.get("manifest")),
                "metadata_exists": _exists(clean_paths.get("metadata")),
                "validation": _validation_summary_for_layer(clean_dir, "_validate/clean_validation.json"),
                "run_status": layer_run_statuses.get("clean"),
            },
            "mart": {
                "dir": str(mart_dir),
                "dir_exists": mart_dir.exists(),
                "outputs": mart_outputs,
                "output_count": len(mart_outputs),
                "output_exists_count": len(mart_outputs) - len(missing_mart_outputs),
                "missing_outputs": missing_mart_outputs,
                "manifest_exists": _exists(mart_paths.get("manifest")),
                "metadata_exists": _exists(mart_paths.get("metadata")),
                "validation": _validation_summary_for_layer(mart_dir, "_validate/mart_validation.json"),
                "run_status": layer_run_statuses.get("mart"),
            },
        },
        "run": {
            "run_dir": str(run_dir),
            "run_dir_exists": run_dir.exists(),
            "run_file_count": run_file_count,
            "years_seen": years_seen,
            "latest_run": latest_run or None,
            "latest_run_record": latest_run_record,
        },
        "warnings": warnings,
        "multi_year_hint": paths.get("_year_resolution"),
    }



def review_readiness(config_path: str, year: int | None = None) -> dict[str, Any]:
    """Check minimale di readiness per review di intake/run candidate.

    Risponde a:
    - il candidate e' runnable almeno al minimo?
    - i layer attesi esistono davvero?
    - c'e' almeno un output leggibile?
    - il run record e' coerente con gli output presenti?
    """
    config = _safe_path(config_path)
    _, cfg = _load_cfg(config)

    years = getattr(cfg, "years", []) if hasattr(cfg, "years") else []
    target_year = year or (years[0] if years else None)

    checks: list[dict[str, Any]] = []

    # --- Config check ---
    checks.append(
        {
            "check": "config_valid",
            "ok": True,
            "detail": "config parse ok",
        }
    )

    # --- Raw layer ---
    s = summary(str(config), target_year)
    raw = s.get("layers", {}).get("raw", {})
    raw_primary = raw.get("primary_output_file")
    if raw_primary:
        raw_ok = raw.get("primary_output_exists")
    else:
        # Nessun manifest: fallback su esistenza dir e presenza file
        raw_dir_path = Path(raw.get("dir", ""))
        raw_ok = raw_dir_path.exists() and any(raw_dir_path.iterdir())
    checks.append(
        {
            "check": "raw_output_present",
            "ok": raw_ok,
            "detail": f"primary_output={raw.get('primary_output_file', 'unknown')}"
            if raw_ok
            else "raw output mancante",
        }
    )

    # --- Clean layer ---
    clean = s.get("layers", {}).get("clean", {})
    clean_path_str = clean.get("output")
    clean_path = Path(clean_path_str) if clean_path_str else None
    clean_rows = _read_parquet_row_count(clean_path) if clean_path else None
    clean_ok = clean.get("output_exists") and (clean_rows is not None)
    checks.append(
        {
            "check": "clean_output_readable",
            "ok": clean_ok,
            "detail": f"{clean_rows} rows"
            if clean_rows is not None
            else "clean output mancante o illeggibile",
        }
    )

    # --- Mart layer ---
    mart = s.get("layers", {}).get("mart", {})
    mart_outputs = mart.get("outputs", [])
    mart_checks: list[dict[str, Any]] = []
    for output_name in mart_outputs:
        o_path = Path(output_name)
        rows = _read_parquet_row_count(o_path)
        mart_checks.append(
            {
                "name": o_path.name,
                "exists": o_path.exists(),
                "readable": rows is not None,
                "rows": rows,
            }
        )
    mart_ok = len(mart_outputs) > 0 and all(
        m.get("exists") and m.get("readable") for m in mart_checks
    )
    checks.append(
        {
            "check": "mart_outputs_readable",
            "ok": mart_ok,
            "detail": mart_checks,
        }
    )

    # --- Run record coherence (helper condiviso con _check_run_record_coherence) ---
    rs = run_state(str(config), target_year)
    run_record = rs.get("latest_run_record")
    coherence_hints = _check_run_record_coherence(run_record, s.get("layers", {}))
    run_coherent = len(coherence_hints) == 0
    if run_coherent:
        run_detail = f"run record coerente ({run_record.get('status', 'unknown')})" if run_record else "nessun run record (ok se output presenti)"
    else:
        run_detail = coherence_hints[0].get("message", "incoerenza run record")

    checks.append({
        "check": "run_record_coherent",
        "ok": run_coherent,
        "detail": run_detail,
    })

    ok_count = sum(1 for c in checks if c["ok"])
    fail_count = sum(1 for c in checks if not c["ok"])

    if fail_count == 0:
        readiness = "ready"
    elif ok_count >= len(checks) - 1:
        readiness = "needs-review"
    else:
        readiness = "incomplete"

    return {
        "dataset": s.get("dataset"),
        "config_path": str(config_path),
        "year": s.get("year"),
        "readiness": readiness,
        "check_count": len(checks),
        "ok_count": ok_count,
        "fail_count": fail_count,
        "checks": checks,
    }


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
            "note": "File binario XLSX. Usa toolkit_show_schema(layer='raw') per lo schema delle colonne.",
            "dataset": paths.get("dataset"),
            "year": paths.get("year"),
        }
    else:
        return {
            "path": str(raw_file),
            "format": suffix.lstrip("."),
            "note": f"Formato '{suffix}' non supportato per preview raw. "
                    "Usa toolkit_show_schema(layer='raw') per lo schema.",
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
