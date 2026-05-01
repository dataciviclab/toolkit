"""Schema inspection and readiness diagnostics for the MCP toolkit client.

Provides read-only diagnostics on config, layers, and run records:
- show_schema: schema of a raw/clean/mart layer
- raw_profile: content of _profile/raw_profile.json
- run_state: run directory state and latest run record
- summary: layer-level overview with existence checks
- blocker_hints: common mismatches between config and outputs
- review_readiness: readiness check for candidate review
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb

from toolkit.mcp.cli_adapter import _toolkit_json, inspect_paths
from toolkit.mcp.errors import ToolkitClientError
from toolkit.mcp.path_safety import _load_cfg, _safe_path
from toolkit.core.run_records import get_run_dir_dataset, list_runs as _list_runs_records


def _sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _schema_from_parquet(parquet_path: Path) -> dict[str, Any]:
    if not parquet_path.exists():
        raise ToolkitClientError(f"Parquet non trovato: {parquet_path}")
    relation = f"read_parquet('{_sql_literal(str(parquet_path))}')"
    try:
        with duckdb.connect(database=":memory:") as conn:
            conn.execute("PRAGMA disable_progress_bar")
            describe_rows = conn.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
    except Exception as exc:
        raise ToolkitClientError(
            f"Lettura schema parquet fallita per {parquet_path}: {exc}"
        ) from exc

    columns = [{"name": row[0], "type": row[1]} for row in describe_rows]
    return {"path": str(parquet_path), "column_count": len(columns), "columns": columns}


def _read_parquet_row_count(parquet_path: Path) -> int | None:
    """Return row count of a parquet file, or None if unreadable."""
    if not parquet_path.exists():
        return None
    try:
        with duckdb.connect(database=":memory:") as conn:
            conn.execute("PRAGMA disable_progress_bar")
            result = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{_sql_literal(str(parquet_path))}')"
            ).fetchone()
            return int(result[0]) if result else None
    except Exception:
        return None


def _exists(path: str | None) -> bool:
    if not path:
        return False
    return Path(path).exists()


def _read_validation_content(path: str | None) -> dict[str, Any] | None:
    """Read a validation JSON file and return its content, or None if missing."""
    if not path or not _exists(path):
        return None
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return None


def _validation_summary_for_layer(layer_dir: Path, validation_filename: str) -> dict[str, Any] | None:
    """Extract summary from a layer's validation JSON.

    Adds: ok, errors_count, warnings_count, row_count, col_count, raw_row_count, clean_row_count.
    Reads row/col counts from summary.stats (clean) or summary.row_counts (mart).
    Falls back to sections.stats for layers that use that path.
    Returns None if the validation file does not exist.
    """
    validation_path = layer_dir / validation_filename
    content = _read_validation_content(str(validation_path))
    if not content:
        return None

    result = {
        "ok": content.get("ok"),
        "errors_count": len(content.get("errors", [])),
        "warnings_count": len(content.get("warnings", [])),
        "row_count": None,
        "col_count": None,
    }

    # Extract stats from summary (clean layer: summary.stats.clean_rows/clean_cols)
    summary = content.get("summary", {})
    stats = summary.get("stats", {})
    result["row_count"] = stats.get("clean_rows") or stats.get("row_count")
    result["col_count"] = stats.get("clean_cols")

    # Fallback: sections.stats (mart layer uses sections differently)
    sections = content.get("sections", {})
    if result["row_count"] is None and "stats" in sections:
        result["row_count"] = sections["stats"].get("row_count")
        result["col_count"] = sections["stats"].get("col_count")

    # Extract transition metadata (clean validation)
    if "transition" in sections:
        t = sections["transition"]
        if "clean_cols" in t:
            result["col_count"] = t.get("clean_cols")
        if "raw_row_count" in t:
            result["raw_row_count"] = t.get("raw_row_count")
        if "clean_row_count" in t:
            result["clean_row_count"] = t.get("clean_row_count")

    # Extract row_counts from mart summary (mart layer)
    if result["row_count"] is None:
        row_counts = summary.get("row_counts", {})
        if row_counts:
            first_key = next(iter(row_counts), None)
            if first_key:
                result["row_count"] = row_counts[first_key]

    return result


def show_schema(config_path: str, layer: str = "clean", year: int | None = None) -> dict[str, Any]:
    config, _cfg = _load_cfg(config_path)
    safe_layer = (layer or "clean").strip().lower()
    if safe_layer not in {"raw", "clean", "mart"}:
        raise ToolkitClientError("layer deve essere uno tra: raw, clean, mart")

    if safe_layer == "raw":
        try:
            payload = _toolkit_json(["inspect", "schema-diff", "--config", str(config), "--json"])
        except Exception as exc:
            raise ToolkitClientError(f"show_schema(raw) fallito per {config}: {exc}") from exc
        entries = [e for e in payload.get("entries", []) if year is None or e.get("year") == year]
        return {
            "dataset": payload.get("dataset"),
            "layer": "raw",
            "year": year,
            "entry_count": len(entries),
            "entries": entries,
        }

    paths = inspect_paths(str(config), year)
    if safe_layer == "clean":
        parquet_path = Path(paths["paths"]["clean"]["output"])
        payload = _schema_from_parquet(parquet_path)
    else:
        outputs = paths["paths"]["mart"].get("outputs") or []
        if not outputs:
            raise ToolkitClientError("Nessun output mart risolto dal toolkit")
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
    paths = inspect_paths(str(config), year)
    raw_dir = Path(paths["paths"]["raw"]["dir"])
    profile_path = raw_dir / "_profile"
    raw_profile_json = profile_path / "raw_profile.json"
    suggested_read_yml = profile_path / "suggested_read.yml"

    if raw_profile_json.exists():
        try:
            profile = json.loads(raw_profile_json.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ToolkitClientError(
                f"raw_profile.json malformato in {raw_profile_json}: {exc}"
            ) from exc
    elif suggested_read_yml.exists():
        # Fallback: suggested_read.yml contains the same hints in YAML form
        import yaml

        try:
            raw_yaml = yaml.safe_load(suggested_read_yml.read_text(encoding="utf-8"))
        except yaml.YAMLError as exc:
            raise ToolkitClientError(
                f"suggested_read.yml non valido in {suggested_read_yml}: {exc}"
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
            "Nessun file raw_profile.json ne suggested_read.yml."
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
    paths = inspect_paths(str(config), year)
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
        except ValueError:
            raise ToolkitClientError(f"since must be a valid ISO datetime, got: {since}")

    until_dt = None
    if until:
        try:
            raw = until.replace("Z", "+00:00")
            until_dt = datetime.fromisoformat(raw)
            if until_dt.tzinfo is None:
                until_dt = until_dt.replace(tzinfo=timezone.utc)
        except ValueError:
            raise ToolkitClientError(f"until must be a valid ISO datetime, got: {until}")

    valid_statuses = {"SUCCESS", "FAILED", "RUNNING", "DRY_RUN"}
    if status and status not in valid_statuses:
        raise ToolkitClientError(f"status must be one of: {', '.join(sorted(valid_statuses))}")

    limit = limit if limit is not None else 20

    records = _list_runs_records(
        run_dir,
        since=since_dt,
        until=until_dt,
        status=status if status else None,
        limit=limit,
    )

    return {
        "dataset": cfg.dataset,
        "config_path": str(config_path),
        "requested_year": year,
        "cross_year": cross_year,
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
        except ValueError:
            raise ToolkitClientError(f"since must be a valid ISO datetime, got: {since}")

    until_dt: datetime | None = None
    if until:
        try:
            raw = until.replace("Z", "+00:00")
            until_dt = datetime.fromisoformat(raw)
            if until_dt.tzinfo is None:
                until_dt = until_dt.replace(tzinfo=timezone.utc)
        except ValueError:
            raise ToolkitClientError(f"until must be a valid ISO datetime, got: {until}")

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
    avg_duration = round(sum(durations) / len(durations), 1) if durations else None

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
    paths = inspect_paths(str(config), year)
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
                "validation": _validation_summary_for_layer(raw_dir, "_validate/raw_validation.json"),
            },
            "clean": {
                "dir": str(clean_dir),
                "dir_exists": clean_dir.exists(),
                "output": clean_paths.get("output"),
                "output_exists": _exists(clean_paths.get("output")),
                "manifest_exists": _exists(clean_paths.get("manifest")),
                "metadata_exists": _exists(clean_paths.get("metadata")),
                "validation": _validation_summary_for_layer(clean_dir, "_validate/clean_validation.json"),
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
    }


def blocker_hints(config_path: str, year: int | None = None) -> dict[str, Any]:
    """Diagnostic hints that flag common mismatches between declared config and actual outputs."""
    config = _safe_path(config_path)
    s = summary(str(config), year)
    layers = s.get("layers", {})
    raw = layers.get("raw", {})
    clean = layers.get("clean", {})
    mart = layers.get("mart", {})
    run = s.get("run", {})

    latest_run = run.get("latest_run") if isinstance(run, dict) else None
    run_record = None
    if latest_run and latest_run.get("path"):
        latest_path = Path(latest_run["path"])
        if latest_path.exists():
            run_record = json.loads(latest_path.read_text(encoding="utf-8"))

    hints: list[dict[str, str]] = []

    # clean output exists but mart outputs are all missing or empty
    if (
        clean.get("output_exists")
        and mart.get("output_count", 0) > 0
        and mart.get("output_exists_count", 0) == 0
    ):
        hints.append(
            {
                "code": "clean_but_no_mart",
                "severity": "warning",
                "message": "clean output esiste ma nessun mart output e' presente",
            }
        )

    # clean dir missing entirely while mart dir exists
    if not clean.get("dir_exists") and mart.get("dir_exists"):
        hints.append(
            {
                "code": "clean_dir_missing",
                "severity": "blocker",
                "message": "mart dir esiste ma clean dir manca: run order incoerente",
            }
        )

    # latest_run record exists but the actual run file is gone
    latest = run.get("latest_run")
    if latest and latest.get("path") and not _exists(latest.get("path")):
        hints.append(
            {
                "code": "latest_run_record_missing",
                "severity": "warning",
                "message": "latest_run reference presente ma file non trovato",
            }
        )

    # resolved output path declared but file missing
    if raw.get("primary_output_file") and not raw.get("primary_output_exists"):
        hints.append(
            {
                "code": "raw_output_missing",
                "severity": "blocker",
                "message": f"raw primary_output_file '{raw['primary_output_file']}' risolto ma file assente",
            }
        )

    if clean.get("output") and not clean.get("output_exists"):
        hints.append(
            {
                "code": "clean_output_missing",
                "severity": "blocker",
                "message": f"clean output '{clean['output']}' risolto ma file assente",
            }
        )

    # mart with multiple outputs but only partial
    if mart.get("output_count", 0) > 1 and mart.get("missing_outputs"):
        missing = mart["missing_outputs"]
        hints.append(
            {
                "code": "mart_partial_outputs",
                "severity": "warning",
                "message": f"{len(missing)} mart output su {mart['output_count']} mancanti: {', '.join(Path(o).name for o in missing[:3])}",
            }
        )

    # run record references a layer status that contradicts file existence
    if run_record:
        layers_map = run_record.get("layers") or {}
        for layer_name, layer_detail in layers_map.items():
            layer_status = (
                layer_detail.get("status") if isinstance(layer_detail, dict) else layer_detail
            )
            if layer_status == "SUCCESS":
                layer_info = layers.get(layer_name, {})
                if layer_name == "clean" and not layer_info.get("output_exists"):
                    hints.append(
                        {
                            "code": "run_says_clean_success_but_output_missing",
                            "severity": "blocker",
                            "message": "run record dice clean SUCCESS ma output file manca",
                        }
                    )
                elif (
                    layer_name == "mart"
                    and layer_info.get("output_exists_count", 0) == 0
                    and layer_info.get("output_count", 0) > 0
                ):
                    hints.append(
                        {
                            "code": "run_says_mart_success_but_outputs_missing",
                            "severity": "blocker",
                            "message": "run record dice mart SUCCESS ma nessun output file presente",
                        }
                    )

    return {
        "dataset": s.get("dataset"),
        "config_path": str(config_path),
        "year": s.get("year"),
        "hint_count": len(hints),
        "hints": hints,
        "blocker_count": sum(1 for h in hints if h.get("severity") == "blocker"),
        "warning_count": sum(1 for h in hints if h.get("severity") == "warning"),
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

    # --- Run record coherence ---
    rs = run_state(str(config), target_year)
    run_record = rs.get("latest_run_record")
    run_coherent = True
    run_detail: str | None = None
    if run_record:
        layers_map = run_record.get("layers") or {}
        for layer_name, layer_detail in layers_map.items():
            layer_status = (
                layer_detail.get("status") if isinstance(layer_detail, dict) else layer_detail
            )
            if layer_status == "SUCCESS":
                layer_info = s.get("layers", {}).get(layer_name, {})
                if layer_name == "clean" and not layer_info.get("output_exists"):
                    run_coherent = False
                    run_detail = f"run dice {layer_name} SUCCESS ma output manca"
                elif layer_name == "mart":
                    if (
                        layer_info.get("output_exists_count", 0) == 0
                        and layer_info.get("output_count", 0) > 0
                    ):
                        run_coherent = False
                        run_detail = f"run dice {layer_name} SUCCESS ma nessun output presente"
        if run_coherent:
            run_detail = f"run record coerente ({run_record.get('status', 'unknown')})"
    else:
        # Nessun run record: non e' un fallimento di readiness se i file esistono
        run_detail = "nessun run record (ok se output presenti)"

    checks.append(
        {
            "check": "run_record_coherent",
            "ok": run_coherent,
            "detail": run_detail,
        }
    )

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
