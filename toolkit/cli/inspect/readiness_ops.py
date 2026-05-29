"""Diagnostica: summary layer, run_state, review_readiness.

Implementazione condivisa tra CLI e MCP.
MCP wrappa le eccezioni in ToolkitClientError.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from toolkit.cli.inspect._helpers import (
    _check_run_record_coherence,
    _exists,
    _payload_for_year,
    _read_parquet_row_count,
    _read_validation_content,
    _validation_summary_for_layer,
)
from toolkit.core.config import load_config



# ---------------------------------------------------------------------------
# run_state
# ---------------------------------------------------------------------------


def run_state(config_path: str, year: int | None = None) -> dict[str, Any]:
    """Stato della run directory: file presenti, latest run, anni visti.

    Returns:
        Dict con stato della run directory.

    Raises:
        FileNotFoundError: config o directory non trovata.
    """
    cfg = load_config(config_path, strict_config=False)
    _target_year: int = year if year is not None else (max(cfg.years) if cfg.years else 0)
    paths = _payload_for_year(cfg, _target_year)

    run_dir = Path(paths["paths"]["run_dir"])
    run_files = sorted(run_dir.glob("*.json")) if run_dir.exists() else []

    latest_run = paths.get("latest_run")
    latest_payload: dict[str, Any] | None = None
    if latest_run and latest_run.get("path"):
        latest_path = Path(latest_run["path"])
        if latest_path.exists():
            try:
                latest_payload = json.loads(latest_path.read_text(encoding="utf-8"))
            except Exception:
                pass

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


# ---------------------------------------------------------------------------
# summary
# ---------------------------------------------------------------------------


def summary(config_path: str, year: int | None = None) -> dict[str, Any]:
    """Layer-level overview with existence checks.

    Restituisce una panoramica dei layer raw/clean/mart: path, esistenza,
    validation, run status. Legge i dati dal disco, non esegue la pipeline.

    Returns:
        Dict con stato dei layer.

    Raises:
        FileNotFoundError: config non trovata.
    """
    cfg = load_config(config_path, strict_config=False)
    _target_year: int = year if year is not None else (max(cfg.years) if cfg.years else 0)
    paths = _payload_for_year(cfg, _target_year)

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


# ---------------------------------------------------------------------------
# review_readiness
# ---------------------------------------------------------------------------


def review_readiness(config_path: str, year: int | None = None) -> dict[str, Any]:
    """Check minimale di readiness per review di intake/run candidate.

    Verifica:
    - il candidate e' runnabile almeno al minimo?
    - i layer attesi esistono davvero?
    - c'e' almeno un output leggibile?
    - il run record e' coerente con gli output presenti?

    Returns:
        Dict con readiness, checks, ok/fail count.

    Raises:
        FileNotFoundError: config non trovata.
    """
    cfg = load_config(config_path, strict_config=False)

    years = list(cfg.years or [])
    target_year = year or (years[0] if years else None)

    checks: list[dict[str, Any]] = []

    # --- Config check ---
    checks.append({
        "check": "config_valid",
        "ok": True,
        "detail": "config parse ok",
    })

    # --- Raw layer ---
    s = summary(str(config_path), target_year)
    raw = s.get("layers", {}).get("raw", {})
    raw_primary = raw.get("primary_output_file")
    if raw_primary:
        raw_ok = raw.get("primary_output_exists")
    else:
        raw_dir_path = Path(raw.get("dir", ""))
        raw_ok = raw_dir_path.exists() and any(raw_dir_path.iterdir())
    checks.append({
        "check": "raw_output_present",
        "ok": raw_ok,
        "detail": f"primary_output={raw.get('primary_output_file', 'unknown')}"
        if raw_ok
        else "raw output mancante",
    })

    # --- Clean layer ---
    clean = s.get("layers", {}).get("clean", {})
    clean_path_str = clean.get("output")
    clean_path = Path(clean_path_str) if clean_path_str else None
    clean_rows = _read_parquet_row_count(clean_path) if clean_path else None
    clean_ok = clean.get("output_exists") and (clean_rows is not None)
    checks.append({
        "check": "clean_output_readable",
        "ok": clean_ok,
        "detail": f"{clean_rows} rows"
        if clean_rows is not None
        else "clean output mancante o illeggibile",
    })

    # --- Mart layer ---
    mart = s.get("layers", {}).get("mart", {})
    mart_outputs = mart.get("outputs", [])
    mart_checks: list[dict[str, Any]] = []
    for output_name in mart_outputs:
        o_path = Path(output_name)
        rows = _read_parquet_row_count(o_path)
        mart_checks.append({
            "name": o_path.name,
            "exists": o_path.exists(),
            "readable": rows is not None,
            "rows": rows,
        })
    mart_ok = len(mart_outputs) > 0 and all(
        m.get("exists") and m.get("readable") for m in mart_checks
    )
    checks.append({
        "check": "mart_outputs_readable",
        "ok": mart_ok,
        "detail": mart_checks,
    })

    # --- Run record coherence ---
    rs = run_state(str(config_path), target_year)
    run_record = rs.get("latest_run_record")
    coherence_hints = _check_run_record_coherence(run_record, s.get("layers", {}))
    run_coherent = len(coherence_hints) == 0
    if run_coherent:
        run_detail = (
            f"run record coerente ({run_record.get('status', 'unknown')})"
            if run_record
            else "nessun run record (ok se output presenti)"
        )
    else:
        run_detail = coherence_hints[0].get("message", "incoerenza run record")

    checks.append({
        "check": "run_record_coherent",
        "ok": run_coherent,
        "detail": run_detail,
    })

    ok_count = sum(1 for c in checks if c["ok"])
    fail_count = sum(1 for c in checks if not c["ok"])

    # --- Extract validation messages from validation JSON ---
    def _validation_msgs(layer_dir: Path, filename: str, max_items: int = 3) -> dict:
        """Read first N warning/error messages from a validation JSON."""
        fpath = str(layer_dir / filename) if layer_dir.exists() else None
        content = _read_validation_content(fpath)
        msgs: dict[str, list[str]] = {"errors": [], "warnings": []}
        if content:
            msgs["errors"] = content.get("errors", [])[:max_items]
            msgs["warnings"] = content.get("warnings", [])[:max_items]
        return msgs

    # --- Validation messages from disk ---
    raw_dir_path = Path(raw.get("dir", ""))
    clean_dir_path = Path(clean.get("dir", ""))
    mart_dir_path = Path(mart.get("dir", ""))
    raw_msgs = _validation_msgs(raw_dir_path, "_validate/raw_validation.json")
    clean_msgs = _validation_msgs(clean_dir_path, "_validate/clean_validation.json")
    mart_msgs = _validation_msgs(mart_dir_path, "_validate/mart_validation.json")

    # --- Extract rich layer info from summary (already computed) ---
    raw_val = raw.get("validation") or {}
    clean_val = clean.get("validation") or {}
    mart_val = mart.get("validation") or {}
    raw_profile_warnings = raw.get("raw_warnings") or []
    raw_profile_hints = {
        "encoding": raw.get("encoding_suggested"),
        "delim": raw.get("delim_suggested"),
        "decimal": raw.get("decimal_suggested"),
        "skip": raw.get("skip_suggested"),
    }

    # Transition stats from clean validation
    raw_row_count = clean_val.get("raw_row_count")
    clean_row_count = clean_val.get("clean_row_count")
    col_drop = None
    row_drop_pct = None
    if raw_row_count is not None and clean_row_count is not None and raw_row_count > 0:
        row_drop_pct = round((raw_row_count - clean_row_count) / raw_row_count * 100, 1)
    raw_col_count = raw_val.get("col_count")
    clean_col_count = clean_val.get("col_count")
    if raw_col_count is not None and clean_col_count is not None:
        col_drop = raw_col_count - clean_col_count

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
        "layers": {
            "raw": {
                "validation": raw_val,
                "validation_msgs": raw_msgs,
                "profile": raw_profile_hints,
                "profile_warnings": raw_profile_warnings,
                "primary_output": raw.get("primary_output_file"),
            },
            "clean": {
                "validation": clean_val,
                "validation_msgs": clean_msgs,
                "output": clean.get("output"),
                "row_count": clean_rows,
                "transition": {
                    "raw_row_count": raw_row_count,
                    "clean_row_count": clean_row_count,
                    "row_drop_pct": row_drop_pct,
                    "col_drop": col_drop,
                },
            },
            "mart": {
                "validation": mart_val,
                "validation_msgs": mart_msgs,
                "tables": mart_checks,
            },
        },
    }
