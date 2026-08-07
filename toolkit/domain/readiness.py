"""Diagnostica: summary layer, run_state, review_readiness.

Implementazione condivisa tra CLI e MCP.
MCP wrappa le eccezioni in ToolkitClientError.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from toolkit.core.io import read_json_or_none

from toolkit.domain.inspect_utils import (
    _check_run_record_coherence,
    _exists,
)
from toolkit.domain.path_resolver import payload_for_year as _payload_for_year
from toolkit.core.config import load_config
from toolkit.core.duckdb_shape import parquet_row_count
from toolkit.core.run_records import get_run_dir


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
            latest_payload = read_json_or_none(latest_path)

    years_seen = (
        sorted({p.parent.name for p in run_dir.parent.glob("*/*.json") if p.parent.name.isdigit()})
        if run_dir.parent.exists()
        else []
    )

    # Ultimo run record assoluto: attraverso TUTTI gli anni con run records
    # (non solo il target year). Usato dai consumer aggregati (registry builder).
    latest_run_record_abs: dict[str, Any] | None = None
    if years_seen:
        abs_year = int(max(years_seen))
        abs_run_dir = get_run_dir(Path(cfg.root), cfg.dataset, abs_year)
        abs_files = sorted(abs_run_dir.glob("*.json")) if abs_run_dir.exists() else []
        if abs_files:
            latest_run_record_abs = read_json_or_none(abs_files[-1])

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
        "latest_run_record_abs": latest_run_record_abs,
    }


# ---------------------------------------------------------------------------
# summary
# ---------------------------------------------------------------------------


def summary(config_path: str | None = None, year: int | None = None) -> dict[str, Any]:
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
        latest_run_record = read_json_or_none(Path(latest_run_path))

    warnings: list[str] = []
    if primary_output_file and not _exists(primary_output_path):
        warnings.append("raw_output_missing")
    if not _exists(clean_paths.get("output")):
        warnings.append("clean_output_missing")
    if mart_outputs and missing_mart_outputs:
        warnings.append("mart_outputs_missing")
    if latest_run_path and not _exists(latest_run_path):
        warnings.append("latest_run_record_missing")

    # Estrai layer run status + validation completa dal run record (se presente)
    layer_run_statuses: dict[str, dict[str, Any]] = {}
    layer_validations: dict[str, dict[str, Any]] = {}
    if latest_run_record:
        for layer_name in ("raw", "clean", "mart"):
            layer_info = (latest_run_record.get("layers") or {}).get(layer_name, {})
            layer_val = (latest_run_record.get("validations") or {}).get(layer_name, {})
            metrics = layer_info.get("metrics") or {}
            # Summary completo (stats+columns+rules) con fallback su stats
            # appiattito per run record scritti prima della migrazione.
            layer_summary = (
                (layer_val.get("summary") or {})
                if isinstance(layer_val.get("summary"), dict)
                else {}
            )
            stats = (
                (layer_summary.get("stats") or layer_val.get("stats") or {})
                if isinstance(layer_summary.get("stats"), dict)
                or isinstance(layer_val.get("stats"), dict)
                else {}
            )
            layer_run_statuses[layer_name] = {
                "status": layer_info.get("status", "PENDING"),
                "validation_passed": layer_val.get("passed"),
                "validation_errors": layer_val.get("errors_count", 0),
                "validation_warnings": layer_val.get("warnings_count", 0),
                "quality_score": layer_val.get("quality_score"),
                "output_rows": metrics.get("output_rows"),
                "col_count": metrics.get("col_count"),
                "output_bytes": metrics.get("output_bytes"),
                "tables_count": metrics.get("tables_count"),
                # Da clean validation stats
                "raw_rows": stats.get("raw_rows"),
                "clean_rows": stats.get("clean_rows"),
                "paqa_score": stats.get("paqa_score"),
                "row_drop_pct": stats.get("row_drop_pct"),
            }
            # Espone la validation completa del run record per i consumer
            # (review_readiness legge columns/rules/row_count da qui).
            if layer_val:
                layer_validations[layer_name] = layer_val

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
                "run_status": layer_run_statuses.get("raw"),
                "validation": layer_validations.get("raw"),
            },
            "clean": {
                "dir": str(clean_dir),
                "dir_exists": clean_dir.exists(),
                "output": clean_paths.get("output"),
                "output_exists": _exists(clean_paths.get("output")),
                "metadata_exists": _exists(clean_paths.get("metadata")),
                "run_status": layer_run_statuses.get("clean"),
                "validation": layer_validations.get("clean"),
            },
            "mart": {
                "dir": str(mart_dir),
                "dir_exists": mart_dir.exists(),
                "outputs": mart_outputs,
                "output_count": len(mart_outputs),
                "output_exists_count": len(mart_outputs) - len(missing_mart_outputs),
                "missing_outputs": missing_mart_outputs,
                "metadata_exists": _exists(mart_paths.get("metadata")),
                "run_status": layer_run_statuses.get("mart"),
                "validation": layer_validations.get("mart"),
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


def review_readiness(config_path: str | None = None, year: int | None = None) -> dict[str, Any]:
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

    # ── Helper: legge campi dal blocco summary con fallback su top-level ──
    def _val_field(validation: dict[str, Any], name: str) -> Any:
        """Campo da validation.summary (nuovo) o validation top-level (legacy)."""
        summary_block = validation.get("summary") or {}
        if isinstance(summary_block, dict) and name in summary_block:
            return summary_block.get(name)
        return validation.get(name)

    # --- Config check ---
    checks.append(
        {
            "check": "config_valid",
            "ok": True,
            "detail": "config parse ok",
        }
    )

    # --- Raw layer ---
    s = summary(str(config_path), target_year)
    raw = s.get("layers", {}).get("raw", {})
    raw_primary = raw.get("primary_output_file")
    if raw_primary:
        raw_ok = raw.get("primary_output_exists")
    else:
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
    clean_val = clean.get("validation") or {}
    clean_rows = _val_field(clean_val, "row_count")
    # Fallback: se validation non disponibile, leggi dal parquet diretto
    if clean_rows is None:
        clean_path_str = clean.get("output")
        clean_path = Path(clean_path_str) if clean_path_str else None
        if clean_path and clean_path.exists():
            clean_rows = parquet_row_count(clean_path)
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
    mart_val = mart.get("validation") or {}
    mart_outputs = mart.get("outputs", [])
    mart_checks: list[dict[str, Any]] = []
    for output_name in mart_outputs:
        o_path = Path(output_name)
        rows = _val_field(mart_val, "row_count") if o_path.exists() else None
        # Fallback: leggi dal parquet se validation non disponibile
        if rows is None and o_path.exists():
            rows = parquet_row_count(o_path)
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

    checks.append(
        {
            "check": "run_record_coherent",
            "ok": run_coherent,
            "detail": run_detail,
        }
    )

    # --- A. Clean column naming (snake_case check) ---
    clean_cols = _val_field(clean_val, "columns") or clean.get("columns")
    if clean_cols:
        _bad_naming: list[str] = []
        for col in clean_cols:
            norm = col.strip().replace(" ", "_").replace("-", "_").lower()
            if col != norm:
                _bad_naming.append(col)
        naming_ok = len(_bad_naming) == 0
        checks.append(
            {
                "check": "clean_columns_naming",
                "ok": naming_ok,
                "detail": "tutte snake_case"
                if naming_ok
                else f"{len(_bad_naming)} colonne non snake_case: {_bad_naming[:5]}",
            }
        )
    else:
        checks.append(
            {"check": "clean_columns_naming", "ok": None, "detail": "colonne clean non disponibili"}
        )

    # --- B. Validation rules coverage ---
    rules_obj = _val_field(clean_val, "rules") or clean.get("rules") or {}
    if clean_cols:
        covered_cols: set[str] = set()
        for rule_name, rule_vals in rules_obj.items():
            if isinstance(rule_vals, list):
                covered_cols.update(rule_vals)
            elif isinstance(rule_vals, dict):
                covered_cols.update(rule_vals.keys())
        coverage_pct = round(len(covered_cols) / len(clean_cols) * 100) if clean_cols else 0
        if coverage_pct >= 80:
            coverage_ok = True
            coverage_detail = f"{coverage_pct}% colonne coperte da regole"
        elif coverage_pct > 0:
            coverage_ok = False
            coverage_detail = (
                f"solo {coverage_pct}% colonne coperte da regole "
                f"({len(covered_cols)}/{len(clean_cols)})"
            )
        else:
            coverage_ok = False
            coverage_detail = "nessuna regola di validazione configurata"
        checks.append(
            {
                "check": "validation_rules_coverage",
                "ok": coverage_ok,
                "detail": coverage_detail,
            }
        )
    else:
        checks.append(
            {
                "check": "validation_rules_coverage",
                "ok": None,
                "detail": "colonne clean non disponibili",
            }
        )

    # --- C. Metadata completeness ---
    has_source_id = bool(cfg.source_id)
    checks.append(
        {
            "check": "metadata_complete",
            "ok": has_source_id,
            "detail": "source_id ✅"
            if has_source_id
            else "source_id ❌ (manca dataset.source_id in dataset.yml)",
        }
    )

    ok_count = sum(1 for c in checks if c["ok"] is True)
    fail_count = sum(1 for c in checks if c["ok"] is False)

    # --- Validation messages from run record ---
    raw_msgs: dict[str, list[str]] = {"errors": [], "warnings": []}
    clean_msgs: dict[str, list[str]] = {"errors": [], "warnings": []}
    mart_msgs: dict[str, list[str]] = {"errors": [], "warnings": []}
    if target_year is not None:
        run_dir = get_run_dir(cfg.root, cfg.dataset, target_year)
        if run_dir.exists():
            try:
                from toolkit.core.run_records import latest_run as _latest_run

                run_record = _latest_run(run_dir)
                for layer_name, msgs in [
                    ("raw", raw_msgs),
                    ("clean", clean_msgs),
                    ("mart", mart_msgs),
                ]:
                    val = (run_record.get("validations") or {}).get(layer_name, {})
                    msgs["errors"] = (val.get("errors") or [])[:3]
                    msgs["warnings"] = (val.get("warnings") or [])[:3]
            except (FileNotFoundError, OSError):
                pass

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
    # (nuovo: summary.stats.* — legacy: campi top-level della validation)
    raw_row_count = _val_field(clean_val, "raw_row_count") or (
        (clean_val.get("summary") or {}).get("stats") or {}
    ).get("raw_rows")
    clean_row_count = _val_field(clean_val, "clean_row_count") or (
        (clean_val.get("summary") or {}).get("stats") or {}
    ).get("clean_rows")
    col_drop = None
    row_drop_pct = None
    if raw_row_count is not None and clean_row_count is not None and raw_row_count > 0:
        row_drop_pct = round((raw_row_count - clean_row_count) / raw_row_count * 100, 1)
    raw_col_count = _val_field(raw_val, "col_count") or (
        (raw_val.get("summary") or {}).get("stats") or {}
    ).get("raw_cols")
    clean_col_count = _val_field(clean_val, "col_count") or (
        (clean_val.get("summary") or {}).get("stats") or {}
    ).get("clean_cols")
    if raw_col_count is not None and clean_col_count is not None:
        col_drop = raw_col_count - clean_col_count

    total_active = ok_count + fail_count
    if fail_count == 0:
        readiness = "ready"
    elif ok_count >= total_active - 1:
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
                "columns": _val_field(clean_val, "columns") or clean.get("columns"),
                "rules": _val_field(clean_val, "rules") or clean.get("rules"),
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
