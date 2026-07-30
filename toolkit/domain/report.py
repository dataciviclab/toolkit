"""Report di run aggregato: JSON per anno + markdown multi-anno.

Costruisce un report unico a partire dagli artifact del run
(run record, validazione, readiness, preflight) e lo persiste su disco.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from toolkit.core.io import read_json_or_none, write_json_atomic
from toolkit.core.metadata import read_layer_metadata
from toolkit.core.paths import (
    METADATA,
    RAW_PROFILE,
    RAW_PROFILE_DIR,
    layer_year_dir,
)
from toolkit.core.run_records import get_run_dir, latest_run

_REPORT_DIR = "_reports"
_RUN_REPORT_FILENAME = "run_report.json"


def _get_run_record(root: Path, dataset: str, year: int) -> dict[str, Any] | None:
    """Legge il run record più recente per un dataset/anno."""
    run_dir = get_run_dir(root, dataset, year)
    try:
        return latest_run(run_dir)
    except (FileNotFoundError, OSError):
        return None


def _collect_mart_tables(root: Path, dataset: str, year: int) -> list[dict[str, Any]]:
    """Legge dalla metadata del mart l'elenco tabelle con row count."""
    mart_dir = layer_year_dir(root, "mart", dataset, year)
    meta = read_layer_metadata(mart_dir)
    table_profiles = meta.get("table_profiles") or {}
    if not table_profiles:
        tables = []
        for f in sorted(mart_dir.glob("*.parquet")):
            tables.append({"name": f.stem, "rows": None})
        return tables
    return [
        {"name": name, "rows": profile.get("row_count")} for name, profile in table_profiles.items()
    ]


def _collect_clean_profile(root: Path, dataset: str, year: int) -> dict[str, Any]:
    """Legge profilo clean da metadata.json."""
    clean_dir = layer_year_dir(root, "clean", dataset, year)
    meta = read_layer_metadata(clean_dir)
    output_profile = meta.get("output_profile") or {}
    return {
        "row_count": output_profile.get("row_count"),
        "col_count": len(output_profile.get("columns") or [])
        if output_profile.get("columns")
        else None,
    }


def _collect_raw_profile(root: Path, dataset: str, year: int) -> dict[str, Any]:
    """Legge profilo raw da metadata.json (encoding, delim, primary_output)."""
    raw_dir = layer_year_dir(root, "raw", dataset, year)
    meta = read_layer_metadata(raw_dir)
    hints = meta.get("profile_hints") or {}
    return {
        "encoding": hints.get("encoding_suggested"),
        "delim": hints.get("delim_suggested"),
        "primary_output": meta.get("primary_output_file"),
    }


def _collect_raw_row_count(root: Path, dataset: str, year: int) -> int | None:
    """Legge row count dal raw_profile.json."""
    raw_dir = layer_year_dir(root, "raw", dataset, year)
    profile_path = raw_dir / RAW_PROFILE_DIR / RAW_PROFILE
    if profile_path.exists():
        pf = read_json_or_none(profile_path) or {}
        return pf.get("row_count")
    return None


def _collect_config_hash(root: Path, dataset: str, year: int) -> str | None:
    """Legge config_hash da raw metadata.json (o clean, mart)."""
    for layer in ("raw", "clean", "mart"):
        ld = layer_year_dir(root, layer, dataset, year)
        meta_path = ld / METADATA
        if meta_path.exists():
            meta = read_json_or_none(meta_path) or {}
            ch = meta.get("config_hash")
            if ch:
                return ch
    return None


def _collect_output_bytes(root: Path, layer: str, dataset: str, year: int) -> int | None:
    """Legge il totale bytes dal metadata.json di un layer."""
    ld = layer_year_dir(root, layer, dataset, year)
    meta = read_layer_metadata(ld)
    outputs = meta.get("outputs") or []
    if outputs:
        total = sum(o.get("bytes", 0) for o in outputs if o.get("bytes") is not None)
        return total if total else None
    return None


def _collect_mart_transitions(root: Path, dataset: str, year: int) -> list[dict[str, Any]]:
    """Legge transition_profiles dal mart metadata.json (clean→mart)."""
    mart_dir = layer_year_dir(root, "mart", dataset, year)
    meta = read_layer_metadata(mart_dir)
    return meta.get("transition_profiles") or []


_VALID_FAILED = frozenset({"FAILED", "BLOCKED", "ERROR"})
_VALID_INCOMPLETE = frozenset({"RUNNING", "DRY_RUN"})


def _duration_seconds(start: str | None, end: str | None) -> float | None:
    """Calcola secondi tra due timestamp ISO."""
    if not start or not end:
        return None
    try:
        fmt = "%Y-%m-%dT%H:%M:%S"
        s = datetime.strptime(start[:19], fmt)
        e = datetime.strptime(end[:19], fmt)
        return (e - s).total_seconds()
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Build report
# ---------------------------------------------------------------------------


def build_run_report(
    config_path: str,
    year: int,
    *,
    root: str | Path,
    dataset: str,
    run_ctx: dict[str, Any] | None = None,
    preflight: dict[str, Any] | None = None,
    step_results: dict[str, Any] | None = None,
    run_mode: str = "full",
    support_datasets: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Costruisce un report di run aggregato per un singolo anno.

    Args:
        config_path: path al dataset.yml
        year: anno del run
        root: root directory degli output
        dataset: slug del dataset
        run_ctx: RunContext.to_dict() — dal run corrente
        preflight: dict dal preflight (opzionale)
        step_results: dict dal passo run_full per l'anno (opzionale)
        run_mode: full / smoke / dry-run
        support_datasets: lista support eseguiti (nome, stato)

    Returns:
        Dict con report strutturato.
    """
    root_path = Path(root)

    # --- Run record ---
    record = run_ctx if run_ctx else _get_run_record(root_path, dataset, year)
    run_status = (record or {}).get("status")
    # Se il run record non esiste (es. run_year solleva eccezione prima
    # di scriverlo), usa lo step_results come fallback.
    if run_status is None and step_results:
        if step_results.get("run") == "failed":
            run_status = "FAILED"
        elif step_results.get("run") == "ok":
            run_status = "SUCCESS"
    run_id = (record or {}).get("run_id")
    duration = (record or {}).get("duration_seconds")
    source_urls = (record or {}).get("source_urls") or []

    # Tempi per layer dal run record
    layer_timings: dict[str, float | None] = {}
    if record:
        layers = record.get("layers") or {}
        for name in ("raw", "clean", "mart"):
            info = layers.get(name) or {}
            layer_timings[name] = _duration_seconds(info.get("started_at"), info.get("finished_at"))

    # --- Readiness (da step_results o calcola) ---
    readiness = (step_results or {}).get("readiness")
    if step_results:
        readiness_checks = {
            "total": step_results.get("checks", 0),
            "ok": step_results.get("checks_ok", 0),
            "fail": step_results.get("checks_fail", 0),
        }
    else:
        readiness_checks = {"total": 0, "ok": 0, "fail": 0}

    # --- Preflight ---
    preflight_summary: dict[str, Any] = {
        "config_ok": False,
        "sources_reachable": 0,
        "sources_total": 0,
    }
    if preflight:
        cs = preflight.get("config_check") or {}
        sources = preflight.get("sources") or []
        reachable = sum(1 for s in sources if s.get("reachable"))
        quality_scores = [
            s.get("quality_score") for s in sources if s.get("quality_score") is not None
        ]
        avg_quality = round(sum(quality_scores) / len(quality_scores)) if quality_scores else None
        preflight_summary = {
            "config_ok": cs.get("ok", False),
            "sources_reachable": reachable,
            "sources_total": len(sources),
            "quality_score_avg": avg_quality,
        }

    # --- Config hash (da metadata di qualsiasi layer) ---
    config_hash = _collect_config_hash(root_path, dataset, year)

    # --- Validation per layer (da memoria, non da disco) ---
    step_val = (step_results or {}).get("validations") or {}
    layers_report: dict[str, Any] = {}
    for lname in ("raw", "clean", "mart"):
        val = step_val.get(lname, {})
        warnings = val.get("warnings", [])
        errors = val.get("errors", [])
        file_bytes = _collect_output_bytes(root_path, lname, dataset, year)
        layer_status = ((record or {}).get("layers") or {}).get(lname, {}).get("status")

        # Distingue layer non presente (es. mart-only: raw/clean assenti) da layer fallito
        val_ok = val.get("passed")
        if val_ok is None and not val and layer_status is None:
            val_ok = None  # layer non eseguito (non è un fallimento)

        layer_entry: dict[str, Any] = {
            "status": layer_status,
            "duration_seconds": layer_timings.get(lname),
            "file_size_bytes": file_bytes,
            "validation": {
                "ok": val_ok,
                "quality_score": val.get("quality_score"),
                "errors": len(errors),
                "warnings": len(warnings),
            },
            "warnings": warnings[:5] if warnings else [],
            "errors": errors[:5] if errors else [],
        }

        if lname == "raw":
            raw_p = _collect_raw_profile(root_path, dataset, year)
            layer_entry["encoding"] = raw_p.get("encoding")
            layer_entry["delim"] = raw_p.get("delim")
            layer_entry["primary_output"] = raw_p.get("primary_output")
            raw_rows = _collect_raw_row_count(root_path, dataset, year)
            layer_entry["raw_rows"] = raw_rows

        elif lname == "clean":
            clean_p = _collect_clean_profile(root_path, dataset, year)
            layer_entry["rows"] = clean_p.get("row_count")
            layer_entry["columns"] = clean_p.get("col_count")
            # Transition raw→clean dal readiness step
            rl = (step_results or {}).get("layers") or {}
            cl = rl.get("clean") or {}
            layer_entry["transition"] = cl.get("transition")

        elif lname == "mart":
            tables = _collect_mart_tables(root_path, dataset, year)
            layer_entry["tables"] = tables
            total_rows = sum(t.get("rows") or 0 for t in tables if t.get("rows") is not None)
            layer_entry["total_rows"] = total_rows if total_rows else None
            # Transizioni clean→mart
            transitions = _collect_mart_transitions(root_path, dataset, year)
            if transitions:
                layer_entry["transitions"] = [
                    {
                        "target": t.get("target_name"),
                        "source_rows": t.get("source_row_count"),
                        "target_rows": t.get("target_row_count"),
                        "delta": t.get("row_count_delta"),
                        "added_columns": t.get("added_columns"),
                        "removed_columns": t.get("removed_columns"),
                    }
                    for t in transitions
                ]

        layers_report[lname] = layer_entry

    return {
        "dataset": dataset,
        "config_path": str(config_path),
        "year": year,
        "run_id": run_id,
        "run_mode": run_mode,
        "toolkit_version": (record or {}).get("toolkit_version"),
        "status": run_status,
        "duration_seconds": duration,
        "config_hash": config_hash,
        "source_urls": source_urls,
        "readiness": readiness,
        "readiness_checks": readiness_checks,
        "preflight": preflight_summary,
        "layers": layers_report,
        "support_datasets": support_datasets or [],
    }


# ---------------------------------------------------------------------------
# Write report to disk
# ---------------------------------------------------------------------------


def write_run_report(report: dict[str, Any], root: str | Path, dataset: str, year: int) -> Path:
    """Scrive il report JSON su disco.

    Path: {root}/data/_reports/{dataset}/{year}_run_report.json
    """
    root_path = Path(root)
    report_dir = root_path / "data" / _REPORT_DIR / dataset
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"{year}_{_RUN_REPORT_FILENAME}"
    write_json_atomic(report_path, report)
    return report_path


# ---------------------------------------------------------------------------
# Build e write Markdown aggregato
# ---------------------------------------------------------------------------


def _status_icon(status: str | None) -> str:
    if status == "SUCCESS":
        return "✅"
    if status in ("FAILED",):
        return "🔴"
    if status in ("SUCCESS_WITH_WARNINGS",):
        return "⚠️"
    return "·"


def _readiness_icon(readiness: str | None) -> str:
    if readiness == "ready":
        return "✅"
    if readiness == "needs-review":
        return "🔶"
    if readiness == "incomplete":
        return "🔴"
    return "·"


def _validation_icon(ok: bool | None) -> str:
    if ok is True:
        return "✅"
    if ok is False:
        return "🔴"
    # None = layer non eseguito (es. mart-only)
    return "·"


def _fmt_duration(sec: float | None) -> str:
    if sec is None:
        return "-"
    if sec < 1:
        return f"{sec * 1000:.0f}ms"
    return f"{sec:.1f}s"


def _fmt_bytes(b: int | None) -> str:
    """Formatta byte in KB/MB leggibile."""
    if b is None:
        return "-"
    if b < 1024:
        return f"{b}B"
    if b < 1024 * 1024:
        return f"{b / 1024:.1f}KB"
    return f"{b / (1024 * 1024):.1f}MB"
