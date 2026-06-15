"""Report di run aggregato: JSON per anno + markdown multi-anno.

Costruisce un report unico a partire dagli artifact del run
(run record, validazione, readiness, preflight) e lo persiste su disco.

Implementazione condivisa — non dipende da CLI o MCP.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from toolkit.core.io import read_json_or_none
from toolkit.core.metadata import read_layer_metadata
from toolkit.core.paths import (
    CLEAN_VALIDATION,
    MART_VALIDATION,
    METADATA,
    RAW_PROFILE,
    RAW_PROFILE_DIR,
    RAW_VALIDATION,
    layer_year_dir,
)
from toolkit.core.run_records import get_run_dir, latest_run

_REPORT_DIR = "_reports"
_RUN_REPORT_FILENAME = "run_report.json"
_DATASET_README = "README.md"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _read_validation(root: Path, layer: str, dataset: str, year: int) -> dict[str, Any]:
    """Legge il validation JSON di un layer, restituendo dict vuoto se non presente."""
    val_map = {
        "raw": RAW_VALIDATION,
        "clean": CLEAN_VALIDATION,
        "mart": MART_VALIDATION,
    }
    val_name = val_map.get(layer)
    if not val_name:
        return {}
    val_path = layer_year_dir(root, layer, dataset, year) / val_name
    if val_path.exists():
        return read_json_or_none(val_path) or {}
    val_path2 = layer_year_dir(root, layer, dataset, year) / "_validate" / val_name
    if val_path2.exists():
        return read_json_or_none(val_path2) or {}
    return {}


def _get_warnings(validation: dict[str, Any]) -> list[str]:
    return validation.get("warnings", [])


def _get_errors(validation: dict[str, Any]) -> list[str]:
    return validation.get("errors", [])


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

    # --- Validation per layer ---
    layers_report: dict[str, Any] = {}
    for lname in ("raw", "clean", "mart"):
        val = _read_validation(root_path, lname, dataset, year)
        warnings = _get_warnings(val)
        errors = _get_errors(val)
        file_bytes = _collect_output_bytes(root_path, lname, dataset, year)
        layer_entry: dict[str, Any] = {
            "status": ((record or {}).get("layers") or {}).get(lname, {}).get("status"),
            "duration_seconds": layer_timings.get(lname),
            "file_size_bytes": file_bytes,
            "validation": {
                "ok": val.get("ok", False),
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
    import json

    report_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
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


def build_dataset_readme(
    dataset: str,
    config_path: str,
    reports: list[dict[str, Any]],
    overall_status: str | None = None,
) -> str:
    """Genera un README.md aggregato per tutti gli anni di un dataset.

    Args:
        dataset: slug del dataset
        config_path: path al dataset.yml
        reports: lista di report JSON (uno per anno)
        overall_status: stato complessivo (passed/failed)

    Returns:
        Testo markdown del README.
    """
    status = overall_status or "passed"
    status_icon = "✅" if status == "passed" else "🔴"

    lines = [
        f"# Run Report: `{dataset}`\n",
    ]

    # Metadata
    lines.append("## Dataset\n")
    lines.append(f"- Config: `{config_path}`")
    lines.append(f"- Stato complessivo: {status_icon} **{status}**")
    years_list = sorted({int(r["year"]) for r in reports if r.get("year") is not None})
    lines.append(f"- Anni processati: {', '.join(str(y) for y in years_list)}")
    lines.append(f"- Anni con report: {len(reports)}")
    lines.append("")

    # Tabella riepilogativa per anno
    lines.append("## Riepilogo per anno\n")
    lines.append("| Anno | Status | Readiness | Qualità | Raw | Clean | Mart | Durata |")
    lines.append("|------|--------|-----------|---------|-----|-------|------|--------|")

    for r in sorted(reports, key=lambda x: x.get("year", 0)):
        year = r.get("year", "?")
        status_icon_col = _status_icon(r.get("status"))
        status_name = r.get("status", "?")
        readiness_icon_col = _readiness_icon(r.get("readiness"))
        readiness_name = r.get("readiness", "?")

        # Qualità
        pf = r.get("preflight") or {}
        qs = pf.get("quality_score_avg")
        q_str = f"**{qs}**" if qs else "-"

        layers = r.get("layers") or {}

        # Raw
        r_raw = layers.get("raw") or {}
        raw_info = f"{_validation_icon(r_raw.get('validation', {}).get('ok'))}"
        enc = r_raw.get("encoding") or ""
        w_count = r_raw.get("validation", {}).get("warnings", 0)
        raw_info += f" {enc}" if enc else ""
        raw_info += f" ({w_count}w)" if w_count else ""
        raw_bytes = r_raw.get("file_size_bytes")
        raw_info += f" · {_fmt_bytes(raw_bytes)}" if raw_bytes else ""

        # Clean
        r_clean = layers.get("clean") or {}
        clean_info = f"{_validation_icon(r_clean.get('validation', {}).get('ok'))}"
        rows = r_clean.get("rows")
        rows_str = f"{rows} righe" if rows is not None else ""
        w_count = r_clean.get("validation", {}).get("warnings", 0)
        clean_info += f" {rows_str}" if rows_str else ""
        clean_info += f" ({w_count}w)" if w_count else ""
        clean_bytes = r_clean.get("file_size_bytes")
        clean_info += f" · {_fmt_bytes(clean_bytes)}" if clean_bytes else ""

        # Mart
        r_mart = layers.get("mart") or {}
        mart_info = f"{_validation_icon(r_mart.get('validation', {}).get('ok'))}"
        tables = r_mart.get("tables") or []
        n_tables = len(tables)
        mart_rows = r_mart.get("total_rows")
        mart_str = f"{n_tables} tabelle"
        if mart_rows is not None:
            mart_str += f" ({mart_rows} righe)"
        w_count = r_mart.get("validation", {}).get("warnings", 0)
        mart_info += f" {mart_str}"
        mart_info += f" ({w_count}w)" if w_count else ""

        duration_str = _fmt_duration(r.get("duration_seconds"))

        lines.append(
            f"| {year} | {status_icon_col} {status_name} | {readiness_icon_col} {readiness_name} "
            f"| {q_str} | {raw_info} | {clean_info} | {mart_info} | {duration_str} |"
        )

    lines.append("")

    # Preflight
    preflight_info = None
    for r in reports:
        pf = r.get("preflight") or {}
        if pf:
            preflight_info = pf
            break

    if preflight_info:
        lines.append("## Preflight\n")
        lines.append(f"- Config valida: {'✅' if preflight_info.get('config_ok') else '🔴'}")
        lines.append(
            f"- Fonti: {preflight_info.get('sources_reachable', '?')}/"
            f"{preflight_info.get('sources_total', '?')} raggiungibili"
        )
        qs = preflight_info.get("quality_score_avg")
        if qs is not None:
            lines.append(f"- Quality score medio: **{qs}/100**")
        lines.append("")

    # Warning ed errori per anno
    lines.append("## Warning ed errori\n")
    for r in sorted(reports, key=lambda x: x.get("year", 0)):
        year = r.get("year", "?")
        layers = r.get("layers") or {}
        has_issues = False
        for lname in ("raw", "clean", "mart"):
            lr = layers.get(lname) or {}
            ws = lr.get("warnings") or []
            es = lr.get("errors") or []
            if ws or es:
                if not has_issues:
                    lines.append(f"### Anno {year}\n")
                    has_issues = True
                lines.append(f"**{lname}**: {len(es)} errori, {len(ws)} warning")
                for w in ws[:3]:
                    lines.append(f"  - ⚠ {w}")
                for e in es[:3]:
                    lines.append(f"  - ❌ {e}")
        if has_issues:
            lines.append("")

    # Readiness per anno
    has_readiness = any(r.get("readiness") for r in reports)
    if has_readiness:
        lines.append("## Review Readiness\n")
        for r in sorted(reports, key=lambda x: x.get("year", 0)):
            year = r.get("year", "?")
            rd = r.get("readiness", "?")
            rc = r.get("readiness_checks") or {}
            icon = _readiness_icon(rd)
            lines.append(
                f"- Anno {year}: {icon} **{rd}** ({rc.get('ok', 0)}/{rc.get('total', 0)} check ok)"
            )
        lines.append("")

    # Support datasets
    has_support = any(r.get("support_datasets") for r in reports if r.get("support_datasets"))
    if has_support:
        lines.append("## Support Datasets\n")
        seen = set()
        for r in reports:
            for s in r.get("support_datasets") or []:
                name = s.get("name")
                if name and name not in seen:
                    seen.add(name)
                    lines.append(f"- {name}")
        lines.append("")

    lines.append("---")
    lines.append(
        f"*Report generato il {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')} "
        f"dal toolkit*"
    )
    lines.append("")

    return "\n".join(lines)


def write_dataset_readme(
    root: str | Path,
    dataset: str,
    reports: list[dict[str, Any]],
    overall_status: str | None = None,
    config_path: str | None = None,
) -> Path:
    """Scrive il README.md aggregato per un dataset.

    Path: {root}/data/_reports/{dataset}/README.md
    """
    root_path = Path(root)
    report_dir = root_path / "data" / _REPORT_DIR / dataset
    report_dir.mkdir(parents=True, exist_ok=True)
    readme_path = report_dir / _DATASET_README

    # Prendi config_path dal primo report se non fornito
    cfg_path: str = config_path or (str(reports[0].get("config_path", "?")) if reports else "?")
    md = build_dataset_readme(dataset, cfg_path, reports, overall_status)
    readme_path.write_text(md, encoding="utf-8")
    return readme_path
