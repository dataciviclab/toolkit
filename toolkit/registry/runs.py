"""Blocco ``run`` degli artifact registry.

L'ultimo run record del toolkit è letto da ``toolkit.domain.readiness.run_state``
(che risolve i path dal config e legge i JSON dei run records) — qui solo la
proiezione verso ``run.schema.json``: run_id, year, status, quality_score,
output_rows, output_bytes per layer.

Nessuna lettura file propria: il parsing dei run records è del toolkit.
"""

from __future__ import annotations

from typing import Any

from toolkit.domain.readiness import run_state

LAYERS = ("raw", "clean", "mart")


def latest_run_record(yml_path: str) -> dict[str, Any] | None:
    """Ultimo run record del toolkit per il config.

    Usa ``run_state`` (lettura run records del toolkit). Il target è il max
    dei config years; se quell'anno non ha run, fallback sull'anno più
    recente tra gli anni con run records (``years_seen``).
    """
    try:
        state = run_state(yml_path)
    except Exception:
        return None
    record = state.get("latest_run_record")
    if record:
        return record
    years_seen = state.get("years_seen") or []
    if not years_seen:
        return None
    try:
        state = run_state(yml_path, year=int(max(years_seen)))
    except Exception:
        return None
    return state.get("latest_run_record")


def run_block(run_record: dict[str, Any] | None) -> dict[str, Any] | None:
    """Proietta un run record del toolkit nel blocco ``run`` degli artifact.

    Ritorna None se il record non ha gli identificativi minimi.
    """
    if not run_record:
        return None
    run_id = run_record.get("run_id")
    year = run_record.get("year")
    status = run_record.get("status")
    if not run_id or year is None or not status:
        return None

    layers = run_record.get("layers") or {}
    validations = run_record.get("validations") or {}

    quality_score: dict[str, Any] = {}
    for layer_name in LAYERS:
        qs = (validations.get(layer_name) or {}).get("quality_score")
        if qs is not None:
            quality_score[layer_name] = qs

    output_rows: dict[str, Any] = {}
    output_bytes: dict[str, Any] = {}
    for layer_name in LAYERS:
        metrics = (layers.get(layer_name) or {}).get("metrics", {})
        rows = metrics.get("output_rows")
        if rows is not None:
            output_rows[layer_name] = rows
        bytes_ = metrics.get("output_bytes")
        if bytes_ is not None:
            output_bytes[layer_name] = bytes_

    block: dict[str, Any] = {
        "run_id": run_id,
        "year": year,
        "status": status,
    }
    if quality_score:
        block["quality_score"] = quality_score
    if output_rows:
        block["output_rows"] = output_rows
    if output_bytes:
        block["output_bytes"] = output_bytes
    if run_record.get("started_at"):
        block["started_at"] = run_record["started_at"]
    if run_record.get("finished_at"):
        block["finished_at"] = run_record["finished_at"]
    if run_record.get("duration_seconds") is not None:
        block["duration_seconds"] = run_record["duration_seconds"]
    return block
