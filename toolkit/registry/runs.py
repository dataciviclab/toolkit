"""Blocco ``last_run`` del registry.

L'ultima esecuzione è letta da ``toolkit.domain.readiness.run_state`` (che
risolve i path dal config e legge i JSON dei run records) — qui solo la
proiezione compatta verso ``registry.schema.json``.

Contratto v2: solo i tre fatti che i consumer leggono davvero (freschezza,
partizione, esito). ``run_id``, ``quality_score``, ``output_bytes``,
``duration_seconds`` restano nel run record del toolkit (``toolkit inspect
runs``): nel registry occupavano 127 KB su 45 repo senza lettori.

``last_run`` è l'esecuzione più recente su **tutte** le partizioni (non quella
dell'anno più alto: per i dataset previsionali l'anno massimo è nel futuro).
"""

from __future__ import annotations

from typing import Any

from toolkit.domain.readiness import run_state


def latest_run_record(yml_path: str) -> dict[str, Any] | None:
    """Ultimo run record del toolkit per il config (attraverso tutte le partizioni).

    Usa ``run_state`` e il campo ``latest_run_record_abs`` (massimo
    ``started_at`` su tutte le partizioni, esposto dal toolkit).
    """
    try:
        state = run_state(yml_path)
    except Exception:
        return None
    return state.get("latest_run_record_abs")


def last_run(run_record: dict[str, Any] | None) -> dict[str, Any] | None:
    """Proietta un run record nel blocco ``last_run`` del registry.

    Ritorna None se il record non ha i campi minimi (year e status).
    """
    if not run_record:
        return None
    year = run_record.get("year")
    status = run_record.get("status")
    at = run_record.get("started_at") or run_record.get("finished_at")
    if year is None or not status or not at:
        return None
    return {"at": at, "year": year, "status": status}


__all__ = ["last_run", "latest_run_record"]
