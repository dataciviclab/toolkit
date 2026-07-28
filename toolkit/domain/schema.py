"""Schema di un layer raw/clean/mart — implementazione condivisa.

Sia CLI che MCP la chiamano.
MCP wrappa le eccezioni in ToolkitClientError.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from toolkit.core.config import load_config
from toolkit.domain.inspect_utils import (
    _raw_schema_payload,
    _schema_from_parquet,
)
from toolkit.domain.path_resolver import payload_for_year as _payload_for_year


def show_schema(config_path: str, layer: str = "clean", year: int | None = None) -> dict[str, Any]:
    """Mostra lo schema (colonne + tipi) di raw, clean o mart.

    Args:
        config_path: path al dataset.yml.
        layer: ``"raw"``, ``"clean"`` (default), o ``"mart"``.
        year: anno. Se ``None`` per dataset multi-year usa l'ultimo.

    Returns:
        Dict con schema del layer richiesto.

    Raises:
        ValueError: layer non valido o anno non disponibile.
        FileNotFoundError: file parquet o config non trovati.
    """
    cfg = load_config(config_path, strict_config=False)

    safe_layer = (layer or "clean").strip().lower()
    if safe_layer not in {"raw", "clean", "mart"}:
        raise ValueError(f"layer deve essere uno tra: raw, clean, mart (ricevuto: {layer})")

    if safe_layer == "raw":
        years = list(cfg.years or [])
        entries = [_raw_schema_payload(cfg, yr) for yr in years]
        if year is not None:
            entries = [e for e in entries if e.get("year") == year]
        return {
            "dataset": cfg.dataset,
            "layer": "raw",
            "year": year,
            "entry_count": len(entries),
            "entries": entries,
        }

    _target_year: int = year if year is not None else (max(cfg.years) if cfg.years else 0)
    paths = _payload_for_year(cfg, _target_year)
    if safe_layer == "clean":
        parquet_path_str = paths["paths"]["clean"].get("output")
        if not parquet_path_str:
            raise FileNotFoundError("Nessun output clean configurato")
        parquet_path = Path(parquet_path_str)
        payload = _schema_from_parquet(parquet_path)
    else:
        outputs = paths["paths"]["mart"].get("outputs") or []
        if not outputs:
            raise FileNotFoundError("Nessun output mart risolto dal toolkit")
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
