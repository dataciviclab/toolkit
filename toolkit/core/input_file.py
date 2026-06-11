"""RawInputFile — input file arricchito con metadata della source."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class RawInputFile:
    """Un file raw selezionato per il clean layer, con metadata della source.

    Attributes:
        path: Percorso assoluto al file raw.
        inject_column: Se presente, colonna da iniettare in ogni riga
            letta da questo file prima dell'unione con altre source.
            Formato: {nome_colonna: valore_fisso}.
    """

    path: Path
    inject_column: dict[str, str] | None = None


def build_raw_input_map(
    raw_sources: list[Any] | None,
    year: int,
) -> dict[str, dict[str, str] | None]:
    """Costruisce una mappa filename → inject_column dalle source config.

    Args:
        raw_sources: Lista di RawSourceConfig (o dict compatibili).
        year: Anno per risolvere placeholder {year} nei filename.

    Returns:
        Dict: nome_file → inject_column dict (o None se non configurato).
    """
    if not raw_sources:
        return {}

    source_map: dict[str, dict[str, str] | None] = {}
    for src in raw_sources:
        inject = None
        # Supporta sia oggetti RawSourceConfig che dict
        if isinstance(src, dict):
            src_dict = src
        else:
            src_dict = src.model_dump() if hasattr(src, "model_dump") else {}

        inject_raw = src_dict.get("inject_column") if isinstance(src_dict, dict) else None
        if inject_raw and isinstance(inject_raw, dict):
            # inject_column è già {col_name: col_value, ...}
            inject = {str(k): str(v) for k, v in inject_raw.items()}

        # Calcola il filename atteso (come fa run_raw in _format_args)
        args = src_dict.get("args") if isinstance(src_dict, dict) else {}
        if isinstance(args, dict):
            filename = args.get("filename")
        else:
            filename = None

        if filename:
            filename = str(filename).replace("{year}", str(year))
        else:
            # Se non c'è filename, usa il name della source
            name = src_dict.get("name") if isinstance(src_dict, dict) else None
            if name:
                filename = str(name)
            else:
                continue

        source_map[filename] = inject

    return source_map


def enrich_input_files(
    input_paths: list[Path],
    raw_sources: list[Any] | None,
    year: int,
) -> list[RawInputFile]:
    """Arricchisce una lista di path con i metadata delle source.

    Abbina ogni file alla source corrispondente tramite il filename
    e restituisce oggetti RawInputFile con inject_column se configurato.
    """
    source_map = build_raw_input_map(raw_sources, year)
    result: list[RawInputFile] = []
    for path in input_paths:
        inject = source_map.get(path.name)
        result.append(RawInputFile(path=path, inject_column=inject))
    return result
