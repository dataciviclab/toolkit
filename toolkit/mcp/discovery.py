"""Discovery: elenca e interroga i dataset disponibili nel workspace.

Fornisce funzioni read-only per:
- list_candidates: scandisce dataset-incubator/candidates e support_datasets
- _read_minimal_config: lettura YAML leggera (senza validazione piena)

Dipende da ``WORKSPACE_ROOT`` in path_safety per risolvere il percorso
del workspace DataCivicLab.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Literal

import yaml

from toolkit.mcp.errors import ToolkitClientError
from toolkit.mcp.path_safety import WORKSPACE_ROOT
from lab_connectors.mcp.errors import ErrorCode


def _read_minimal_config(dataset_yml: Path) -> dict[str, Any]:
    """Legge solo i campi essenziali da dataset.yml senza validazione piena.

    Restituisce un dict con: name, years (e raw data per reference).
    """
    try:
        data = yaml.safe_load(dataset_yml.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"_error": f"YAML non valido: {exc}"}

    if not isinstance(data, dict):
        return {"_error": "dataset.yml non è una mappa YAML"}

    ds = data.get("dataset", {}) or {}
    if not isinstance(ds, dict):
        return {"_error": "dataset non è un mapping"}

    name = ds.get("name", dataset_yml.parent.name)
    years = ds.get("years", [])
    if isinstance(years, int):
        years = [years]

    return {
        "name": str(name) if name else None,
        "years": [int(y) for y in years] if isinstance(years, list) else [],
    }


def _find_latest_run_status(slug: str) -> str | None:
    """Cerca l'ultimo run record per un dataset slug e restituisce lo status.

    Scansiona ``{WORKSPACE}/out/data/_runs/{slug}/``.
    """
    runs_base = WORKSPACE_ROOT / "out" / "data" / "_runs" / slug
    if not runs_base.exists():
        return None

    # Cerca l'ultimo run tra tutti gli anni
    latest: dict[str, Any] | None = None
    latest_mtime: float = 0.0

    for year_dir in sorted(runs_base.iterdir()):
        if not year_dir.is_dir() or not year_dir.name.isdigit():
            continue
        for run_file in year_dir.glob("*.json"):
            try:
                mtime = run_file.stat().st_mtime
                if mtime > latest_mtime:
                    data = json.loads(run_file.read_text(encoding="utf-8"))
                    if isinstance(data, dict):
                        latest = data
                        latest_mtime = mtime
            except Exception:
                continue

    if latest:
        return latest.get("status")
    return None


def list_candidates(
    stage: Literal["candidates", "support", "all"] = "all",
    status_filter: str | None = None,
) -> list[dict[str, Any]]:
    """Elenca tutti i dataset disponibili nel workspace.

    Args:
        stage: "candidates" → solo ``candidates/``,
               "support" → solo ``support_datasets/``,
               "all" → entrambi (default).
        status_filter: filtra per ``last_run_status``.
                       Valori: ``"SUCCESS"``, ``"FAILED"``, ``"DRY_RUN"``,
                       ``"RUNNING"``, o ``None`` (nessun filtro, default).

    Returns:
        Lista ordinata per slug, ogni elemento con:
        - slug: nome directory del dataset
        - dataset_name: nome dal dataset.yml (o slug se assente)
        - stage: "candidates" | "support"
        - years: lista anni configurati
        - last_run_status: SUCCESS / FAILED / DRY_RUN / None
        - has_clean: bool (presenza directory out/data/clean/{slug}/)
        - has_mart: bool (presenza directory out/data/mart/{slug}/)
    """
    incubator = WORKSPACE_ROOT / "dataset-incubator"

    dirs_to_scan: list[tuple[str, Path]] = []
    if stage in ("candidates", "all"):
        candidates_dir = incubator / "candidates"
        if candidates_dir.exists():
            dirs_to_scan.append(("candidates", candidates_dir))
    if stage in ("support", "all"):
        support_dir = incubator / "support_datasets"
        if support_dir.exists():
            dirs_to_scan.append(("support", support_dir))

    results: list[dict[str, Any]] = []

    for stage_name, scan_dir in dirs_to_scan:
        # Cerca dataset.yml ricorsivamente (supporta candidates con sub-candidates)
        for dataset_yml in sorted(scan_dir.rglob("dataset.yml")):
            # Salta il template
            if "templates" in dataset_yml.parts:
                continue

            # Usa la directory padre come slug, ma costruisci un
            # parent_slug se il dataset.yml è in una subdirectory
            parent_dir = dataset_yml.parent
            rel_path = parent_dir.relative_to(scan_dir)
            slug = str(rel_path.as_posix())
            # Per path semplici (1 livello) usa solo il nome dir
            if rel_path.parent == Path("."):
                slug = parent_dir.name
            else:
                # Sub-candidate: mantieni path relativo come slug composito
                slug = str(rel_path.as_posix())

            minimal = _read_minimal_config(dataset_yml)
            name = minimal.get("name") or slug
            years = minimal.get("years", [])

            # Presenza layer: per sub-candidates, il dataset name è
            # quello dal dataset.yml (potrebbe differire dallo slug)
            dataset_name_for_path = name if name != slug else parent_dir.name
            out_root = WORKSPACE_ROOT / "out" / "data"
            clean_dir = out_root / "clean" / dataset_name_for_path
            mart_dir = out_root / "mart" / dataset_name_for_path
            has_clean = clean_dir.exists() and any(clean_dir.iterdir())
            has_mart = mart_dir.exists() and any(mart_dir.iterdir())

            last_run_status = _find_latest_run_status(dataset_name_for_path)

            results.append({
                "slug": slug,
                "dataset_name": name,
                "stage": stage_name,
                "years": years,
                "last_run_status": last_run_status,
                "has_clean": has_clean,
                "has_mart": has_mart,
                "config_path": str(dataset_yml),
            })

    if status_filter is not None:
        valid_statuses = {"SUCCESS", "FAILED", "DRY_RUN", "RUNNING"}
        if status_filter not in valid_statuses:
            raise ToolkitClientError(
                f"status_filter deve essere uno tra: {', '.join(sorted(valid_statuses))} (o None)",
                code=ErrorCode.INVALID_PARAMS,
            )
        results = [r for r in results if r["last_run_status"] == status_filter]

    return results
