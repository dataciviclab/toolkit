"""Lettura leggera di dataset.yml — pattern condivisi tra script DI e toolkit.

Centralizza la logica di lettura YAML che era duplicata in 6 script
di ``dataset-incubator``. Non fa validazione Pydantic piena
(vedi ``toolkit.core.config.load_config`` per quello), solo
estrazione dei campi più usati per diagnostica, cataloghi e CI.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any


def load_dataset_manifest(path: str | Path) -> dict[str, Any]:
    """Legge ``dataset.yml`` e restituisce un dict con i campi più usati.

    Args:
        path: Path al file ``dataset.yml`` o alla directory che lo contiene.

    Returns:
        Dict con: ``slug``, ``name``, ``years``, ``source_id``,
        ``time_coverage``, ``sources``, ``support``, ``extra_ca_cert_urls``.

        Tutti i campi opzionali sono ``None`` o ``[]`` se assenti.
    """
    import yaml

    cfg_path = Path(path)
    if cfg_path.is_dir():
        cfg_path = cfg_path / "dataset.yml"

    if not cfg_path.exists():
        return {"slug": cfg_path.parent.name, "error": f"dataset.yml non trovato in {cfg_path}"}

    try:
        with cfg_path.open(encoding="utf-8") as f:
            cfg: dict[str, Any] = yaml.safe_load(f) or {}
    except Exception as exc:
        return {"slug": cfg_path.parent.name, "error": f"YAML parse error: {exc}"}

    slug = cfg.get("slug") or cfg_path.parent.name
    ds: dict[str, Any] = cfg.get("dataset") or {}

    # Campi base
    has_dataset_section = "dataset" in cfg
    result: dict[str, Any] = {
        "dataset": has_dataset_section,
        "slug": slug,
        "name": ds.get("name"),
        "years": ds.get("years") or [],
        "source_id": ds.get("source_id"),
        "time_coverage": ds.get("time_coverage"),
    }

    # raw.sources
    raw: dict[str, Any] = cfg.get("raw") or {}
    result["sources"] = raw.get("sources") or []

    # extra_ca_cert_urls da raw.sources[].args
    cert_urls: list[str] = []
    for src in result["sources"]:
        args: dict[str, Any] = src.get("args") or {}
        single = args.get("extra_ca_cert_url")
        if single:
            cert_urls.append(single)
        multiple = args.get("extra_ca_cert_urls")
        if multiple:
            cert_urls.extend(multiple)
    result["extra_ca_cert_urls"] = cert_urls

    # support (sia root-level che dentro dataset)
    result["support"] = cfg.get("support") or ds.get("support") or []

    return result


def detect_candidate_layout(path: str | Path) -> str:
    """Rileva il tipo di candidate dataset.

    Scansiona la directory per determinare la struttura:

    - ``single_source``: un solo file ``dataset.yml`` con raw.sources non vuoto
    - ``multi-source``: una directory ``sources/`` con più sotto-directory
    - ``compose``: ``dataset.yml`` senza raw/clean, solo support + mart
    - ``support_dataset``: nella directory ``support_datasets/``
    """
    cfg_path = Path(path)
    if cfg_path.is_dir():
        cfg_path = cfg_path / "dataset.yml"

    if not cfg_path.exists():
        return "unknown"

    import yaml

    with cfg_path.open(encoding="utf-8") as f:
        cfg: dict[str, Any] = yaml.safe_load(f) or {}

    raw: dict[str, Any] = cfg.get("raw") or {}
    has_raw = bool(raw.get("sources"))
    has_mart = _has_mart_sql(cfg_path.parent)
    has_support_entries = bool(cfg.get("support"))

    parent_dir = cfg_path.parent
    sources_subdir = parent_dir / "sources"

    if sources_subdir.is_dir():
        sub_items = [d for d in sources_subdir.iterdir() if d.is_dir()]
        if len(sub_items) > 1:
            return "multi-source"

    has_root_dataset = (cfg_path.parent / "dataset.yml").exists()
    has_sources_dir = (cfg_path.parent / "sources").is_dir()

    if has_root_dataset and has_sources_dir:
        return "ambiguous"

    if has_raw and has_mart:
        return "single-source"

    if has_support_entries and not has_raw:
        return "compose"

    return "single-source"


def has_mart_sql(path: str | Path) -> bool:
    """Verifica se esiste il file ``sql/mart.sql`` nella directory candidate."""
    return _has_mart_sql(Path(path))


def _has_mart_sql(path: Path) -> bool:
    return (path / "sql" / "mart.sql").exists()
