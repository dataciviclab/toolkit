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


def validate_config(path: str | Path) -> dict[str, Any]:
    """Validazione leggera di un file ``dataset.yml``.

    Controlli:
    - ``dataset`` section presente
    - ``dataset.name`` presente
    - ``dataset.years`` presente e non vuoto
    - Almeno una ``raw.sources`` o sezione ``support``
    - Opzionale: ``source_id`` se ci sono sources

    Args:
        path: Path al file ``dataset.yml`` o directory.

    Returns:
        Dict con ``ok`` (bool), ``errors`` (list),
        ``warnings`` (list), ``slug`` (str).
    """
    from toolkit.core.dataset_loader import load_dataset_manifest

    manifest = load_dataset_manifest(path)
    if "error" in manifest:
        return {
            "ok": False,
            "errors": [manifest["error"]],
            "warnings": [],
            "slug": manifest.get("slug", "?"),
        }

    errors: list[str] = []
    warnings: list[str] = []

    slug = manifest["slug"]

    if not manifest.get("dataset"):
        errors.append("Sezione 'dataset' mancante")

    name = manifest.get("name")
    if not name:
        errors.append("'dataset.name' mancante o vuoto")

    years = manifest.get("years", [])
    if not years:
        errors.append("'dataset.years' mancante o vuoto")
    elif not all(isinstance(y, int) for y in years):
        errors.append("'dataset.years' deve essere una lista di interi")

    sources = manifest.get("sources", [])
    support = manifest.get("support", [])

    if not sources and not support:
        errors.append("Nessuna 'raw.sources' né sezione 'support' — niente da fetchare")

    if sources and not manifest.get("source_id"):
        warnings.append("'dataset.source_id' non impostato (utile per catalogazione)")

    return {
        "ok": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "slug": slug,
    }


def has_mart_sql(path: str | Path) -> bool:
    """Verifica se esiste SQL mart nella directory candidate.

    Controlla tre pattern (in ordine di specificità):
    - ``sql/mart.sql`` — file esatto
    - ``sql/mart*.sql`` — qualsiasi file che inizia con ``mart`` in ``sql/``
    - ``sql/mart/*.sql`` — qualsiasi file ``.sql`` in ``sql/mart/``

    Corrisponde al contratto di ``dataset-incubator``.
    """
    return _has_mart_sql(Path(path))


def _has_mart_sql(path: Path) -> bool:
    sql_dir = path / "sql"
    if not sql_dir.is_dir():
        return False
    # mart.sql esatto
    if (sql_dir / "mart.sql").exists():
        return True
    # mart*.sql in sql/
    if any(p.name.startswith("mart") and p.suffix == ".sql" for p in sql_dir.iterdir()):
        return True
    # sql/mart/*.sql
    mart_dir = sql_dir / "mart"
    if mart_dir.is_dir() and any(p.suffix == ".sql" for p in mart_dir.iterdir()):
        return True
    return False
