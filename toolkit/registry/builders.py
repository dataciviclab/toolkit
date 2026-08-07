"""Builder degli artifact registry condivisi.

Tre proiezioni dello stesso stato (dataset.yml + run records + parquet locali):

- ``build_clean_catalog`` — inventario queryabile dei clean parquet.
- ``build_mart_catalog``  — inventario delle tabelle mart (slug {dataset}__{mart}).
- ``build_signals``      — health check operativo (repo-signals standard ACB).

Tutta la lettura del runtime è delegata al toolkit (config model, path resolver,
run_state, parquet_schema) — qui solo proiezione e arricchimento di catalogo.

Derive-mode:
- ``local`` (default): schemi e anni dai parquet locali e dai run records.
  Nessuna lettura GCS.
- ``check-gcs``: come local, più verifica di presenza dei parquet pubblici
  (object_exists) per segnalare gli URL non risolti.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from toolkit.registry.layout import DatasetManifest, RepoLayout, iter_manifests
from toolkit.registry.paths import PathContract
from toolkit.registry.runs import latest_run_record, run_block
from toolkit.registry.schema_reader import (
    clean_parquet_path,
    latest_clean_columns,
    load_semantic_types,
)
from toolkit.registry.validation import validate_artifact


def _section_of(manifest: DatasetManifest, layout: RepoLayout) -> str:
    """Nome della sezione del layout che contiene il manifest (es. 'compose')."""
    try:
        return str(manifest.yml_path.relative_to(layout.repo_root).parts[0])
    except ValueError:
        return ""


def _mart_ok(manifest: DatasetManifest) -> bool:
    """True se il dataset dichiara mart (tables) o ha sql mart presenti."""
    if manifest.mart_tables:
        return True
    sql_dir = manifest.base_dir / "sql"
    if sql_dir.is_dir():
        return any(p.name.startswith("mart") and p.suffix == ".sql" for p in sql_dir.iterdir())
    return False


# ---------------------------------------------------------------------------
# clean_catalog
# ---------------------------------------------------------------------------


def build_clean_catalog(
    layout: RepoLayout,
    existing: dict[str, Any] | None = None,
    derive_mode: str = "local",
    check_gcs: bool = False,
    slug: str | None = None,
    semantic_types_path: Path | None = None,
    path_contract: PathContract | None = None,
) -> tuple[dict[str, Any], list[str]]:
    """Costruisce il clean_catalog per il layout del repo.

    Args:
        layout: Struttura del repo (dataset_dirs, source_repo).
        existing: Catalogo precedente (metadata editoriali preservati:
            name/description/source/stage umani, role/semantic_type colonne).
        derive_mode: ``"local"`` (default) o ``"check-gcs"``.
        check_gcs: alias per derive_mode="check-gcs" (compat coi vecchi flag).
        slug: Limita a un singolo slug (per test e debug).
        semantic_types_path: Path a semantic_types.yaml (opzionale).
        path_contract: Contratto GCS (default: layout DI, year).

    Returns:
        Tuple (catalog, errors). Gli errori NON bloccano il catalogo: le
        entry non derivabili sono escluse e segnalate.
    """
    if derive_mode == "check-gcs" or check_gcs:
        derive_mode = "check-gcs"
    contract = path_contract or PathContract()
    alias_map = load_semantic_types(semantic_types_path)

    editorial: dict[str, dict[str, Any]] = {}
    if existing:
        for ds in existing.get("datasets", []) or []:
            editorial[ds.get("slug", "")] = ds

    datasets: list[dict[str, Any]] = []
    errors: list[str] = []

    for manifest in iter_manifests(layout):
        if slug and manifest.slug != slug:
            continue

        columns, latest_year = latest_clean_columns(manifest, alias_map)
        if columns is None:
            # Nessun parquet locale: possibile solo se l'entry è editoriale
            # (preservata) — altrimenti errore e skip.
            if manifest.slug in editorial:
                datasets.append(editorial[manifest.slug])
                continue
            errors.append(
                f"{manifest.slug}: nessun parquet clean locale "
                f"per anni {manifest.years} (runnare la pipeline per generarlo)"
            )
            continue
        if latest_year is None:
            errors.append(f"{manifest.slug}: schema letto ma anno non risolvibile")
            continue

        years_present = sorted(y for y in manifest.years if clean_parquet_path(manifest.cfg, y))
        years_eff = years_present or [latest_year]

        entry: dict[str, Any] = {
            "slug": manifest.slug,
            "name": manifest.slug.replace("_", " ").title(),
            "description": "",
            "source": "",
            "source_id": manifest.source_id,
            "period": manifest.period or {"start": years_eff[0], "end": years_eff[-1]},
            "years": years_eff,
            "tags": list(manifest.tags),
            "category": manifest.category,
            "columns": columns,
            "location": contract.clean_location(manifest.slug, years_eff),
            "stage": "incubating",
            "registry_source": "derive_auto",
        }

        # Merge editoriale: i campi umani sovrascrivono; role/semantic_type
        # delle colonne preservati se presenti.
        old = editorial.get(manifest.slug)
        if old:
            for field in ("name", "description", "source", "source_id", "stage", "period"):
                if old.get(field):
                    entry[field] = old[field]
            old_cols = {c.get("name"): c for c in old.get("columns", [])}
            for col in entry["columns"]:
                oc = old_cols.get(col["name"])
                if oc:
                    if oc.get("description"):
                        col["description"] = oc["description"]
                    if oc.get("role"):
                        col["role"] = oc["role"]
                    if oc.get("semantic_type"):
                        col["semantic_type"] = oc["semantic_type"]

        # Link ai mart (convention {dataset}__{mart})
        if manifest.mart_tables:
            entry["mart_refs"] = [
                f"{manifest.slug}__{t.get('name')}" for t in manifest.mart_tables if t.get("name")
            ]

        # Blocco run (ultimo run record del toolkit)
        run_rec = latest_run_record(str(manifest.yml_path))
        if run_block(run_rec):
            entry["run"] = run_block(run_rec)

        datasets.append(entry)

    # Preserva entry editoriali senza parquet locale né derive (es. adottati)
    derived_slugs = {d["slug"] for d in datasets}
    for ds_slug, ds in editorial.items():
        if ds_slug not in derived_slugs:
            datasets.append(ds)

    catalog: dict[str, Any] = {
        "schema_version": 1,
        "name": "Lab Clean Registry",
        "description": "Catalogo canonico dei clean parquet pubblici del Lab.",
        "source_repo": layout.source_repo,
        "updated_at": str(datetime.now(UTC).date()),
        "datasets": sorted(datasets, key=lambda d: d["slug"]),
    }

    if derive_mode == "check-gcs":
        errors.extend(_check_gcs_locations(catalog, contract))

    errors.extend(validate_artifact(catalog, "clean_catalog.schema.json"))
    return catalog, errors


def _check_gcs_locations(catalog: dict[str, Any], contract: PathContract) -> list[str]:
    """Verifica che le location gs:// risolvano ad almeno un parquet pubblico."""
    from lab_connectors.gcs import object_exists
    from lab_connectors.gcs.paths import parse_gs_url

    errors: list[str] = []
    for ds in catalog.get("datasets", []):
        loc = ds.get("location") or {}
        path = loc.get("path", "")
        if not path.startswith("gs://"):
            continue
        bucket, key = parse_gs_url(path)
        if "*" in key:
            prefix = key.split("*")[0].rstrip("/") + "/"
            if not object_exists(bucket, prefix + "pipeline_run.json"):
                errors.append(f"{ds['slug']}: nessun pipeline_run.json pubblicato in {prefix}")
        elif not object_exists(bucket, key):
            errors.append(f"{ds['slug']}: parquet non trovato su GCS ({path})")
    return errors


# ---------------------------------------------------------------------------
# mart_catalog
# ---------------------------------------------------------------------------


def build_mart_catalog(
    layout: RepoLayout,
    path_contract: PathContract | None = None,
    slug: str | None = None,
) -> tuple[dict[str, Any], list[str]]:
    """Costruisce il mart_catalog per il layout del repo.

    Sorgenti: sezione ``mart:`` del dataset.yml (tables, validate.table_rules)
    + run records (blocco run). Nessuna lettura parquet in v1 (columns
    opzionale, non popolato).
    """
    contract = path_contract or PathContract()
    marts: list[dict[str, Any]] = []
    errors: list[str] = []
    runs_cache: dict[str, dict[str, Any]] = {}

    for manifest in iter_manifests(layout):
        if slug and manifest.slug != slug:
            continue
        run_rec = latest_run_record(str(manifest.yml_path))
        if manifest.slug not in runs_cache:
            runs_cache[manifest.slug] = run_block(run_rec) or {}
        for table in manifest.mart_tables:
            name = table.get("name")
            if not name:
                continue
            rule = manifest.mart_rules.get(name, {})
            entry: dict[str, Any] = {
                "slug": f"{manifest.slug}__{name}",
                "dataset": manifest.slug,
                "table": name,
                "description": f"{manifest.slug} — mart {name}",
            }
            if rule.get("primary_key"):
                entry["primary_key"] = list(rule["primary_key"])
            if rule.get("required_columns"):
                entry["required_columns"] = list(rule["required_columns"])
            if rule.get("min_rows") is not None:
                entry["min_rows"] = rule["min_rows"]
            if runs_cache[manifest.slug]:
                entry["run"] = runs_cache[manifest.slug]
            year = max(manifest.years, default=None)
            if contract.mart_layout == "year" and year is None:
                errors.append(
                    f"{manifest.slug}__{name}: location mart non risolvibile "
                    "(layout 'year' senza years dichiarati)"
                )
                continue
            entry["location"] = contract.mart_location(manifest.slug, name, year=year)
            marts.append(entry)

    catalog: dict[str, Any] = {
        "schema_version": 1,
        "name": "Lab Mart Registry",
        "description": "Tabelle analitiche (mart) pubblicate dal Lab.",
        "source_repo": layout.source_repo,
        "updated_at": str(datetime.now(UTC).date()),
        "marts": sorted(marts, key=lambda m: m["slug"]),
    }
    errors.extend(validate_artifact(catalog, "mart_catalog.schema.json"))
    return catalog, errors


# ---------------------------------------------------------------------------
# pipeline_signals
# ---------------------------------------------------------------------------


def build_signals(
    layout: RepoLayout,
    topic: str = "pipeline_state",
) -> tuple[dict[str, Any], list[str]]:
    """Costruisce pipeline_signals per ACB (repo-signals standard).

    Semantica status: ok = struttura coerente + mart presente; warn = nessun
    mart; error = struttura rotta (clean.sql mancante per dataset non-compose).
    NOTA: status=ok NON significa pubblicato (vedi clean_catalog.stage).
    """
    signals: list[dict[str, Any]] = []
    errors: list[str] = []
    compose_ids: set[str] = set()

    for manifest in iter_manifests(layout):
        mart_ok = _mart_ok(manifest)
        is_compose = manifest.is_mart_only

        failures: list[str] = []
        if not is_compose and not manifest.has_clean_sql:
            failures.append("missing sql/clean.sql")

        if failures:
            status = "error"
            action = "correggere la struttura del dataset"
        elif not mart_ok:
            status = "warn"
            action = "aggiungere mart SQL per completare il dataset"
        else:
            status = "ok"
            action = ""

        years_label = _years_label(manifest.years)
        mart_label = "mart: si" if mart_ok else "mart: no"
        detail = f"{years_label} — {mart_label}"
        if failures:
            detail += " — " + "; ".join(failures)

        signal: dict[str, Any] = {
            "id": manifest.slug,
            "source_id": manifest.source_id,
            "status": status,
            "label": manifest.slug,
            "detail": detail,
            "action": action,
        }
        if manifest.years:
            signal["years"] = list(manifest.years)
        run_rec = latest_run_record(str(manifest.yml_path))
        if run_block(run_rec):
            signal["run"] = run_block(run_rec)
        signals.append(signal)

        # Prefisso "compose:" per i manifest in sezione compose del layout
        if _section_of(manifest, layout) == "compose":
            compose_ids.add(manifest.slug)

    for sig in signals:
        if sig["id"] in compose_ids:
            sig["id"] = f"compose:{sig['id']}"

    by_status = {"ok": 0, "warn": 0, "error": 0}
    for s in signals:
        by_status[s["status"]] = by_status.get(s["status"], 0) + 1

    payload: dict[str, Any] = {
        "schema_version": "1",
        "generated_at": datetime.now(UTC).date().isoformat(),
        "repo": layout.source_repo.split("/")[-1] or "unknown",
        "topic": topic,
        "summary": {"total": len(signals), "by_status": by_status},
        "signals": signals,
    }
    errors.extend(validate_artifact(payload, "pipeline_signals.schema.json"))
    return payload, errors


def _years_label(years: Iterable[int]) -> str:
    years = sorted(years)
    if not years:
        return "anni: ?"
    if len(years) == 1:
        return f"anno {years[0]}"
    return f"anni {years[0]}-{years[-1]}"


# ---------------------------------------------------------------------------
# Convenience: unico entry point
# ---------------------------------------------------------------------------


def build_registry(
    layout: RepoLayout,
    *,
    derive_mode: str = "local",
    path_contract: PathContract | None = None,
    existing_catalog: dict[str, Any] | None = None,
    semantic_types_path: Path | None = None,
    slug: str | None = None,
) -> dict[str, Any]:
    """Genera i tre artifact per un repo in una sola chiamata.

    Returns:
        Dict ``{"clean_catalog": {...}, "mart_catalog": {...},
        "pipeline_signals": {...}, "errors": {...}}``.
    """
    catalog, cc_errors = build_clean_catalog(
        layout,
        existing=existing_catalog,
        derive_mode=derive_mode,
        semantic_types_path=semantic_types_path,
        path_contract=path_contract,
        slug=slug,
    )
    marts, mc_errors = build_mart_catalog(layout, path_contract=path_contract, slug=slug)
    signals, ps_errors = build_signals(layout)
    return {
        "clean_catalog": catalog,
        "mart_catalog": marts,
        "pipeline_signals": signals,
        "errors": {
            "clean_catalog": cc_errors,
            "mart_catalog": mc_errors,
            "pipeline_signals": ps_errors,
        },
    }
