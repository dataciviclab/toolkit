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


def _dedup_tags(tags: Iterable[str]) -> list[str]:
    """Dedup dei tag preservando l'ordine (i dataset.yml possono duplicarli)."""
    seen: set[str] = set()
    out: list[str] = []
    for tag in tags:
        if tag and tag not in seen:
            seen.add(tag)
            out.append(tag)
    return out


# ---------------------------------------------------------------------------
# clean_catalog
# ---------------------------------------------------------------------------


# Chiavi valide per una entry dataset (registry snello): il builder le deriva
# da dataset.yml/parquet, le entry editoriali preservate (senza parquet locale)
# vengono filtrate su questi campi — le chiavi morte (run, years,
# registry_source) non devono tornare dall'existing.
_DATASET_ENTRY_FIELDS: tuple[str, ...] = (
    "slug",
    "name",
    "description",
    "source",
    "source_id",
    "period",
    "tags",
    "category",
    "columns",
    "location",
    "stage",
    "mart_refs",
)


def _slim_dataset_entry(entry: dict[str, Any]) -> dict[str, Any]:
    """Filtra una entry dataset (es. editoriale) sulle sole chiavi contratto.

    Ricalcola anche ``role`` delle colonne con ``semantic_type`` → dimension:
    l'existing storico può avere role sbagliati (es. ``year`` → metric) che il
    builder deriva correttamente solo sulle entry con parquet locale.
    """
    slim = {k: v for k, v in entry.items() if k in _DATASET_ENTRY_FIELDS}
    for col in slim.get("columns", []) or []:
        if col.get("semantic_type") and col.get("role") != "dimension":
            col["role"] = "dimension"
    return slim


def build_clean_catalog(
    layout: RepoLayout,
    existing: dict[str, Any] | None = None,
    slug: str | None = None,
    semantic_types_path: Path | None = None,
    path_contract: PathContract | None = None,
) -> tuple[dict[str, Any], dict[str, list[str]]]:
    """Costruisce il clean_catalog per il layout del repo.

    Args:
        layout: Struttura del repo (dataset_dirs, source_repo).
        existing: Catalogo precedente (metadata editoriali preservati:
            name/description/source/stage umani, role/semantic_type colonne).
        slug: Limita a un singolo slug (per test e debug).
        semantic_types_path: Path a semantic_types.yaml (opzionale).
        path_contract: Contratto GCS (default: layout DI, year).

    Returns:
        Tuple (catalog, errors) con errors = {"derive": [...], "validation": [...]}.
        Gli errori derive NON bloccano il catalogo: le entry non derivabili
        sono escluse e segnalate. Gli errori validation indicano un artifact
        non conforme allo schema.
    """
    contract = path_contract or PathContract()
    alias_map = load_semantic_types(semantic_types_path)

    editorial: dict[str, dict[str, Any]] = {}
    if existing:
        for ds in existing.get("datasets", []) or []:
            editorial[ds.get("slug", "")] = ds

    datasets: list[dict[str, Any]] = []
    derive_errors: list[str] = []

    for manifest in iter_manifests(layout):
        if slug and manifest.slug != slug:
            continue

        columns, latest_year = latest_clean_columns(manifest, alias_map)
        if columns is None:
            # Nessun parquet locale: possibile solo se l'entry è editoriale
            # (preservata) — altrimenti errore e skip.
            if manifest.slug in editorial:
                datasets.append(_slim_dataset_entry(editorial[manifest.slug]))
                continue
            derive_errors.append(
                f"{manifest.slug}: nessun parquet clean locale "
                f"per anni {manifest.years} (runnare la pipeline per generarlo)"
            )
            continue
        if latest_year is None:
            derive_errors.append(f"{manifest.slug}: schema letto ma anno non risolvibile")
            continue

        years_present = sorted(y for y in manifest.years if clean_parquet_path(manifest.cfg, y))
        years_eff = years_present or [latest_year]

        entry: dict[str, Any] = {
            "slug": manifest.slug,
            "name": manifest.slug.replace("_", " ").title(),
            "description": manifest.description,
            "source": "",
            "source_id": manifest.source_id,
            "period": manifest.period or {"start": years_eff[0], "end": years_eff[-1]},
            "tags": _dedup_tags(manifest.tags),
            "category": manifest.category,
            "columns": columns,
            "location": contract.clean_location(manifest.slug, years_eff),
            "stage": "incubating",
        }

        # Merge editoriale: i campi umani sovrascrivono. role è SEMPRE
        # derivato (semantic_type/type), mai dall'editoriale: l'existing
        # storico può avere role sbagliati (es. year→metric) che bloccherebbero
        # il fix. description e semantic_type restano editabili a mano.
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
                    if oc.get("semantic_type"):
                        col["semantic_type"] = oc["semantic_type"]

        # Link ai mart (convention {dataset}__{mart})
        if manifest.mart_tables:
            entry["mart_refs"] = [
                f"{manifest.slug}__{t.get('name')}" for t in manifest.mart_tables if t.get("name")
            ]

        datasets.append(entry)

    # Preserva entry editoriali senza parquet locale né derive (es. adottati)
    derived_slugs = {d["slug"] for d in datasets}
    for ds_slug, ds in editorial.items():
        if ds_slug not in derived_slugs:
            datasets.append(_slim_dataset_entry(ds))

    catalog: dict[str, Any] = {
        "schema_version": 1,
        "name": "Lab Clean Registry",
        "description": "Catalogo canonico dei clean parquet pubblici del Lab.",
        "source_repo": layout.source_repo,
        "updated_at": str(datetime.now(UTC).date()),
        "datasets": sorted(datasets, key=lambda d: d["slug"]),
    }

    # Validazione: sul registry completo (registry.schema.json), non per sezione.
    return catalog, {"derive": derive_errors, "validation": []}


# ---------------------------------------------------------------------------
# mart_catalog
# ---------------------------------------------------------------------------


def build_mart_catalog(
    layout: RepoLayout,
    path_contract: PathContract | None = None,
    slug: str | None = None,
) -> tuple[dict[str, Any], dict[str, list[str]]]:
    """Costruisce il mart_catalog per il layout del repo.

    Sorgenti: sezione ``mart:`` del dataset.yml (tables, validate.table_rules)
    + run records (blocco run). Nessuna lettura parquet in v1 (columns
    opzionale, non popolato).
    """
    contract = path_contract or PathContract()
    marts: list[dict[str, Any]] = []
    derive_errors: list[str] = []

    for manifest in iter_manifests(layout):
        if slug and manifest.slug != slug:
            continue
        for table in manifest.mart_tables:
            name = table.get("name")
            if not name:
                continue
            entry: dict[str, Any] = {
                "slug": f"{manifest.slug}__{name}",
                "dataset": manifest.slug,
                "table": name,
            }
            # Tabella con years esplicite = multi-anno: il runner la scrive
            # flat (data/mart/{dataset}/{table}.parquet, no dir anno) → la
            # location pubblicata deve essere flat. Tabella per-anno → year.
            table_years = table.get("years")
            if table_years:
                # Flat: stessa struttura del runner multi-anno.
                entry["location"] = contract.mart_location(manifest.slug, name, year=None)
                marts.append(entry)
                continue
            year = max(manifest.years, default=None)
            if contract.mart_layout == "year" and year is None:
                derive_errors.append(
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
    return catalog, {"derive": derive_errors, "validation": []}


# ---------------------------------------------------------------------------
# pipeline_signals
# ---------------------------------------------------------------------------


def build_signals(
    layout: RepoLayout,
    topic: str = "pipeline_state",
) -> tuple[dict[str, Any], dict[str, list[str]]]:
    """Costruisce pipeline_signals per ACB (repo-signals standard).

    Semantica status: ok = struttura coerente + mart presente; warn = nessun
    mart; error = struttura rotta (clean.sql mancante per dataset non-compose).
    NOTA: status=ok NON significa pubblicato (vedi clean_catalog.stage).

    Il fallback sui run storici (run record locale mancante in CI) è
    centralizzato in ``_merge_existing_runs`` (build_registry).
    """
    signals: list[dict[str, Any]] = []
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
    return payload, {"derive": [], "validation": []}


def _years_label(years: Iterable[int]) -> str:
    years = sorted(years)
    if not years:
        return "anni: ?"
    if len(years) == 1:
        return f"anno {years[0]}"
    return f"anni {years[0]}-{years[-1]}"


# ---------------------------------------------------------------------------
# codelists (nomi delle dimensioni)
# ---------------------------------------------------------------------------


def build_codelists(
    layout: RepoLayout,
    codelists_dir: Path | None = None,
) -> tuple[dict[str, Any], dict[str, list[str]]]:
    """Costruisce la sezione codelists del registry: i NOMI dei codelist.

    Legge ``{repo_root}/codelists/*.csv`` (convenzione eurostat: geo.csv,
    units.csv, freq.csv, ...) e pubblica solo i nomi come lista. Il contenuto
    (le righe) NON è embeddato: è letto dal CSV del repo dal consumer on-demand
    (pattern eurostat-mcp). Embeddare le righe rendeva il registry enorme
    (es. eurostat: codelists = 60% del file) senza che alcun consumer leggesse
    i valori dal registry.

    Returns:
        Tuple (payload, errors) con errors = {"derive": [], "validation": [...]}.
        Se la dir codelists non esiste, ritorna una lista vuota senza errori
        (codelists è opzionale).
    """
    codelists_dir = codelists_dir or (layout.repo_root / "codelists")

    if not codelists_dir.is_dir():
        return (
            {"schema_version": 1, "source_repo": layout.source_repo, "codelists": []},
            {"derive": [], "validation": []},
        )

    names = sorted(p.stem for p in codelists_dir.glob("*.csv"))
    payload: dict[str, Any] = {
        "schema_version": 1,
        "source_repo": layout.source_repo,
        "updated_at": str(datetime.now(UTC).date()),
        "codelists": names,
    }
    return payload, {"derive": [], "validation": []}


# ---------------------------------------------------------------------------
# Convenience: unico entry point
# ---------------------------------------------------------------------------


def build_registry(
    layout: RepoLayout,
    *,
    path_contract: PathContract | None = None,
    existing_catalog: dict[str, Any] | None = None,
    existing_signals: dict[str, Any] | None = None,
    semantic_types_path: Path | None = None,
    slug: str | None = None,
) -> dict[str, Any]:
    """Genera l'artifact registry UNICO per un repo (fusion ADR).

    Il file ``registry.json`` committato sostituisce i 5 artifact separati
    (clean_catalog/mart_catalog/pipeline_signals/codelists/entity_graph):
    le proiezioni sono sezioni dello stesso dict, validato contro
    ``registry.schema.json``.

    Returns:
        Dict ``{"registry": {...}, "errors": {...}}`` con:
        - ``registry``: il payload unico (``datasets``, ``marts``,
          ``signals``, ``codelists``, ``entities``, ``summary``);
        - ``errors``: per sezione ``{"derive": [...], "validation": [...]}``
          + ``registry`` per la validazione dell'intero artifact.
    """
    catalog, cc_errors = build_clean_catalog(
        layout,
        existing=existing_catalog,
        semantic_types_path=semantic_types_path,
        path_contract=path_contract,
        slug=slug,
    )
    marts, mc_errors = build_mart_catalog(layout, path_contract=path_contract, slug=slug)
    signals, ps_errors = build_signals(layout)
    codelists, cl_errors = build_codelists(layout)
    graph = build_entity_graph(catalog, semantic_types_path=semantic_types_path)

    registry: dict[str, Any] = {
        "schema_version": 1,
        "repo": layout.source_repo.split("/")[-1] or "unknown",
        "source_repo": layout.source_repo,
        "updated_at": str(datetime.now(UTC).date()),
        "datasets": catalog.get("datasets", []),
        "marts": marts.get("marts", []),
        "signals": signals.get("signals", []),
        "codelists": codelists.get("codelists", []),
        "entities": {
            "entities": graph.get("entities", {}),
            "bridges": graph.get("bridges", []),
        },
    }

    # Preserva i run storici dei signals: in CI post-merge i dataset non
    # rieseguiti non hanno run record locali, quindi il blocco run sparirebbe
    # a ogni rigenerazione. Un UNICO pass centralizzato (dopo la costruzione
    # delle sezioni) ripopola i run mancanti da existing — niente fallback
    # duplicati nei singoli builder.
    registry = _merge_existing_runs(registry, existing_signals=existing_signals)

    # Ordine chiavi deterministico: le entry derivano da path diversi
    # (builder, existing, run restore) e l'ordine di inserimento del dict
    # cambierebbe a ogni rigenerazione → diff rumorosi (v. PR post-merge
    # #815/#817). Un UNICO pass finalizza l'ordine canonico per sezione.
    registry = _canonicalize_registry(registry)

    registry_errors = validate_artifact(registry, "registry.schema.json")
    errors = {
        "datasets": cc_errors,
        "marts": mc_errors,
        "signals": ps_errors,
        "codelists": cl_errors,
        "entities": {"derive": [], "validation": []},
        "registry": {"derive": [], "validation": registry_errors},
    }
    return {"registry": registry, "errors": errors}


def _merge_existing_runs(
    registry: dict[str, Any],
    *,
    existing_signals: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Ripopola i blocco ``run`` mancanti dei signals dall'existing (CI).

    Quando il run record locale manca (post-merge: i dataset non rieseguiti
    non sono nel runner), i signals derivati non hanno ``run``. L'existing
    (registry.json committato) preserva lo storico.

    Solo i signals hanno il blocco ``run`` nel registry snello (datasets/marts
    non lo portano più: nessun consumer lo legge). Il run locale vince sempre;
    l'existing interviene solo dove manca.
    """
    signals_run: dict[str, dict[str, Any]] = {}
    if existing_signals:
        for s in existing_signals.get("signals", []) or []:
            if s.get("run") and s.get("id"):
                signals_run[s["id"]] = s["run"]

    for s in registry.get("signals", []) or []:
        if "run" not in s and s.get("id") in signals_run:
            s["run"] = signals_run[s["id"]]

    return registry


# Ordine canonico delle chiavi per sezione. Le chiavi non elencate vengono
# accodate in ordine di arrivo (nessuna perdita).
_CANONICAL_ORDER: dict[str, tuple[str, ...]] = {
    "datasets": (
        "slug",
        "name",
        "description",
        "source",
        "source_id",
        "period",
        "tags",
        "category",
        "columns",
        "location",
        "stage",
        "mart_refs",
    ),
    "marts": (
        "slug",
        "dataset",
        "table",
        "location",
    ),
    "signals": (
        "id",
        "source_id",
        "status",
        "label",
        "detail",
        "action",
        "run",
    ),
}


def _canonicalize_entry(entry: dict[str, Any], order: tuple[str, ...]) -> dict[str, Any]:
    """Riordina le chiavi di una entry secondo l'ordine canonico di sezione.

    Chiavi note → ordine canonico; chiavi extra → accodate in ordine di
    arrivo (preservate, niente perdite).
    """
    out: dict[str, Any] = {}
    for key in order:
        if key in entry:
            out[key] = entry[key]
    for key, value in entry.items():
        if key not in out:
            out[key] = value
    return out


def _canonicalize_registry(registry: dict[str, Any]) -> dict[str, Any]:
    """Applica l'ordine chiavi canonico a tutte le entry delle sezioni."""
    for section, order in _CANONICAL_ORDER.items():
        entries = registry.get(section)
        if isinstance(entries, list):
            registry[section] = [_canonicalize_entry(e, order) for e in entries]
    return registry


# Nomi display delle entità nel grafo (default: il nome entity dal vocabolario).
ENTITY_LABELS: dict[str, str] = {
    "Comune": "Comune (ISTAT)",
    "Ente": "Ente pubblico",
    "Provincia": "Provincia",
    "Regione": "Regione",
    "Scuola": "Scuola",
    "Gara": "Gara / Appalto",
    "Progetto": "Progetto",
    "Impresa": "Impresa",
    "Nazione": "Nazione",
    "Tempo": "Tempo / data",
    "Attività economica": "Attività economica",
    "Stazione Appaltante": "Stazione Appaltante",
    "Ente sanitario": "Ente sanitario",
    "Procedimento": "Procedimento giudiziario",
    "Categoria merceologica": "Categoria merceologica",
    "Persona": "Persona",
    "Atto legislativo": "Atto legislativo / Disegno di legge",
    "Gruppo parlamentare": "Gruppo parlamentare",
}


def build_entity_graph(
    catalog: dict[str, Any],
    semantic_types_path: Path | None = None,
) -> dict[str, Any]:
    """Grafo entità → dataset dal clean_catalog (5° artifact registry).

    Nodi = entità del mondo reale (dalla semantic_type di ogni colonna),
    archi = dataset che le descrivono, bridge = relazioni tra entità
    (via ``bridge`` del vocabolario semantic_types). Logica portata da
    ``dataset-incubator/tools/graph/build_graph.py``; consumata da
    lab-dashboard (pagina dataset) e dal MCP clean-query (dataset_graph).
    """
    alias_map = load_semantic_types(semantic_types_path)
    # Struttura completa {stype: {entity, bridge}} dal vocabolario raw
    # (load_semantic_types ritorna solo alias→stype).
    from toolkit.registry.schema_reader import DEFAULT_SEMANTIC_TYPES

    vocab_path = semantic_types_path or DEFAULT_SEMANTIC_TYPES
    types_info: dict[str, dict[str, Any]] = {}
    if vocab_path.is_file():
        import yaml

        data = yaml.safe_load(vocab_path.read_text(encoding="utf-8")) or {}
        for stype, info in (data.get("types") or {}).items():
            if isinstance(info, dict):
                types_info[stype] = info
    if not types_info:
        # Fallback: vocabolario ridotto da alias_map (senza entity/bridge)
        types_info = {stype: {"entity": "", "bridge": None} for stype in alias_map}

    nodes: dict[str, dict[str, Any]] = {}
    relations: list[dict[str, Any]] = []

    for ds in catalog.get("datasets", []) or []:
        slug = ds.get("slug", "")
        ds_name = ds.get("name", slug)
        for col in ds.get("columns", []) or []:
            st = col.get("semantic_type")
            if not st:
                continue
            type_info = types_info.get(st, {})
            entity = type_info.get("entity") or "Sconosciuto"

            if entity not in nodes:
                nodes[entity] = {
                    "entity": entity,
                    "label": ENTITY_LABELS.get(entity, entity),
                    "datasets": [],
                    "types": {},
                }
            ds_entry = {"slug": slug, "name": ds_name, "column": col["name"], "semantic_type": st}
            if not any(
                d["slug"] == slug and d["column"] == col["name"] for d in nodes[entity]["datasets"]
            ):
                nodes[entity]["datasets"].append(ds_entry)
            nodes[entity]["types"][st] = nodes[entity]["types"].get(st, 0) + 1

            bridge = type_info.get("bridge")
            if bridge:
                via = bridge.get("via", "")
                on = bridge.get("on_column", "")
                to_st = bridge.get("to", "")
                to_entity = (types_info.get(to_st) or {}).get("entity", "Sconosciuto")
                if to_entity and to_entity != entity:
                    rel = {
                        "from": {"entity": entity, "dataset": slug, "via": st},
                        "to": {
                            "entity": to_entity,
                            "bridge": via,
                            "on": on,
                            "semantic_type": to_st,
                        },
                    }
                    if rel not in relations:
                        relations.append(rel)

    for entity in nodes:
        nodes[entity]["datasets"].sort(key=lambda d: d["slug"])

    return {
        "schema_version": 1,
        "entities": {e: info for e, info in sorted(nodes.items())},
        "bridges": relations,
    }
