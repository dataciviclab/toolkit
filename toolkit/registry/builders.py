"""Builder dell'artifact registry condiviso (schema v2).

Genera l'artifact registry UNICO per repo (fusion ADR): ``registry.json`` con
sezioni ``datasets``, ``marts``, ``checks``, ``codelists``.

Il registry è un catalogo di discovery, non un report di esecuzione:

- ``datasets``: cosa esiste (metadati, colonne, partizioni pubblicate con le
  righe clean, ultima esecuzione) — niente location per voce: il contratto
  GCS del repo (``gcs.prefix``/``gcs.clean``) e ``partitions`` permettono ai
  consumer di derivare i path (``lab_connectors.registry.locations``);
- ``marts``: tabelle analitiche con il *come* sono partizionate (``layout``);
- ``checks``: solo anomalie riproducibili dal config (clean.sql mancante,
  nessun mart) — vuoto quando è tutto a posto;
- ``codelists``: nomi dei codelist (contenuto letto dai CSV del repo).

Il contratto v1 (``location`` per voce, ``signals``, ``entities``, ``stage``)
non è più prodotto. ``schema_version: 2``; il parsing di v1 e v2 è
centralizzato in ``lab_connectors.registry.models``.

Derive-mode: locale. Schemi, partizioni e run records vengono dal workspace,
nessuna lettura GCS.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from lab_connectors.registry.locations import normalize_prefix

from toolkit.registry.layout import DatasetManifest, RepoLayout, iter_manifests
from toolkit.registry.paths import PathContract
from toolkit.registry.runs import last_run, latest_run_record
from toolkit.registry.schema_reader import (
    clean_partition_rows,
    latest_clean_columns,
    load_semantic_types,
    load_valid_types,
    normalize_semantic_type,
)
from toolkit.registry.validation import validate_artifact

log = logging.getLogger(__name__)

SCHEMA_VERSION = 2


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


def _omit_empty(entry: dict[str, Any]) -> dict[str, Any]:
    """Rimuove i campi vuoti (``""``, ``None``, liste/dict vuoti).

    Il contratto v2 non scrive campi vuoti: la stessa entry costava ~209 KB
    di sole ``"description": ""`` su 7.215 colonne. Il parsing tollera
    l'assenza (``.get(..., "")``), quindi il file resta compatibile.

    Si applica anche alle colonne: sono il grosso del file e la descrizione
    è quasi sempre vuota finché non arriva l'arricchimento editoriale.
    """
    out: dict[str, Any] = {}
    for key, value in entry.items():
        if key == "columns" and isinstance(value, list):
            value = [_omit_empty(col) if isinstance(col, dict) else col for col in value]
        if value not in ("", None) and not (isinstance(value, (list, dict)) and not value):
            out[key] = value
    return out


def _partition_map(partitions: Mapping[Any, int] | None) -> dict[int, int]:
    """Normalizza le chiavi di ``partitions`` a interi (anno o legislatura)."""
    if not partitions:
        return {}
    out: dict[int, int] = {}
    for key, value in partitions.items():
        try:
            out[int(key)] = int(value)
        except (TypeError, ValueError):
            continue
    return dict(sorted(out.items()))


# ---------------------------------------------------------------------------
# datasets
# ---------------------------------------------------------------------------


# Chiavi valide per una entry dataset. Il builder le deriva da dataset.yml,
# parquet e run records; le entry editoriali preservate (senza parquet locale)
# vengono filtrate su questi campi — le chiavi v1 morte (signals, entities,
# stage, source) non devono tornare dall'existing.
_DATASET_ENTRY_FIELDS: tuple[str, ...] = (
    "slug",
    "name",
    "description",
    "source_id",
    "tags",
    "category",
    "period",
    "partitions",
    "columns",
    "mart_refs",
    "last_run",
    "location",
)


def _slim_dataset_entry(entry: dict[str, Any]) -> dict[str, Any]:
    """Filtra una entry dataset (es. editoriale v1) sulle chiavi contratto v2.

    Ricalcola anche ``role`` delle colonne con ``semantic_type`` → dimension:
    l'existing storico può avere role sbagliati (es. ``year`` → metric) che il
    builder deriva correttamente solo sulle entry con parquet locale.

    ``location`` resta un override ammesso: durante la transizione v1→v2 le
    entry non rieseguite non hanno ancora ``partitions`` e senza location
    perderebbero il parquet pubblicato.
    """
    slim = {k: v for k, v in entry.items() if k in _DATASET_ENTRY_FIELDS}
    slim["partitions"] = _partition_map(slim.get("partitions"))
    if isinstance(slim.get("location"), dict):
        slim["location"] = slim["location"].get("path") or ""
    for col in slim.get("columns", []) or []:
        if col.get("semantic_type") and col.get("role") != "dimension":
            col["role"] = "dimension"
    return _omit_empty(slim)


def build_clean_catalog(
    layout: RepoLayout,
    existing: dict[str, Any] | None = None,
    slug: str | None = None,
    semantic_types_path: Path | None = None,
    path_contract: PathContract | None = None,
) -> tuple[dict[str, Any], dict[str, list[str]]]:
    """Costruisce la sezione ``datasets`` del registry.

    Args:
        layout: Struttura del repo (dataset_dirs, source_repo).
        existing: Registry precedente (metadata editoriali preservati:
            name/description/source_id, role/semantic_type colonne, partitions
            e last_run delle partizioni non rieseguite).
        slug: Limita a un singolo slug (per test e debug).
        semantic_types_path: Path a semantic_types.yaml (opzionale).
        path_contract: Contratto GCS (accettato per compatibilità di firma:
            i path non sono più scritti nel registry).

    Returns:
        Tuple (catalog, errors) con errors = {"derive": [...], "validation": [...]}.
        Gli errori derive NON bloccano il catalogo: le entry non derivabili
        sono escluse e segnalate.
    """
    del path_contract  # i path si derivano dal contratto gcs a valle
    alias_map = load_semantic_types(semantic_types_path)
    valid_types = load_valid_types(semantic_types_path)

    editorial: dict[str, dict[str, Any]] = {}
    if existing:
        for ds in existing.get("datasets", []) or []:
            editorial[ds.get("slug", "")] = ds

    datasets: list[dict[str, Any]] = []
    derive_errors: list[str] = []
    coverage_gaps: list[str] = []

    for manifest in iter_manifests(layout):
        if slug and manifest.slug != slug:
            continue

        old = editorial.get(manifest.slug)
        columns, _latest_year = latest_clean_columns(manifest, alias_map)
        local_partitions = clean_partition_rows(manifest)
        if manifest.has_clean_sql and not local_partitions:
            # Il run ha prodotto clean per questo dataset? Se no è un run
            # selettivo (o un dataset fallito): `partitions` sarebbe parziale.
            coverage_gaps.append(manifest.slug)
        # Le partizioni non rieseguite (CI: run per dataset) restano dal
        # registry committato; il locale vince sempre sulla stessa chiave.
        partitions = _partition_map(
            {**_partition_map((old or {}).get("partitions")), **local_partitions}
        )

        # Nessun parquet locale (run selettiva, dataset non rieseguito, run
        # fallita): si preserva l'entry committata ANCHE quando ha già
        # `partitions`. Il guard `not partitions` la escludeva, e l'entry
        # veniva ricostruita dal manifest perdendo `columns` — e la `location`
        # non-glob, che il blocco no-narrowing qui sotto non recupera (salva
        # solo le glob). Visto su dataset-incubator#959: `gse_cer_elenco`
        # perdeva 12 colonne perché era l'unico con `partitions` e senza
        # parquet locale (run post-merge selettiva su un altro dataset).
        if columns is None and not manifest.is_mart_only:
            if old is not None:
                datasets.append(_slim_dataset_entry(old))
                continue
            derive_errors.append(
                f"{manifest.slug}: nessun parquet clean locale "
                f"per anni {manifest.years} (runnare la pipeline per generarlo)"
            )
            continue

        entry: dict[str, Any] = {
            "slug": manifest.slug,
            "name": manifest.slug.replace("_", " ").title(),
            "description": manifest.description,
            "source_id": manifest.source_id,
            "tags": _dedup_tags(manifest.tags),
            "category": manifest.category,
        }
        # period: time_coverage dichiarato vince; altrimenti gli estremi delle
        # partizioni pubblicate; altrimenti gli anni dichiarati nel config.
        if manifest.period:
            entry["period"] = manifest.period
        elif partitions:
            years = sorted(partitions)
            entry["period"] = {"start": years[0], "end": years[-1]}
        elif manifest.years:
            entry["period"] = {"start": min(manifest.years), "end": max(manifest.years)}
        if partitions:
            entry["partitions"] = partitions
        # Un dataset mart-only non ha clean: niente columns/partitions. La card
        # resta visibile e i mart sono linkati da mart_refs.
        if columns is not None:
            entry["columns"] = columns

        # Mai restringere la copertura pubblicata: se l'existing dichiarava un
        # glob multi-file e le partizioni note sono una sola (out/ locale
        # parziale, run selettiva), derivare un path puntuale nasconderebbe le
        # altre partizioni pubblicate. Si conserva la location v1 come override.
        old_path = ""
        if old:
            old_loc = old.get("location")
            old_path = old_loc.get("path", "") if isinstance(old_loc, dict) else (old_loc or "")
        if old_path and "*" in old_path and len(partitions) <= 1:
            entry["location"] = old_path

        # Merge editoriale: i campi umani sovrascrivono. role è SEMPRE
        # derivato (semantic_type/type), mai dall'editoriale.
        if old:
            for field in ("name", "description", "source_id"):
                if old.get(field):
                    entry[field] = old[field]
            # period: il time_coverage dichiarato nel config ha priorità
            # sull'editoriale; l'editoriale copre solo l'assenza (senza questa
            # eccezione un cambio di copertura nel config non propagherebbe
            # mai al registry — regression energia-italia #6)
            if not manifest.period and old.get("period"):
                entry["period"] = old["period"]
            old_cols = {c.get("name"): c for c in old.get("columns", [])}
            for col in entry.get("columns", []):
                oc = old_cols.get(col["name"])
                if oc:
                    if oc.get("description"):
                        col["description"] = oc["description"]
                    old_st = oc.get("semantic_type")
                    if old_st:
                        normalized = normalize_semantic_type(old_st, alias_map, valid_types)
                        if normalized:
                            col["semantic_type"] = normalized
                        else:
                            log.warning(
                                "%s.%s: semantic_type '%s' non nel vocabolario — droppato",
                                manifest.slug,
                                col["name"],
                                old_st,
                            )

        # Link ai mart (convention {dataset}__{mart})
        if manifest.mart_tables:
            # Le tabelle `internal: true` sono staging del compose: restano in
            # out/ per le tabelle successive, ma non sono un contratto pubblico
            # e non vanno linkate nel catalogo (nessun oggetto su GCS).
            entry["mart_refs"] = [
                f"{manifest.slug}__{t.get('name')}"
                for t in manifest.mart_tables
                if t.get("name") and not t.get("internal")
            ]

        entry["last_run"] = last_run(latest_run_record(str(manifest.yml_path))) or (
            old.get("last_run") if old else None
        )
        datasets.append(_omit_empty(entry))

    # Preserva entry editoriali senza parquet locale né derive (es. adottati)
    derived_slugs = {d["slug"] for d in datasets}
    for ds_slug, ds in editorial.items():
        if ds_slug not in derived_slugs:
            datasets.append(_slim_dataset_entry(ds))

    catalog: dict[str, Any] = {
        "source_repo": layout.source_repo,
        "datasets": sorted(datasets, key=lambda d: d["slug"]),
    }
    return catalog, {"derive": derive_errors, "validation": [], "coverage": coverage_gaps}


# ---------------------------------------------------------------------------
# marts
# ---------------------------------------------------------------------------


def _mart_layout(
    manifest: DatasetManifest, contract: PathContract
) -> tuple[str, int | None, str | None]:
    """Layout di pubblicazione del mart: (layout, year, errore).

    Rispecchia il runner: tabella con ``years`` esplicite → flat; repo flat →
    flat; dataset multi-anno → glob (i file per-anno hanno lo stesso nome);
    mono-anno → path puntuale.
    """
    if contract.mart_layout == "flat":
        return "flat", None, None
    if len(manifest.years) > 1:
        return "glob", None, None
    if len(manifest.years) == 1:
        return "year", manifest.years[0], None
    return "", None, "layout 'year' senza years dichiarati"


def build_mart_catalog(
    layout: RepoLayout,
    path_contract: PathContract | None = None,
    slug: str | None = None,
) -> tuple[dict[str, Any], dict[str, list[str]]]:
    """Costruisce la sezione ``marts``.

    Nessun path: ogni mart dichiara ``layout`` (flat|year|glob) e, per
    ``year``, l'anno. Il path è derivato dal contratto ``gcs`` del repo.
    """
    contract = path_contract or PathContract()
    marts: list[dict[str, Any]] = []
    derive_errors: list[str] = []

    for manifest in iter_manifests(layout):
        if slug and manifest.slug != slug:
            continue
        rules = manifest.mart_rules or {}
        for table in manifest.mart_tables:
            name = table.get("name")
            if not name:
                continue
            if table.get("internal"):
                # staging interno: niente voce `marts` (non esiste su GCS)
                continue
            entry: dict[str, Any] = {
                "slug": f"{manifest.slug}__{name}",
                "dataset": manifest.slug,
                "table": name,
            }
            # Tabella con years esplicite = multi-anno: il runner la scrive
            # flat (data/mart/{dataset}/{table}.parquet, no dir anno).
            if table.get("years"):
                entry["layout"] = "flat"
            else:
                mart_layout, year, error = _mart_layout(manifest, contract)
                if error:
                    derive_errors.append(f"{manifest.slug}__{name}: {error}")
                    continue
                entry["layout"] = mart_layout
                if year is not None:
                    entry["year"] = year
            primary_key = (rules.get(name) or {}).get("primary_key")
            if primary_key:
                entry["primary_key"] = list(primary_key)
            marts.append(_omit_empty(entry))

    catalog: dict[str, Any] = {"marts": sorted(marts, key=lambda m: m["slug"])}
    return catalog, {"derive": derive_errors, "validation": []}


# ---------------------------------------------------------------------------
# checks (anomalie)
# ---------------------------------------------------------------------------


def build_checks(layout: RepoLayout) -> tuple[dict[str, Any], dict[str, list[str]]]:
    """Anomalie di struttura riproducibili dal config (vuoto = tutto a posto).

    Sostituisce la vecchia sezione ``signals``: solo voci ``warn``/``error``,
    nessuna riga di stato "ok" per ogni dataset (era il 16% del file senza
    aggiungere informazione — lo stato operativo vive in ``last_run``).

    Unico controllo oggi: dataset senza mart. La vecchia regola "mancante
    sql/clean.sql" era irraggiungibile (``is_mart_only`` è definito come
    ``not has_clean_sql``), per questo tutte le 559 signals storiche erano
    ``ok``.
    """
    checks: list[dict[str, Any]] = []

    for manifest in iter_manifests(layout):
        if _mart_ok(manifest):
            continue
        checks.append(
            {
                "id": manifest.slug,
                "status": "warn",
                "detail": "nessun mart",
                "action": "aggiungere mart SQL per completare il dataset",
            }
        )

    return {"checks": checks}, {"derive": [], "validation": []}


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
    (pattern eurostat-mcp). Se la dir non esiste, lista vuota senza errori.
    """
    codelists_dir = codelists_dir or (layout.repo_root / "codelists")
    if not codelists_dir.is_dir():
        return {"codelists": []}, {"derive": [], "validation": []}
    names = sorted(p.stem for p in codelists_dir.glob("*.csv"))
    return {"codelists": names}, {"derive": [], "validation": []}


# ---------------------------------------------------------------------------
# Convenience: unico entry point
# ---------------------------------------------------------------------------


def build_registry(
    layout: RepoLayout,
    *,
    path_contract: PathContract | None = None,
    existing: dict[str, Any] | None = None,
    semantic_types_path: Path | None = None,
    slug: str | None = None,
) -> dict[str, Any]:
    """Genera l'artifact registry UNICO per un repo (schema v2).

    Args:
        layout: Repo layout.
        path_contract: Contratto GCS del repo (``--prefix``/``--flat``).
        existing: Registry committato precedente: metadata editoriali,
            partizioni e ultima esecuzione delle entry non rieseguite.
        semantic_types_path: Vocabolario semantic types (opzionale).
        slug: Limita a un singolo slug (debug).

    Returns:
        Dict ``{"registry": {...}, "errors": {...}}`` con ``errors`` per
        sezione (``derive``/``validation``) + ``registry`` per la validazione
        dell'intero artifact.
    """
    contract = path_contract or PathContract()
    catalog, cc_errors = build_clean_catalog(
        layout,
        existing=existing,
        semantic_types_path=semantic_types_path,
        path_contract=contract,
        slug=slug,
    )
    marts, mc_errors = build_mart_catalog(layout, path_contract=contract, slug=slug)
    checks_payload, ck_errors = build_checks(layout)
    codelists, cl_errors = build_codelists(layout)

    gcs: dict[str, str] = {"clean": contract.clean_layout}
    prefix = normalize_prefix(contract.prefix)
    if prefix:
        gcs["prefix"] = prefix

    registry: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "repo": layout.source_repo.split("/")[-1] or "unknown",
        "source_repo": layout.source_repo,
        "updated_at": str(datetime.now(UTC).date()),
        "gcs": gcs,
        "datasets": catalog.get("datasets", []),
        "marts": marts.get("marts", []),
        "checks": checks_payload.get("checks", []),
        "codelists": codelists.get("codelists", []),
    }

    # Preserva `last_run` delle entry non rieseguite: in CI post-merge le
    # partizioni non rieseguite non hanno run record locali, quindi il blocco
    # sparirebbe a ogni rigenerazione. Le partizioni sono già fuse in
    # build_clean_catalog (locale vince).
    registry = _merge_existing_last_run(registry, existing=existing)

    # Ordine chiavi deterministico: le entry derivano da path diversi
    # (builder, existing) e l'ordine di inserimento cambierebbe a ogni
    # rigenerazione → diff rumorosi.
    registry = _canonicalize_registry(registry)

    registry_errors = validate_artifact(registry, "registry.schema.json")
    coverage_gaps = cc_errors.get("coverage", [])
    errors = {
        "datasets": cc_errors,
        "marts": mc_errors,
        "checks": ck_errors,
        "codelists": cl_errors,
        "registry": {"derive": [], "validation": registry_errors},
    }
    return {"registry": registry, "errors": errors, "incomplete": coverage_gaps}


def _merge_existing_last_run(
    registry: dict[str, Any],
    *,
    existing: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Ripopola i blocchi ``last_run`` mancanti dall'existing (CI e transizione v1).

    Due sorgenti, in ordine:
    - ``existing.datasets[].last_run`` (registry v2 già committato);
    - ``existing.signals[].run`` (registry v1: la freschezza stava lì; al primo
      build v2 le entry senza run record locali la perderebbero).
    """
    previous: dict[str, dict[str, Any]] = {}
    if existing:
        for ds in existing.get("datasets", []) or []:
            block = ds.get("last_run")
            if block and block.get("at") and ds.get("slug"):
                previous[ds["slug"]] = block
        for sig in existing.get("signals", []) or []:
            run = sig.get("run") or {}
            at = run.get("started_at") or run.get("at")
            year = run.get("year")
            status = run.get("status")
            if not (at and year is not None and status):
                continue
            slug = str(sig.get("label") or sig.get("id") or "").rsplit(":", 1)[-1]
            if slug:
                previous.setdefault(slug, {"at": at, "year": year, "status": status})

    for ds in registry.get("datasets", []) or []:
        if "last_run" not in ds and ds.get("slug") in previous:
            ds["last_run"] = previous[ds["slug"]]

    return registry


# Ordine canonico delle chiavi per sezione. Le chiavi non elencate vengono
# accodate in ordine di arrivo (nessuna perdita).
_CANONICAL_ORDER: dict[str, tuple[str, ...]] = {
    "datasets": (
        "slug",
        "name",
        "description",
        "source_id",
        "tags",
        "category",
        "period",
        "partitions",
        "columns",
        "mart_refs",
        "last_run",
        "location",
    ),
    "marts": (
        "slug",
        "dataset",
        "table",
        "layout",
        "year",
        "primary_key",
        "location",
    ),
    "checks": (
        "id",
        "status",
        "detail",
        "action",
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
