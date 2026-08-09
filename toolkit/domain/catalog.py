"""Catalog resolver — fonte unica per dataset discovery.

Legge i registry committati (``{repo}/registry/registry.json`` unico, fusion
ADR, con fallback sui file legacy) e scansiona il workspace locale e i
dataset.yml per fornire una vista unificata di tutti i dataset del Lab, sia
in sviluppo che pubblicati.

Supporta tre ``source``:
- ``"gcs"``: dataset pubblicati su GCS (dai registry committati)
- ``"workspace"``: dataset in sviluppo (da dataset.yml + clean parquet locali)
- ``"all"`` (default): unione di entrambi

Le funzioni qui NON gestiscono errori MCP (ToolkitClientError) — quelle
vanno aggiunte nei wrapper MCP.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from lab_connectors.gcs.paths import CLEAN_BUCKET, MART_BUCKET

from toolkit.core.duckdb_shape import parquet_preview
from toolkit.core.io import read_json_or_none, read_yaml
from toolkit.core.paths import WORKSPACE_ROOT

# ---------------------------------------------------------------------------
# Costanti
# ---------------------------------------------------------------------------

VALID_LAYERS = frozenset({"clean", "mart"})

LOCAL_BUCKET = "local"  # bucket fittizio per file locali
LOCAL_MART_BUCKET = "local-mart"  # bucket fittizio per mart locali
VALID_SOURCES: frozenset[Literal["gcs", "workspace", "all"]] = frozenset(
    {"gcs", "workspace", "all"}
)
VALID_STAGES: frozenset[Literal["candidates", "support", "all"]] = frozenset(
    {"candidates", "support", "all"}
)
VALID_RUN_STATUSES = frozenset({"SUCCESS", "FAILED", "DRY_RUN", "RUNNING"})


# ---------------------------------------------------------------------------
# Helper comuni
# ---------------------------------------------------------------------------


def _is_data_parquet(file: dict[str, Any]) -> bool:
    """True se il file e' un parquet dati (non pipeline_run.json)."""
    return file["path"].endswith(".parquet")


def _matches_layer(file: dict[str, Any], layer: str | None) -> bool:
    """True se il file appartiene al layer richiesto.

    - ``layer=None``: tutto.
    - ``layer="clean"``: bucket clean o locale.
    - ``layer="mart"``: bucket mart.
    """
    if layer is None:
        return True
    if layer == "clean":
        return file["bucket"] in (CLEAN_BUCKET, LOCAL_BUCKET)
    if layer == "mart":
        return file["bucket"] in (MART_BUCKET, LOCAL_MART_BUCKET)
    return False


def _is_local(file: dict[str, Any]) -> bool:
    """True se il file e' locale (non su GCS)."""
    return file.get("_local", False) or file.get("bucket") == LOCAL_BUCKET


def _parse_clean_filename(filename: str) -> tuple[str, int] | None:
    """Estrae slug e anno da ``{slug}_{year}_clean.parquet``."""
    if not filename.endswith("_clean.parquet"):
        return None
    stem = filename[: -len(".parquet")]
    if not stem.endswith("_clean"):
        return None
    prefix = stem[: -len("_clean")]
    *slug_parts, year_str = prefix.rsplit("_", 1)
    if not slug_parts or not slug_parts[0]:
        return None
    if year_str.isdigit() and len(year_str) == 4:
        return ("_".join(slug_parts), int(year_str))
    return None


def _find_latest_run_status(slug: str, runs_root: Path | None = None) -> str | None:
    """Cerca l'ultimo run record per un dataset slug.

    Scansiona ``{runs_root or WORKSPACE/out}/data/_runs/{slug}/``.
    """
    if runs_root is not None:
        runs_base = runs_root / "data" / "_runs" / slug
    else:
        runs_base = WORKSPACE_ROOT / "out" / "data" / "_runs" / slug
    if not runs_base.exists():
        return None

    latest: dict[str, Any] | None = None
    latest_mtime: float = 0.0

    for year_dir in sorted(runs_base.iterdir()):
        if not year_dir.is_dir() or not year_dir.name.isdigit():
            continue
        for run_file in year_dir.glob("*.json"):
            try:
                mtime = run_file.stat().st_mtime
            except OSError:
                continue
            if mtime > latest_mtime:
                data = read_json_or_none(run_file)
                if isinstance(data, dict):
                    latest = data
                    latest_mtime = mtime

    return latest.get("status") if latest else None


# ---------------------------------------------------------------------------
# Scan workspace — parquet files + pipeline metadata
# ---------------------------------------------------------------------------


def _scan_workspace_parquets(workspace: Path = WORKSPACE_ROOT) -> list[dict[str, Any]]:
    """Scansiona il workspace per clean/mart parquet locali (solo repo dati).

    Come ``_scan_committed_catalogs``: solo dir di primo livello con
    ``registry/`` o ``datasets/`` (eurostat/, dataset-incubator/, dcl-bologna/,
    ...). Esclude repo non-dati (es. project residui, out/, data/) che
    non hanno dataset.yml — evitando rglob costosi su decine di repo.

    Clean: ``{slug}/{year}/{slug}_{year}_clean.parquet`` — slug dal filename.
    Mart: ``{slug}/{year}/mart_{table}.parquet`` (naming canonico della
    pipeline: il file è ``mart_{table}.parquet``) — slug dalla directory,
    bucket ``local-mart`` per il layer.
    """
    if not workspace.is_dir():
        return []

    entries: list[dict[str, Any]] = []
    # Dedup per clean (3-tuple) e mart (4-tuple con table).
    seen: set[tuple[str, int | None, str] | tuple[str, int | None, str, str]] = set()

    for repo_dir in sorted(p for p in workspace.iterdir() if p.is_dir()):
        # Solo repo dati: registry/ (artifact committati) o datasets/ (layout).
        if not (repo_dir / "registry").is_dir() and not (repo_dir / "datasets").is_dir():
            continue

        # Clean parquet
        for fpath in repo_dir.rglob("*_clean.parquet"):
            parsed = _parse_clean_filename(fpath.name)
            if parsed is None:
                continue
            slug, year = parsed
            rel_path = str(fpath.relative_to(workspace))
            dedup_key = (slug, year, LOCAL_BUCKET)
            if dedup_key in seen:
                continue
            seen.add(dedup_key)

            stat = fpath.stat()
            entries.append(
                {
                    "url": str(fpath),
                    "slug": slug,
                    "bucket": LOCAL_BUCKET,
                    "year": year,
                    "path": rel_path,
                    "size_bytes": stat.st_size,
                    "updated": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
                    "_local": True,
                }
            )

        # Mart locali (naming canonico pipeline: mart_{table}.parquet).
        # Struttura standard: {slug}/{year}/mart_*.parquet; layout legacy
        # flat: {slug}/mart_*.parquet (anno sconosciuto → None).
        for fpath in repo_dir.rglob("mart_*.parquet"):
            parts = fpath.relative_to(repo_dir).parts
            # parts[-1]=file, parts[-2]=year|slug, parts[-3]=slug (se year)
            if len(parts) >= 3 and parts[-2].isdigit() and len(parts[-2]) == 4:
                mart_slug = parts[-3]
                mart_year: int | None = int(parts[-2])
            else:
                mart_slug = parts[-2]
                mart_year = None
            rel_path = str(fpath.relative_to(workspace))
            table = fpath.stem
            mart_key: tuple[str, int | None, str, str] = (
                mart_slug,
                mart_year,
                LOCAL_MART_BUCKET,
                table,
            )
            if mart_key in seen:
                continue
            seen.add(mart_key)

            stat = fpath.stat()
            entries.append(
                {
                    "url": str(fpath),
                    "slug": mart_slug,
                    "bucket": LOCAL_MART_BUCKET,
                    "year": mart_year,
                    "path": rel_path,
                    "table": table,
                    "size_bytes": stat.st_size,
                    "updated": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
                    "_local": True,
                }
            )

    return entries


def _scan_workspace_configs(
    workspace: Path = WORKSPACE_ROOT,
    stage: str = "all",
) -> dict[str, dict[str, Any]]:
    """Scansiona dataset.yml nel workspace e restituisce metadata di pipeline.

    Cross-repo (come ``_scan_workspace_parquets``): usa la scoperta per
    convenzione ``repo_dataset_dirs`` (toolkit.registry.layout) — ogni dir di
    primo livello con {slug}/dataset.yml è una sezione dati (datasets/,
    support/, candidates/...). Nuovi repo con layout custom funzionano senza
    toccare il codice.

    Returns:
        Dict slug → {dataset_name, stage, years, has_clean, has_mart,
                     last_run_status, config_path, root}
    """
    from toolkit.registry.layout import repo_dataset_dirs

    # Nome dir DI → stage legacy (contratto pre-fusion).
    DI_STAGE = {"candidates": "candidates", "compose": "compose", "support_datasets": "support"}

    results: dict[str, dict[str, Any]] = {}
    dirs_to_scan: list[tuple[str, Path]] = []

    for repo_dir in sorted(p for p in workspace.iterdir() if p.is_dir()):
        for section in repo_dataset_dirs(repo_dir):
            section_dir = repo_dir / section
            if not section_dir.is_dir():
                continue
            # Filtro stage: "all" prende tutto; "candidates"/"support" solo DI.
            if stage == "candidates" and section != "candidates":
                continue
            if stage == "support" and section != "support_datasets":
                continue
            stage_name = DI_STAGE.get(section, section)
            dirs_to_scan.append((stage_name, section_dir))

    for stage_name, scan_dir in dirs_to_scan:
        for dataset_yml in sorted(scan_dir.rglob("dataset.yml")):
            if "templates" in dataset_yml.parts:
                continue

            parent_dir = dataset_yml.parent
            rel_path = parent_dir.relative_to(scan_dir)
            dir_slug = str(rel_path.as_posix())
            if rel_path.parent == Path("."):
                dir_slug = parent_dir.name

            # Lettura YAML leggera (senza validazione piena)
            try:
                data = read_yaml(dataset_yml)
            except Exception:
                continue
            if not isinstance(data, dict):
                continue

            # Slug: dataset.yml > directory name — normalizzato a underscore
            slug = (data.get("slug") or dir_slug).replace("-", "_")

            ds = data.get("dataset", {}) or {}
            name = ds.get("name", slug) if isinstance(ds, dict) else slug
            years = ds.get("years", []) if isinstance(ds, dict) else []
            if isinstance(years, int):
                years = [years]

            root_raw = data.get("root")
            if root_raw and isinstance(root_raw, str):
                resolved_root = (dataset_yml.parent / root_raw).resolve()
            else:
                resolved_root = workspace / "out"

            out_root = resolved_root / "data"
            # I path su disco seguono la convenzione slug (underscore)
            dataset_name_for_path = slug
            clean_dir = out_root / "clean" / dataset_name_for_path
            mart_dir = out_root / "mart" / dataset_name_for_path
            has_clean = clean_dir.exists() and any(clean_dir.iterdir())
            has_mart = mart_dir.exists() and any(mart_dir.iterdir())
            last_run_status = _find_latest_run_status(
                dataset_name_for_path, runs_root=resolved_root
            )

            results[slug] = {
                "dataset_name": str(name),
                "stage": stage_name,
                "years": [int(y) for y in years] if isinstance(years, list) else [],
                "has_clean": has_clean,
                "has_mart": has_mart,
                "last_run_status": last_run_status,
                "config_path": str(dataset_yml),
                "root": str(resolved_root),
            }

    return results


def _gcs_files_from_registry(workspace: Path = WORKSPACE_ROOT) -> list[dict[str, Any]]:
    """File GCS dai registry.json committati (fusion ADR — drop del manifest).

    Le location del catalogo sono esatte per repo (layout DI year o eurostat
    flat): file diretto (``.parquet``) o dir (→ glob ``*.parquet``). I mart
    hanno la location file diretta. Sostituisce il gcs_manifest.json, che
    assegnava lo slug ``parts[0]`` (sbagliato per i layout con prefisso org).
    """
    from toolkit.registry.reader import load_repo_registry

    files: list[dict[str, Any]] = []
    if not workspace.is_dir():
        return files
    for repo_dir in sorted(p for p in workspace.iterdir() if p.is_dir()):
        payload, _is_legacy = load_repo_registry(repo_dir)
        if payload is None:
            continue
        # clean: datasets → location
        for ds in payload.get("datasets", []) or []:
            loc = ds.get("location") or {}
            path = loc.get("path", "")
            if not path or CLEAN_BUCKET not in path:
                continue
            url = path if path.endswith(".parquet") else path.rstrip("/") + "/*.parquet"
            files.append(
                {
                    "url": url,
                    "slug": ds.get("slug", ""),
                    "bucket": CLEAN_BUCKET,
                    "year": None,
                    "path": url,
                    "_gcs": True,
                }
            )
        # mart: location file diretta (slug = dataset, table = nome tabella)
        for m in payload.get("marts", []) or []:
            loc = m.get("location") or {}
            path = loc.get("path", "")
            if not path or MART_BUCKET not in path or not path.endswith(".parquet"):
                continue
            files.append(
                {
                    "url": path,
                    "slug": m.get("dataset", ""),
                    "bucket": MART_BUCKET,
                    "year": None,
                    "path": path,
                    "table": m.get("table"),
                    "_gcs": True,
                }
            )
    return files


def _merge_gcs_and_local(
    gcs_files: list[dict[str, Any]],
    local_files: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Fonde entry GCS e locali per file-level.

    Stesso slug+anno+bucket → preferisce locale.
    Stesso slug+anno, bucket diverso → tiene entrambi.
    """
    local_keys: set[tuple[str, int | None, str]] = {
        (lf["slug"], lf["year"], lf["bucket"]) for lf in local_files
    }
    merged = list(local_files)
    for gf in gcs_files:
        key = (gf["slug"], gf["year"], gf["bucket"])
        if key not in local_keys:
            merged.append(gf)
    return merged


# ---------------------------------------------------------------------------
# Catalogo semantico committato (mossa 1: find semantico nel resolver)
# ---------------------------------------------------------------------------

# Campi semantici arricchiti dal catalogo committato (se presenti).
SEMANTIC_FIELDS = (
    "name",
    "description",
    "source",
    "source_id",
    "tags",
    "category",
    "period",
    "mart_refs",
)


def _scan_committed_catalogs(workspace: Path = WORKSPACE_ROOT) -> dict[str, dict[str, Any]]:
    """Scansiona i cataloghi semantici committati nei repo del workspace.

    Usa ``load_repo_registry`` (lo stesso reader del resolver GCS): legge il
    ``registry.json`` unico dei repo migrati (fusion ADR) e il fallback
    ``clean_catalog.json`` dei repo legacy. Ritorna ``{slug: entry semantica}``
    con ``_repo`` (nome dir) aggiunto.

    La semantica (columns con role/semantic_type, description, tags, period)
    vive nei registry generati dal registry builder e committati nei repo:
    qui diventa la fonte per il find semantico del resolver.
    """
    from toolkit.registry.reader import load_repo_registry

    result: dict[str, dict[str, Any]] = {}
    if not workspace.is_dir():
        return result
    for repo_dir in sorted(p for p in workspace.iterdir() if p.is_dir()):
        payload, _is_legacy = load_repo_registry(repo_dir)
        if payload is None:
            continue
        for ds in payload.get("datasets", []) or []:
            slug = ds.get("slug")
            if slug:
                entry = dict(ds)
                entry["_repo"] = repo_dir.name
                result[slug] = entry
    return result


def _semantic_query_hits(
    semantic: dict[str, dict[str, Any]], query: str
) -> dict[str, list[dict[str, Any] | None]]:
    """Slug che matchano la query sulla semantica (meta o colonne).

    Returns:
        ``{slug: [None | colonna_matchata, ...]}`` — ``None`` = meta match
        (name/description/source/tags/category), dict = colonna matchata.
    """
    q = query.lower()
    hits: dict[str, list[dict[str, Any] | None]] = {}
    for slug, s in semantic.items():
        matched: list[dict[str, Any] | None] = []
        meta_parts = " ".join(
            str(s.get(k) or "") for k in ("name", "description", "source", "tags", "category")
        )
        if q in slug.lower() or q in meta_parts.lower():
            matched.append(None)
        for col in s.get("columns", []) or []:
            if (
                q in str(col.get("name", "")).lower()
                or q in str(col.get("description", "")).lower()
            ):
                matched.append(
                    {
                        "name": col.get("name"),
                        "type": col.get("type"),
                        "role": col.get("role"),
                        "description": col.get("description"),
                    }
                )
        if matched:
            hits[slug] = matched
    return hits


# ---------------------------------------------------------------------------
# Resolver
# ---------------------------------------------------------------------------


class CatalogResolver:
    """Risolve slug dataset → metadati, unificando GCS + workspace.

    Supporta tre modalita' (``source``):
    - ``"gcs"``: solo file GCS dai registry.json committati (fusion ADR)
    - ``"workspace"``: solo workspace (dataset in sviluppo, con pipeline status)
    - ``"all"`` (default): unione — merge GCS + workspace, arricchito con pipeline status
    """

    def __init__(
        self,
        manifest_url: str | None = None,
        workspace: str | Path | None = None,
        include_local: bool = True,
    ) -> None:
        # manifest_url è accettato per compatibilità di firma ma NON è più
        # usato: il gcs_manifest.json è stato rimpiazzato dai registry.json
        # committati (fusion ADR — path GCS esatti per repo).
        self._manifest_url = manifest_url
        self._workspace = Path(workspace or WORKSPACE_ROOT)
        self._include_local = include_local
        self._gcs_entries: list[dict[str, Any]] | None = None
        self._local_entries: list[dict[str, Any]] | None = None
        self._workspace_configs: dict[str, dict[str, Any]] | None = None
        self._semantic: dict[str, dict[str, Any]] | None = None

    def _load_semantic(self) -> dict[str, dict[str, Any]]:
        """Carica i cataloghi semantici committati nel workspace (lazy)."""
        if self._semantic is not None:
            return self._semantic
        if not self._include_local:
            self._semantic = {}
            return self._semantic
        self._semantic = _scan_committed_catalogs(self._workspace)
        return self._semantic

    def _merge_semantic(self, entry: dict[str, Any], semantic: dict[str, Any] | None) -> None:
        """Arricchisce un entry con i campi semantici del catalogo committato."""
        if not semantic:
            return
        for field in SEMANTIC_FIELDS:
            if semantic.get(field):
                entry[field] = semantic[field]
        entry["_repo"] = semantic.get("_repo")
        entry["_has_semantic"] = True

    def _gcs_files(self) -> list[dict[str, Any]]:
        """File GCS dai registry.json committati (cache per istanza).

        I registry committati nel workspace sono la fonte GCS: disponibili
        sempre (non dipendono da ``include_local``, che governa solo lo scan
        dei parquet locali).
        """
        if self._gcs_entries is not None:
            return self._gcs_entries
        self._gcs_entries = _gcs_files_from_registry(self._workspace)
        return self._gcs_entries

    def _load_local_parquets(self) -> list[dict[str, Any]]:
        if self._local_entries is not None:
            return self._local_entries
        if not self._include_local:
            self._local_entries = []
            return self._local_entries
        self._local_entries = _scan_workspace_parquets(self._workspace)
        return self._local_entries

    def _load_workspace_configs(self) -> dict[str, dict[str, Any]]:
        if self._workspace_configs is not None:
            return self._workspace_configs
        if not self._include_local:
            self._workspace_configs = {}
            return self._workspace_configs
        self._workspace_configs = _scan_workspace_configs(self._workspace)
        return self._workspace_configs

    # ------------------------------------------------------------------
    # Pubblici
    # ------------------------------------------------------------------

    def list_datasets(
        self,
        query: str = "",
        layer: str | None = None,
        limit: int = 15,
        source: str = "all",
        stage: str = "all",
        status_filter: str | None = None,
        metric_only: bool = False,
    ) -> dict[str, Any]:
        """Cerca dataset per slug, sorgente, layer o testo.

        Args:
            query: Testo da cercare — oltre allo slug, matcha la semantica
                dei cataloghi committati (name, description, tags, category,
                colonne) se presente nel workspace.
            layer: ``"clean"``, ``"mart"`` o ``None`` (entrambi).
            limit: Max risultati (default 15). Usa ``0`` per nessun limite.
            source: ``"gcs"``, ``"workspace"`` o ``"all"`` (default).
            stage: Filtro workspace: ``"candidates"``, ``"support"``, ``"all"`` (default).
            status_filter: Filtro run status: ``"SUCCESS"``, ``"FAILED"``, ecc.
            metric_only: Se True, solo dataset con almeno una colonna role=metric
                (richiede la semantica del catalogo committato).

        Returns:
            Dict con ``datasets`` (lista), ``total_count``, ``truncated``.
            Le entry con semantica includono ``description``, ``tags``,
            ``category``, ``period``, ``matched_columns`` e ``meta_match``.
        """
        # Input validation
        if source not in VALID_SOURCES:
            raise ValueError(f"source deve essere uno tra: {', '.join(sorted(VALID_SOURCES))}")
        if stage not in VALID_STAGES:
            raise ValueError(f"stage deve essere uno tra: {', '.join(sorted(VALID_STAGES))}")
        if status_filter is not None and status_filter not in VALID_RUN_STATUSES:
            raise ValueError(
                f"status_filter deve essere uno tra: {', '.join(sorted(VALID_RUN_STATUSES))}"
            )

        # Build agg map: slug → entry
        datasets: dict[str, dict[str, Any]] = {}

        # Semantic layer: hit di query sulla semantica + slug con metriche
        semantic = self._load_semantic()
        sem_hits: dict[str, list[dict[str, Any] | None]] = (
            _semantic_query_hits(semantic, query) if query else {}
        )
        metric_slugs: set[str] = set()
        if metric_only:
            metric_slugs = {
                slug
                for slug, s in semantic.items()
                if any(c.get("role") == "metric" for c in (s.get("columns") or []))
            }

        # --- Source: GCS ---
        if source in ("gcs", "all"):
            gcs_configs = self._load_workspace_configs()
            for f in self._gcs_files():
                if not _is_data_parquet(f):
                    continue
                if not _matches_layer(f, layer):
                    continue
                slug = f["slug"]
                if slug is None:
                    continue
                if query and query.lower() not in slug.lower() and slug not in sem_hits:
                    continue
                if metric_only and slug not in metric_slugs:
                    continue

                if slug not in datasets:
                    info = gcs_configs.get(slug, {})
                    datasets[slug] = self._empty_entry(slug, info)
                    datasets[slug]["_buckets"] = set()
                entry = datasets[slug]
                entry["_buckets"].add(f["bucket"])
                if f["year"] is not None:
                    entry["years"].add(f["year"])
                entry["file_count"] += 1
                entry["total_size_bytes"] += f.get("size_bytes", 0)
                entry["has_remote"] = True

        # --- Source: workspace ---
        if source in ("workspace", "all"):
            configs = self._load_workspace_configs()
            local_files = self._load_local_parquets()

            # Parquet-only slugs (dataset senza dataset.yml, es. sub-dataset)
            parquet_only_slugs = {lf["slug"] for lf in local_files} - set(configs)

            # Process config entries
            for slug in set(configs) | parquet_only_slugs:
                if query and query.lower() not in slug.lower() and slug not in sem_hits:
                    continue
                if metric_only and slug not in metric_slugs:
                    continue
                info = configs.get(slug, {})

                if status_filter is not None and info.get("last_run_status") != status_filter:
                    continue

                if slug not in datasets:
                    datasets[slug] = self._empty_entry(slug, info)
                entry = datasets[slug]

                # Pipeline metadata (dal workspace config, se presente)
                if info:
                    self._merge_pipeline_info(entry, info)

                # Clean parquet count
                for lf in local_files:
                    if lf["slug"] == slug and _matches_layer(lf, layer):
                        if lf["year"] is not None:
                            entry["years"].add(lf["year"])
                        entry["file_count"] += 1
                        entry["total_size_bytes"] += lf.get("size_bytes", 0)
                        entry["has_local"] = True

        # Apply stage filter (solo workspace): tieni anche parquet-only
        if source == "workspace":
            datasets = {s: e for s, e in datasets.items() if e.get("stage") or e.get("has_local")}

        # Finalizza entry
        result = []
        for slug in sorted(datasets):
            entry = datasets[slug]
            entry["years"] = sorted(entry["years"])
            self._merge_semantic(entry, semantic.get(slug))
            hits = sem_hits.get(slug)
            if hits is not None:
                entry["matched_columns"] = [h for h in hits if h]
                entry["meta_match"] = any(h is None for h in hits)
            self._finalize_layer(entry, layer)
            result.append(entry)

        total_count = len(result)
        truncated = bool(limit and total_count > limit)
        if limit and total_count > limit:
            result = result[:limit]

        return {"datasets": result, "total_count": total_count, "truncated": truncated}

    def _empty_entry(self, slug: str, info: dict[str, Any]) -> dict[str, Any]:
        return {
            "slug": slug,
            "dataset_name": info.get("dataset_name", slug),
            "stage": info.get("stage"),
            "years": set(),
            "file_count": 0,
            "total_size_bytes": 0,
            "has_local": False,
            "has_remote": False,
            "has_clean": info.get("has_clean", False),
            "has_mart": info.get("has_mart", False),
            "last_run_status": info.get("last_run_status"),
            "config_path": info.get("config_path"),
        }

    def _merge_pipeline_info(self, entry: dict[str, Any], info: dict[str, Any]) -> None:
        """Arricchisce un entry con pipeline metadata dal workspace."""
        for key in (
            "stage",
            "has_clean",
            "has_mart",
            "last_run_status",
            "config_path",
            "dataset_name",
        ):
            val = info.get(key)
            if val is not None:
                entry[key] = val
        # Anni dal workspace config (lista → set per merge con GCS)
        ws_years = info.get("years", [])
        if ws_years:
            entry["years"].update(ws_years)

    def _finalize_layer(self, entry: dict[str, Any], layer: str | None) -> None:
        """Calcola il layer effettivo da bucket GCS + flag workspace."""
        if layer:
            entry["layer"] = layer
            return

        buckets = entry.pop("_buckets", set())
        has_gcs_clean = CLEAN_BUCKET in buckets
        has_gcs_mart = MART_BUCKET in buckets
        has_local_bucket = LOCAL_BUCKET in buckets

        is_clean = (
            has_gcs_clean
            or has_local_bucket
            or entry.get("has_clean", False)
            or entry.get("has_local", False)
        )
        is_mart = has_gcs_mart or entry.get("has_mart", False)

        if is_clean and is_mart:
            entry["layer"] = "clean,mart"
        elif is_clean:
            entry["layer"] = "clean"
        else:
            entry["layer"] = "mart"

    def resolve_slug(
        self,
        slug: str,
        layer: str | None = None,
        year: int | None = None,
        table: str | None = None,
    ) -> list[dict[str, Any]]:
        """Risolve uno slug nei file parquet corrispondenti.

        Args:
            slug: Slug del dataset.
            layer: ``"clean"``, ``"mart"`` o ``None`` (entrambi).
            year: Anno specifico o ``None`` (tutti).
            table: Nome tabella mart (solo layer=mart).
                Es. ``"mart_top_sa"`` — match sul filename senza estensione.

        Cerca prima nel workspace locale, poi su GCS.
        """
        gcs_files = self._gcs_files()
        local_files = self._load_local_parquets()
        files = _merge_gcs_and_local(gcs_files, local_files)

        matching = []
        for f in files:
            if not _is_data_parquet(f):
                continue
            if not _matches_layer(f, layer):
                continue
            if f["slug"] != slug:
                continue
            if year is not None and f["year"] is not None and f["year"] != year:
                continue
            if table is not None:
                # Match sul filename senza estensione
                fname = f["path"].rsplit("/", 1)[-1]  # ultimo segmento
                fstem = fname.rsplit(".", 1)[0] if "." in fname else fname
                if fstem != table:
                    continue
            matching.append(f)

        if not matching:
            parts = [f"layer={layer or 'any'}", f"year={year or 'any'}"]
            if table:
                parts.append(f"table={table}")
            raise FileNotFoundError(f"Slug '{slug}' non trovato ({', '.join(parts)})")

        matching.sort(key=lambda f: (0 if _is_local(f) else 1, -(f["year"] or 9999), f["path"]))
        return matching

    def describe_slug(
        self,
        slug: str,
        layer: str = "clean",
        year: int | None = None,
        source: str = "all",
    ) -> dict[str, Any]:
        """Schema DuckDB + row count per slug.

        Args:
            slug: Slug del dataset.
            layer: ``"clean"`` (default) o ``"mart"``.
            year: Anno specifico o ``None`` (ultimo disponibile).
            source: ``"gcs"``, ``"workspace"``, ``"all"`` (default).

        Raises:
            FileNotFoundError: se slug non trovato nella source richiesta.
        """
        # Source filter: verifica esistenza prima di risolvere
        if source == "gcs":
            gcs_slugs = {f["slug"] for f in self._gcs_files() if _is_data_parquet(f)}
            if slug not in gcs_slugs:
                raise FileNotFoundError(f"Slug '{slug}' non trovato su GCS (source=gcs)")
        elif source == "workspace":
            configs = self._load_workspace_configs()
            local_slugs = {lf["slug"] for lf in self._load_local_parquets()}
            if slug not in configs and slug not in local_slugs:
                raise FileNotFoundError(
                    f"Slug '{slug}' non trovato nel workspace (source=workspace)"
                )

        files = self.resolve_slug(slug, layer=layer, year=year)
        if not files:
            raise FileNotFoundError(f"Slug '{slug}' non trovato (year={year})")

        target = files[0]
        parquet_path = Path(target["url"])
        actual_year = target["year"]
        is_local = _is_local(target)

        try:
            result = parquet_preview(parquet_path, limit=5)
            result["slug"] = slug
            result["year"] = actual_year
            result["layer"] = layer
            result["_local"] = is_local
            # Arricchimento semantico dal catalogo committato (se presente):
            # description/tags/period + role/semantic_type/description colonne.
            sem = self._load_semantic().get(slug)
            if sem:
                for field in ("description", "tags", "category", "period", "source"):
                    if sem.get(field):
                        result[field] = sem[field]
                sem_cols = {c.get("name"): c for c in (sem.get("columns") or [])}
                for col in result.get("columns", []) or []:
                    sc = sem_cols.get(col.get("name"))
                    if sc:
                        if sc.get("role"):
                            col["role"] = sc["role"]
                        if sc.get("semantic_type"):
                            col["semantic_type"] = sc["semantic_type"]
                        if sc.get("description"):
                            col["description"] = sc["description"]
                result["_repo"] = sem.get("_repo")
            return result
        except Exception as exc:
            raise RuntimeError(
                f"Impossibile leggere schema per {slug} ({parquet_path}): {exc}"
            ) from exc
