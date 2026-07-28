"""Catalog resolver condiviso CLI+MCP — fonte unica per dataset discovery.

Legge ``gcs_manifest.json`` (auto-generato dalla CI), scansiona il workspace
locale e i dataset.yml per fornire una vista unificata di tutti i dataset
del Lab, sia in sviluppo che pubblicati.

Supporta tre ``source``:
- ``"gcs"``: dataset pubblicati su GCS (dal manifest)
- ``"workspace"``: dataset in sviluppo (da dataset.yml + clean parquet locali)
- ``"all"`` (default): unione di entrambi

Usato da:
- CLI (futuro comando ``toolkit catalog ...``)
- MCP ``toolkit_find`` e ``toolkit_dataset_overview`` (via ``mcp/catalog_ops.py``)

Le funzioni qui NON gestiscono errori MCP (ToolkitClientError) — quelle
vanno aggiunte nei wrapper MCP.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from lab_connectors.gcs.manifest import MANIFEST_URL, read_manifest

from toolkit.core.duckdb_shape import parquet_preview
from toolkit.core.io import read_json_or_none, read_yaml
from toolkit.core.paths import WORKSPACE_ROOT

# ---------------------------------------------------------------------------
# Costanti
# ---------------------------------------------------------------------------

CLEAN_BUCKET = "dataciviclab-clean"
MART_BUCKET = "dataciviclab-mart"
VALID_LAYERS = frozenset({"clean", "mart"})

LOCAL_BUCKET = "local"  # bucket fittizio per file locali
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
        return file["bucket"] == MART_BUCKET
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
    """Scansiona il workspace per clean parquet locali."""
    incubator = workspace / "dataset-incubator"
    if not incubator.is_dir():
        return []

    entries: list[dict[str, Any]] = []
    seen: set[tuple[str, int, str]] = set()

    for fpath in incubator.rglob("*_clean.parquet"):
        parsed = _parse_clean_filename(fpath.name)
        if parsed is None:
            continue
        slug, year = parsed
        rel_path = str(fpath.relative_to(workspace))
        dedup_key = (slug, year, rel_path)
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

    return entries


def _scan_workspace_configs(
    workspace: Path = WORKSPACE_ROOT,
    stage: str = "all",
) -> dict[str, dict[str, Any]]:
    """Scansiona dataset.yml nel workspace e restituisce metadata di pipeline.

    Returns:
        Dict slug → {dataset_name, stage, years, has_clean, has_mart,
                     last_run_status, config_path, root}
    """
    incubator = workspace / "dataset-incubator"
    if not incubator.is_dir():
        return {}

    results: dict[str, dict[str, Any]] = {}
    dirs_to_scan: list[tuple[str, Path]] = []

    if stage in ("candidates", "all"):
        candidates_dir = incubator / "candidates"
        if candidates_dir.exists():
            dirs_to_scan.append(("candidates", candidates_dir))
    if stage in ("support", "all"):
        support_dir = incubator / "support_datasets"
        if support_dir.exists():
            dirs_to_scan.append(("support", support_dir))

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
# Resolver
# ---------------------------------------------------------------------------


class CatalogResolver:
    """Risolve slug dataset → metadati, unificando GCS + workspace.

    Supporta tre modalita' (``source``):
    - ``"gcs"``: solo manifest GCS (dataset pubblicati)
    - ``"workspace"``: solo workspace (dataset in sviluppo, con pipeline status)
    - ``"all"`` (default): unione — merge GCS + workspace, arricchito con pipeline status
    """

    def __init__(
        self,
        manifest_url: str | None = None,
        workspace: str | Path | None = None,
        include_local: bool = True,
    ) -> None:
        self._manifest_url = manifest_url or MANIFEST_URL
        self._workspace = Path(workspace or WORKSPACE_ROOT)
        self._include_local = include_local
        self._gcs_manifest: dict[str, Any] | None = None
        self._local_entries: list[dict[str, Any]] | None = None
        self._workspace_configs: dict[str, dict[str, Any]] | None = None

    def _load_gcs(self) -> dict[str, Any]:
        if self._gcs_manifest is not None:
            return self._gcs_manifest
        self._gcs_manifest = read_manifest(self._manifest_url)
        return self._gcs_manifest

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
    ) -> dict[str, Any]:
        """Cerca dataset per slug, sorgente, layer o testo.

        Args:
            query: Testo nello slug (case-insensitive, substring).
            layer: ``"clean"``, ``"mart"`` o ``None`` (entrambi).
            limit: Max risultati (default 15). Usa ``0`` per nessun limite.
            source: ``"gcs"``, ``"workspace"`` o ``"all"`` (default).
            stage: Filtro workspace: ``"candidates"``, ``"support"``, ``"all"`` (default).
            status_filter: Filtro run status: ``"SUCCESS"``, ``"FAILED"``, ecc.

        Returns:
            Dict con ``datasets`` (lista), ``total_count``, ``truncated``.
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

        # --- Source: GCS ---
        if source in ("gcs", "all"):
            manifest = self._load_gcs()
            gcs_configs = self._load_workspace_configs()
            for f in manifest.get("files", []):
                if not _is_data_parquet(f):
                    continue
                if not _matches_layer(f, layer):
                    continue
                slug = f["slug"]
                if slug is None:
                    continue
                if query and query.lower() not in slug.lower():
                    continue

                if slug not in datasets:
                    info = gcs_configs.get(slug, {})
                    datasets[slug] = self._empty_entry(slug, info)
                    datasets[slug]["_buckets"] = set()
                entry = datasets[slug]
                entry["_buckets"].add(f["bucket"])
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
                if query and query.lower() not in slug.lower():
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
    ) -> list[dict[str, Any]]:
        """Risolve uno slug nei file parquet corrispondenti.

        Cerca prima nel workspace locale, poi su GCS.
        """
        manifest = self._load_gcs()
        gcs_files = manifest.get("files", [])
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
            # year=None nel manifest = file con serie storica completa
            # mantienilo (il filtro anno va nell'SQL, non nel path)
            matching.append(f)

        if not matching:
            raise FileNotFoundError(
                f"Slug '{slug}' non trovato (layer={layer or 'any'}, year={year or 'any'})"
            )

        matching.sort(key=lambda f: (0 if _is_local(f) else 1, -(f["year"] or 9999), f["path"]))
        return matching

    def describe_slug(
        self,
        slug: str,
        layer: str = "clean",
        year: int | None = None,
    ) -> dict[str, Any]:
        """Schema DuckDB + row count per slug."""
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
            return result
        except Exception as exc:
            raise RuntimeError(
                f"Impossibile leggere schema per {slug} ({parquet_path}): {exc}"
            ) from exc
