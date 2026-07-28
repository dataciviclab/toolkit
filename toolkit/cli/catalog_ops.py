"""Catalog resolver condiviso CLI+MCP basato sul GCS manifest.

Legge ``gcs_manifest.json`` (auto-generato dalla CI in lab-connectors,
pubblicato su GCS) e scansiona il workspace locale per risolvere slug
di dataset ai loro path parquet, sia per clean che per mart.

Unifica due viste:
- **Remota**: dataset pubblicati su GCS (dal manifest)
- **Locale**: dataset in sviluppo nel workspace (dai clean parquet locali)
  — permette di usare ``toolkit_find`` e ``toolkit_dataset_overview`` anche
  prima del push su GCS

Usato da:
- CLI (futuro comando ``toolkit catalog ...``)
- MCP ``toolkit_find`` e ``toolkit_dataset_overview`` (via ``mcp/catalog_ops.py``)

Le funzioni qui NON gestiscono errori MCP (ToolkitClientError) — quelle
vanno aggiunte nei wrapper MCP. Le funzioni qui sollevano eccezioni
Python standard (ValueError, FileNotFoundError, RuntimeError).
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from lab_connectors.gcs.manifest import MANIFEST_URL, read_manifest

from toolkit.core.duckdb_shape import parquet_preview

# ---------------------------------------------------------------------------
# Costanti
# ---------------------------------------------------------------------------

CLEAN_BUCKET = "dataciviclab-clean"
MART_BUCKET = "dataciviclab-mart"
VALID_LAYERS = frozenset({"clean", "mart"})

# Ricaviamo il workspace root come fa path_safety.py
_TOOLKIT_ROOT = Path(__file__).resolve().parents[2]
WORKSPACE_ROOT = Path(
    os.environ.get("DATACIVICLAB_WORKSPACE", str(_TOOLKIT_ROOT.parent))
).expanduser()

LOCAL_BUCKET = "local"  # bucket fittizio per file locali


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _is_data_parquet(file: dict[str, Any]) -> bool:
    """True se il file e' un parquet dati (non pipeline_run.json o altri JSON).

    I file dati hanno estensione .parquet.
    I file di servizio (pipeline_run.json, metadata.json, ecc.) sono esclusi.
    """
    return file["path"].endswith(".parquet")


def _matches_layer(file: dict[str, Any], layer: str | None) -> bool:
    """True se il file appartiene al layer richiesto.

    Args:
        file: Entry del manifest/manifest-like.
        layer: ``"clean"``, ``"mart"`` o ``None`` (tutti).

    Regole:
    - Se layer=None, qualsiasi file passa.
    - Se layer="clean": bucket=dataciviclab-clean oppure bucket=local.
    - Se layer="mart": bucket=dataciviclab-mart.
    - I file locali sono sempre clean (bucket="local").
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
    """Estrae slug e anno da un filename ``{slug}_{year}_clean.parquet``.

    Returns:
        ``(slug, year)`` o ``None`` se il nome non corrisponde al pattern.
    """
    if not filename.endswith("_clean.parquet"):
        return None
    stem = filename[: -len(".parquet")]  # toglie .parquet
    if not stem.endswith("_clean"):
        return None
    prefix = stem[: -len("_clean")]  # toglie _clean
    *slug_parts, year_str = prefix.rsplit("_", 1)
    if not slug_parts or not slug_parts[0]:
        return None
    if year_str.isdigit() and len(year_str) == 4:
        return ("_".join(slug_parts), int(year_str))
    return None


def _scan_workspace(workspace: Path = WORKSPACE_ROOT) -> list[dict[str, Any]]:
    """Scansiona il workspace locale per clean parquet.

    Cerca file ``*_clean.parquet`` sotto ``dataset-incubator/``,
    estrae slug e anno dal filename.

    Returns:
        Lista di entry in formato compatibile con il manifest GCS,
        con ``_local=True`` aggiunto.
    """
    incubator = workspace / "dataset-incubator"
    if not incubator.is_dir():
        return []

    entries: list[dict[str, Any]] = []
    seen: set[tuple[str, int, str]] = set()  # (slug, year, path) per dedup

    for fpath in incubator.rglob("*_clean.parquet"):
        parsed = _parse_clean_filename(fpath.name)
        if parsed is None:
            continue
        slug, year = parsed

        # Path relativo al workspace per consistenza
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


def _merge_entries(
    gcs_files: list[dict[str, Any]],
    local_files: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Fonde entry GCS e locali.

    Regole:
    - Stesso slug + stesso anno + stesso bucket: preferisce locale
      (sostituisce l'entry GCS con quella locale).
    - Stesso slug + stesso anno + bucket diverso: tiene entrambi.
    - Stesso slug + anno diverso: tiene entrambi.

    Returns:
        Lista unificata di entry.
    """
    # Costruiamo un set di (slug, year, bucket) dalle entry locali per match rapido
    local_keys: set[tuple[str, int | None, str]] = set()
    for lf in local_files:
        local_keys.add((lf["slug"], lf["year"], lf["bucket"]))

    merged = list(local_files)  # locali sempre inclusi

    for gf in gcs_files:
        key = (gf["slug"], gf["year"], gf["bucket"])
        if key in local_keys:
            # Gia' coperto da entry locale, skip
            continue
        merged.append(gf)

    return merged


# ---------------------------------------------------------------------------
# Resolver
# ---------------------------------------------------------------------------


class CatalogResolver:
    """Risolve slug dataset → metadati parquet su GCS e workspace locale.

    Unifica due fonti:
    - ``gcs_manifest.json`` su GCS (dataset pubblicati)
    - Scansione del workspace ``dataset-incubator/`` (dataset in sviluppo)

    La risoluzione preferisce i file locali quando disponibili
    (sono la versione piu' recente in sviluppo).
    """

    def __init__(
        self,
        manifest_url: str | None = None,
        workspace: str | Path | None = None,
        include_local: bool = True,
    ) -> None:
        """Init.

        Args:
            manifest_url: URL del manifest GCS (default: MANIFEST_URL).
            workspace: Path del workspace (default: da env o toolkit parent).
            include_local: Se ``True``, include anche i dataset locali.
        """
        self._manifest_url = manifest_url or MANIFEST_URL
        self._workspace = Path(workspace or WORKSPACE_ROOT)
        self._include_local = include_local
        self._gcs_manifest: dict[str, Any] | None = None
        self._local_entries: list[dict[str, Any]] | None = None

    def _load_gcs(self) -> dict[str, Any]:
        """Carica (o ricarica) il manifest da GCS."""
        self._gcs_manifest = read_manifest(self._manifest_url)
        return self._gcs_manifest

    def _load_local(self) -> list[dict[str, Any]]:
        """Scansiona il workspace locale (con cache sul resolver)."""
        if self._local_entries is not None:
            return self._local_entries
        if not self._include_local:
            self._local_entries = []
            return self._local_entries
        self._local_entries = _scan_workspace(self._workspace)
        return self._local_entries

    def _get_files(self) -> list[dict[str, Any]]:
        """Restituisce i file unificati (GCS + locali)."""
        manifest = self._load_gcs()
        gcs_files = manifest.get("files", [])
        local_files = self._load_local()
        return _merge_entries(gcs_files, local_files)

    # ------------------------------------------------------------------
    # Pubblici
    # ------------------------------------------------------------------

    def list_datasets(
        self,
        query: str = "",
        layer: str | None = None,
        limit: int = 15,
    ) -> dict[str, Any]:
        """Cerca dataset per slug, bucket o testo.

        Cerca sia su GCS (pubblicati) che nel workspace (in sviluppo).

        Args:
            query: Testo da cercare nello slug (case-insensitive, substring).
                Vuoto = tutti i dataset.
            layer: ``"clean"``, ``"mart"`` o ``None`` (entrambi).
            limit: Max risultati (default 15). Usa ``0`` per nessun limite.

        Returns:
            Dict con:
            - ``datasets``: lista di dict (slug, layer, years, file_count,
              total_size_bytes, has_local, has_remote).
            - ``total_count``: numero totale di risultati (prima del limit).
            - ``truncated``: ``True`` se il limit ha tagliato risultati.
            Ordinati per slug.
        """
        files = self._get_files()

        # Raggruppa per slug
        datasets: dict[str, dict[str, Any]] = {}
        for f in files:
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
                datasets[slug] = {
                    "slug": slug,
                    "layer": layer or "clean",  # placeholder, sovrascritto sotto
                    "buckets": set(),
                    "years": set(),
                    "file_count": 0,
                    "total_size_bytes": 0,
                    "has_local": False,
                    "has_remote": False,
                }
            entry = datasets[slug]
            entry["buckets"].add(f["bucket"])
            entry["years"].add(f["year"])
            entry["file_count"] += 1
            entry["total_size_bytes"] += f.get("size_bytes", 0)
            if _is_local(f):
                entry["has_local"] = True
            else:
                entry["has_remote"] = True

        # Converte set → lista ordinata, calcola layer effettivo
        result = []
        for slug in sorted(datasets):
            entry = datasets[slug]
            entry["years"] = sorted(entry["years"])
            buckets = entry.pop("buckets")
            if layer:
                entry["layer"] = layer
            else:
                # Se ha locale, il layer effettivo considera anche LOCAL_BUCKET
                has_clean = CLEAN_BUCKET in buckets or LOCAL_BUCKET in buckets
                has_mart = MART_BUCKET in buckets
                if has_clean and has_mart:
                    entry["layer"] = "clean,mart"
                elif has_clean:
                    entry["layer"] = "clean"
                else:
                    entry["layer"] = "mart"
            result.append(entry)

        total_count = len(result)
        truncated = bool(limit and total_count > limit)
        if limit and total_count > limit:
            result = result[:limit]

        return {
            "datasets": result,
            "total_count": total_count,
            "truncated": truncated,
        }

    def resolve_slug(
        self,
        slug: str,
        layer: str | None = None,
        year: int | None = None,
    ) -> list[dict[str, Any]]:
        """Risolve uno slug nei file parquet corrispondenti.

        Cerca prima nel workspace locale (versione in sviluppo),
        poi su GCS (pubblicato). I file locali hanno priorita'.

        Args:
            slug: Slug del dataset (es. ``anac_bandi_gara``).
            layer: ``"clean"``, ``"mart"`` o ``None`` (entrambi).
            year: Anno specifico o ``None`` (tutti).

        Returns:
            Lista di file dict (url, slug, bucket, year, path, size_bytes, updated).
            Ordinati per anno discendente, poi per path.

        Raises:
            FileNotFoundError: se lo slug non esiste ne' in locale ne' su GCS.
        """
        files = self._get_files()

        matching = []
        for f in files:
            if not _is_data_parquet(f):
                continue
            if not _matches_layer(f, layer):
                continue
            if f["slug"] != slug:
                continue
            if year is not None and f["year"] != year:
                continue
            matching.append(f)

        if not matching:
            raise FileNotFoundError(
                f"Slug '{slug}' non trovato (layer={layer or 'any'}, year={year or 'any'})"
            )

        # Ordina: locali prima (priorita'), poi anno discendente, poi path
        matching.sort(key=lambda f: (0 if _is_local(f) else 1, -(f["year"] or 0), f["path"]))
        return matching

    def describe_slug(
        self,
        slug: str,
        layer: str = "clean",
        year: int | None = None,
    ) -> dict[str, Any]:
        """Schema DuckDB + row count per uno slug.

        Usa DuckDB DESCRIBE sul primo parquet trovato (ultimo anno
        disponibile, o anno specificato). Se il dataset esiste in locale,
        descrive il parquet locale (priorita').

        Args:
            slug: Slug del dataset.
            layer: ``"clean"`` (default) o ``"mart"``.
            year: Anno specifico o ``None`` (ultimo disponibile).

        Returns:
            Dict con slug, year, layer, columns, row_count, preview.

        Raises:
            FileNotFoundError: se lo slug non esiste.
            RuntimeError: se DuckDB non riesce a leggere il parquet.
        """
        files = self.resolve_slug(slug, layer=layer, year=year)
        if not files:
            raise FileNotFoundError(f"Slug '{slug}' non trovato (year={year})")

        # Prende il primo (locale se disponibile, poi ultimo anno)
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
