"""Catalog resolver condiviso CLI+MCP basato sul GCS manifest.

Legge ``gcs_manifest.json`` (auto-generato dalla CI in lab-connectors,
pubblicato su GCS) per risolvere slug di dataset ai loro path parquet
su GCS, sia per clean che per mart.

Usato da:
- CLI (futuro comando ``toolkit catalog ...``)
- MCP ``toolkit_find`` e ``toolkit_dataset_overview`` (via ``mcp/catalog_ops.py``)

Le funzioni qui NON gestiscono errori MCP (ToolkitClientError) — quelle
vanno aggiunte nei wrapper MCP. Le funzioni qui sollevano eccezioni
Python standard (ValueError, FileNotFoundError, RuntimeError).
"""

from __future__ import annotations

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
        file: Entry del manifest.
        layer: ``"clean"``, ``"mart"`` o ``None`` (tutti).

    Regole:
    - Se layer=None, qualsiasi file passa.
    - Se layer="clean": bucket=dataciviclab-clean.
    - Se layer="mart": bucket=dataciviclab-mart.
    """
    if layer is None:
        return True
    if layer == "clean":
        return file["bucket"] == CLEAN_BUCKET
    if layer == "mart":
        return file["bucket"] == MART_BUCKET
    return False


# ---------------------------------------------------------------------------
# Resolver
# ---------------------------------------------------------------------------


class CatalogResolver:
    """Risolve slug dataset → metadati parquet su GCS usando il manifest.

    Legge il ``gcs_manifest.json`` pubblicato su GCS, filtra i file parquet
    dati e fornisce metodi di ricerca e risoluzione.

    Il resolver non fa caching interno — il chiamante (CLI o MCP) gestisce
    la frequenza di refresh.
    """

    def __init__(self, manifest_url: str | None = None) -> None:
        """Init.

        Args:
            manifest_url: URL del manifest (default: MANIFEST_URL).
        """
        self._manifest_url = manifest_url or MANIFEST_URL
        self._manifest: dict[str, Any] | None = None

    def _load(self) -> dict[str, Any]:
        """Carica (o ricarica) il manifest da GCS.

        Returns:
            Il manifest completo.

        Raises:
            FileNotFoundError: se il manifest non e' raggiungibile.
            ValueError: se il JSON e' malformato.
            TimeoutError: se la richiesta scade.
        """
        self._manifest = read_manifest(self._manifest_url)
        return self._manifest

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

        Args:
            query: Testo da cercare nello slug (case-insensitive, substring).
                Vuoto = tutti i dataset.
            layer: ``"clean"``, ``"mart"`` o ``None`` (entrambi).
            limit: Max risultati (default 15). Usa ``0`` per nessun limite.

        Returns:
            Dict con:
            - ``datasets``: lista di dict (slug, layer, years, file_count, total_size_bytes).
            - ``total_count``: numero totale di risultati (prima del limit).
            - ``truncated``: ``True`` se il limit ha tagliato risultati.
            Ordinati per slug.
        """
        manifest = self._load()
        files = manifest.get("files", [])

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
                }
            entry = datasets[slug]
            entry["buckets"].add(f["bucket"])
            entry["years"].add(f["year"])
            entry["file_count"] += 1
            entry["total_size_bytes"] += f.get("size_bytes", 0)

        # Converte set → lista ordinata, calcola layer effettivo
        result = []
        for slug in sorted(datasets):
            entry = datasets[slug]
            entry["years"] = sorted(entry["years"])
            buckets = entry.pop("buckets")
            # layer reale dai bucket effettivi in cui lo slug appare
            if layer:
                entry["layer"] = layer
            else:
                has_clean = CLEAN_BUCKET in buckets
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

        Args:
            slug: Slug del dataset (es. ``anac_bandi_gara``).
            layer: ``"clean"``, ``"mart"`` o ``None`` (entrambi).
            year: Anno specifico o ``None`` (tutti).

        Returns:
            Lista di file dict (url, slug, bucket, year, path, size_bytes, updated).
            Ordinati per anno discendente, poi per path.

        Raises:
            FileNotFoundError: se lo slug non esiste nel manifest.
        """
        manifest = self._load()
        files = manifest.get("files", [])

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
                f"Slug '{slug}' non trovato nel manifest "
                f"(layer={layer or 'any'}, year={year or 'any'})"
            )

        # Ordina: anno discendente, poi path
        matching.sort(key=lambda f: (-(f["year"] or 0), f["path"]))
        return matching

    def describe_slug(
        self,
        slug: str,
        layer: str = "clean",
        year: int | None = None,
    ) -> dict[str, Any]:
        """Schema DuckDB + row count per uno slug.

        Usa DuckDB DESCRIBE sul primo parquet trovato (ultimo anno
        disponibile, o anno specificato). Di default descrive il layer
        clean (dato normalizzato). Passa ``layer="mart"`` per le aggregazioni.

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

        # Prende il primo (ultimo anno, ordinato da resolve_slug)
        target = files[0]
        parquet_path = Path(target["url"])
        actual_year = target["year"]

        try:
            result = parquet_preview(parquet_path, limit=5)
            result["slug"] = slug
            result["year"] = actual_year
            result["layer"] = layer
            return result
        except Exception as exc:
            raise RuntimeError(
                f"Impossibile leggere schema per {slug} ({parquet_path}): {exc}"
            ) from exc
