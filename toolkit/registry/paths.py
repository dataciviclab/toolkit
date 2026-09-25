"""Path contract GCS per gli artifact registry.

Thin wrapper su ``lab_connectors.gcs.paths`` che aggiunge il supporto
ai layout ``year`` vs ``flat`` e la costruzione delle ``location`` dict.

Usage::

    from toolkit.registry.paths import PathContract

    contract = PathContract(prefix="eurostat", clean_layout="flat")
    url = contract.clean_parquet_url("gdp_nuts3", 2024)
    # → "gs://dataciviclab-clean/eurostat/gdp_nuts3/gdp_nuts3_2024_clean.parquet"
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from lab_connectors.gcs.paths import get_bucket, gs_url


@dataclass(frozen=True)
class PathContract:
    """Contratto di posizionamento GCS per un repo.

    Delega a ``lab_connectors.gcs.paths.gs_url`` per la costruzione URL,
    aggiungendo il toggle layout year/flat e le location dict.

    Args:
        prefix: Organizzazione nel bucket (es. ``"eurostat"``). Vuoto = root.
        clean_layout: ``"year"`` (default) o ``"flat"`` per il layer clean.
        mart_layout: ``"year"`` (default) o ``"flat"`` per il layer mart.
    """

    prefix: str = ""
    clean_layout: str = "year"
    mart_layout: str = "year"

    # ── Clean ──────────────────────────────────────────────────────────────

    def clean_parquet_url(self, slug: str, year: int) -> str:
        """URL gs:// del parquet clean per slug+anno."""
        # lab_connectors.gcs.paths.resolve richiede prefix con trailing slash
        prefix = f"{self.prefix}/" if self.prefix else ""
        if self.clean_layout == "flat":
            # Flat: year nel filename, non nella directory
            bucket = get_bucket("clean")
            return f"gs://{bucket}/{prefix}{slug}/{slug}_{year}_clean.parquet"
        return gs_url("clean", "clean_parquet", prefix=prefix, slug=slug, year=str(year))

    def clean_location(self, slug: str, years: list[int]) -> dict[str, Any]:
        """Location per il clean_catalog (pattern con wildcard se multi-file)."""
        prefix = f"{self.prefix}/" if self.prefix else ""
        if self.clean_layout == "flat":
            return {
                "type": "gcs",
                "path": gs_url("clean", "clean_parquet_flat", prefix=prefix, slug=slug),
                "multi_file": False,
            }

        multi = len(years) > 1
        if multi:
            from lab_connectors.gcs.paths import get_bucket

            bucket = get_bucket("clean")
            path = f"gs://{bucket}/{prefix}{slug}/*/{slug}_*_clean.parquet"
        else:
            path = gs_url("clean", "clean_parquet", prefix=prefix, slug=slug, year=str(years[0]))
        return {"type": "gcs", "path": path, "multi_file": multi}

    # ── Mart ───────────────────────────────────────────────────────────────

    def mart_parquet_url(self, dataset: str, table: str, year: int | None = None) -> str:
        """URL gs:// del parquet mart per dataset+tabella.

        ``year=None`` = mart multi-anno (years esplicite nel config): il
        runner lo scrive flat ``{dataset}/{table}.parquet`` (nessuna dir
        anno) → anche con ``mart_layout="year"`` l'URL è flat.
        """
        # lab_connectors.gcs.paths.resolve richiede prefix con trailing slash
        prefix = f"{self.prefix}/" if self.prefix else ""
        if self.mart_layout == "flat" or year is None:
            pattern = "mart_parquet_flat"
            kwargs = {"slug": dataset, "table": table}
        else:
            pattern = "mart_parquet"
            kwargs = {"slug": dataset, "year": str(year), "table": table}
        return gs_url("mart", pattern, prefix=prefix, **kwargs)

    def mart_location(self, dataset: str, table: str, year: int | None = None) -> dict[str, Any]:
        """Location per il mart_catalog."""
        return {
            "type": "gcs",
            "path": self.mart_parquet_url(dataset, table, year=year),
            "multi_file": False,
        }
