"""Path contract GCS per gli artifact registry.

Estende il contratto di ``lab_connectors.gcs.paths`` (bucket names) con due
layout per layer:

- ``year``: ``{prefix}/{slug}/{year}/{file}`` — layout dataset-incubator.
- ``flat``: ``{prefix}/{slug}/{file}`` — layout no-year, es. eurostat.

``prefix`` è l'organizzazione nel bucket (es. ``eurostat``); vuoto per i
dataset a root bucket (layout DI). La chiave canonica è sempre lo slug
(``dataset.name``, underscore).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from lab_connectors.gcs.paths import MART_BUCKET, CLEAN_BUCKET


@dataclass(frozen=True)
class PathContract:
    """Contratto di posizionamento GCS per un repo.

    Args:
        prefix: Organizzazione nel bucket (es. ``"eurostat"``). Vuoto = root.
        clean_layout: ``"year"`` (default) o ``"flat"`` per il layer clean.
        mart_layout: ``"year"`` (default) o ``"flat"`` per il layer mart.
    """

    prefix: str = ""
    clean_layout: str = "year"
    mart_layout: str = "year"

    def _prefixed(self, bucket: str) -> str:
        return f"gs://{bucket}/{self.prefix}" if self.prefix else f"gs://{bucket}"

    # ── Clean ──────────────────────────────────────────────────────────────

    def clean_parquet_url(self, slug: str, year: int) -> str:
        """URL gs:// del parquet clean per slug+anno."""
        base = self._prefixed(CLEAN_BUCKET)
        if self.clean_layout == "flat":
            return f"{base}/{slug}/{slug}_{year}_clean.parquet"
        return f"{base}/{slug}/{year}/{slug}_{year}_clean.parquet"

    def clean_location(self, slug: str, years: list[int]) -> dict[str, Any]:
        """Location per il clean_catalog (pattern con wildcard se multi-file)."""
        if self.clean_layout == "flat":
            return {
                "type": "gcs",
                "path": f"{self._prefixed(CLEAN_BUCKET)}/{slug}/",
                "multi_file": False,
            }
        multi = len(years) > 1
        path = (
            f"{self._prefixed(CLEAN_BUCKET)}/{slug}/*/{slug}_*_clean.parquet"
            if multi
            else self.clean_parquet_url(slug, years[0])
        )
        return {"type": "gcs", "path": path, "multi_file": multi}

    # ── Mart ───────────────────────────────────────────────────────────────

    def mart_parquet_url(self, dataset: str, table: str, year: int | None = None) -> str:
        """URL gs:// del parquet mart per dataset+tabella.

        ``year=None`` = mart multi-anno (years esplicite nel config): il
        runner lo scrive flat ``{dataset}/{table}.parquet`` (nessuna dir
        anno) → anche con ``mart_layout="year"`` l'URL è flat.
        """
        base = self._prefixed(MART_BUCKET)
        if self.mart_layout == "flat" or year is None:
            return f"{base}/{dataset}/{table}.parquet"
        return f"{base}/{dataset}/{year}/{table}.parquet"

    def mart_location(self, dataset: str, table: str, year: int | None = None) -> dict[str, Any]:
        """Location per il mart_catalog."""
        return {
            "type": "gcs",
            "path": self.mart_parquet_url(dataset, table, year=year),
            "multi_file": False,
        }
