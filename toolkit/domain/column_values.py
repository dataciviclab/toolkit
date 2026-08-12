"""Profilo valori delle colonne dimensionali di un parquet clean.

Per ogni colonna con ``role=dimension`` calcola gli aggregati di valore:
cardinalita' (n_distinct, distinct_ratio), null (n_null) e top-N valori
con conteggio e percentuale. Output: dict JSON-serializzabile.

I/O solo DuckDB (lettura parquet). Nessun side-effect su file.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from lab_connectors.duckdb import safe_connect
from toolkit.core.io import write_json_atomic
from toolkit.core.sql_utils import q_ident, sql_path


def _dimension_columns(columns: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [c for c in columns if c.get("role") == "dimension"]


def _json_safe_value(value: Any) -> Any:
    """Converte valori non-JSON (date, timestamp, ...) in stringa."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _build_summary_sql(parquet: Path, dims: list[dict[str, Any]]) -> str:
    """SQL unico: n_rows + n_distinct (approx) + n_null per colonna dimensionale.

    ``APPROX_COUNT_DISTINCT`` (HyperLogLog) tiene la memoria costante anche su
    dataset grandi con molte dimensioni (es. anac_bandi_gara: 1.2M righe x 57
    dimensioni): il COUNT(DISTINCT) esatto farebbe OOM nel memory limit 2GB.
    Errore tipico ~1.6%; sufficiente per cardinalita' e distinct_ratio.
    """
    parts: list[str] = ["SELECT COUNT(*) AS n_rows"]
    for col in dims:
        ident = q_ident(col["name"])
        alias_distinct = q_ident(f"d_{col['name']}")
        alias_null = q_ident(f"nn_{col['name']}")
        parts.append(f"  ,APPROX_COUNT_DISTINCT({ident}) AS {alias_distinct}")
        parts.append(f"  ,SUM(CASE WHEN {ident} IS NULL THEN 1 ELSE 0 END) AS {alias_null}")
    parts.append(f"FROM read_parquet('{sql_path(parquet)}')")
    return "\n".join(parts)


def _build_top_sql(parquet: Path, col_name: str, top_n: int) -> str:
    ident = q_ident(col_name)
    return (
        f"SELECT {ident} AS value, COUNT(*) AS n\n"
        f"FROM read_parquet('{sql_path(parquet)}')\n"
        f"WHERE {ident} IS NOT NULL\n"
        f"GROUP BY {ident}\n"
        f"ORDER BY n DESC, value\n"
        f"LIMIT {int(top_n)}"
    )


def build_column_values_profile(
    parquet: Path,
    columns: list[dict[str, Any]],
    *,
    top_n: int = 20,
) -> dict[str, Any]:
    """Aggregati di valore per le colonne dimensionali di un parquet clean.

    Args:
        parquet: Path al parquet clean.
        columns: Colonne del catalogo (``name``, ``type``, ``role``,
            ``semantic_type`` opzionale) — es. da ``registry.schema_reader``.
        top_n: Numero massimo di top values per colonna (default 20).

    Returns:
        Dict JSON-serializzabile con sezioni ``n_rows`` e ``columns``
        (una entry per colonna dimensionale: type, role, n_null, n_distinct,
        distinct_ratio, top_values, top_truncated).
    """
    dims = _dimension_columns(columns)
    result: dict[str, Any] = {
        "n_rows": None,
        "n_columns_profiled": len(dims),
        "count_distinct_mode": "approx",
        "columns": {},
    }

    if not dims:
        return result

    summary_sql = _build_summary_sql(parquet, dims)
    with safe_connect() as con:
        summary = con.execute(summary_sql).fetchone()
        summary_names = [d[0] for d in con.description]

        n_rows = int(summary[summary_names.index("n_rows")])
        result["n_rows"] = n_rows

        for col in dims:
            name = col["name"]
            d_idx = summary_names.index(f"d_{name}")
            nn_idx = summary_names.index(f"nn_{name}")
            n_distinct = int(summary[d_idx]) if summary[d_idx] is not None else 0
            n_null = int(summary[nn_idx]) if summary[nn_idx] is not None else 0

            entry: dict[str, Any] = {
                "type": col.get("type"),
                "role": col.get("role"),
            }
            if col.get("semantic_type"):
                entry["semantic_type"] = col["semantic_type"]
            entry["n_null"] = n_null
            entry["n_distinct"] = n_distinct
            entry["distinct_ratio"] = round(n_distinct / n_rows, 6) if n_rows else None

            rows = con.execute(_build_top_sql(parquet, name, top_n)).fetchall()
            top_values = [{"value": _json_safe_value(r[0]), "n": int(r[1])} for r in rows]
            for tv in top_values:
                tv["pct"] = round(tv["n"] * 100.0 / n_rows, 2) if n_rows else None
            entry["top_values"] = top_values
            entry["top_truncated"] = len(top_values) >= top_n

            result["columns"][name] = entry

    return result


# ---------------------------------------------------------------------------
# Batch su workspace (discovery + generazione)
# ---------------------------------------------------------------------------


@dataclass
class WorkspaceProfileResult:
    """Esito della generazione batch su tutti i repo del workspace."""

    processed: int = 0
    written: int = 0
    skipped: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    duration_seconds: float = 0.0


def generate_workspace_column_values(
    workspace_root: Path,
    out_dir: Path,
    *,
    top_n: int = 20,
    latest_year_only: bool = True,
    log: Any | None = None,
) -> WorkspaceProfileResult:
    """Genera i profili valori per tutti i dataset clean locali del workspace.

    Scopre i repo dati via ``repo_dataset_dirs`` + ``iter_manifests`` e per
    ogni manifest risolve l'ultimo anno con parquet clean locale. Output:
    ``<out_dir>/<slug>__column_values.json`` (uno per dataset).

    Args:
        workspace_root: Root del workspace (contiene i repo dati).
        out_dir: Directory di output (creata se mancante).
        top_n: Top values per colonna.
        latest_year_only: True = solo ultimo anno con parquet; False = tutti
            gli anni con parquet (nome file ``<slug>__<year>__column_values.json``).
        log: Logger opzionale (usa ``print`` se None).

    Returns:
        Esito (processed/written/skipped/errors/duration).
    """
    from toolkit.registry.layout import RepoLayout, iter_manifests, repo_dataset_dirs
    from toolkit.registry.schema_reader import (
        clean_parquet_path,
        load_semantic_types,
        parquet_columns,
    )

    def _log(msg: str) -> None:
        if log is not None:
            log(msg)
        else:
            print(msg)

    alias_map = load_semantic_types()
    result = WorkspaceProfileResult()
    started = datetime.now(UTC)
    out_dir.mkdir(parents=True, exist_ok=True)
    written_slugs: set[str] = set()

    repos = sorted(p for p in workspace_root.iterdir() if p.is_dir())
    for repo_dir in repos:
        sections = repo_dataset_dirs(repo_dir)
        if not sections:
            continue
        layout = RepoLayout(repo_root=repo_dir, dataset_dirs=sections)
        for manifest in iter_manifests(layout):
            result.processed += 1
            slug = manifest.slug
            if slug in written_slugs:
                # Stesso slug presente in piu' repo (es. mirror dedicato):
                # il primo repo in ordine alfabetico vince, il duplicato
                # e' escluso per non sovrascrivere lo stesso file.
                result.skipped.append(f"{slug} (duplicato in {repo_dir.name})")
                continue
            years = sorted(manifest.years, reverse=True)
            if latest_year_only:
                # Usa l'anno configurato più alto che ha un parquet clean locale.
                selected = next(
                    (y for y in years if clean_parquet_path(manifest.cfg, y) is not None), None
                )
                if selected is None:
                    result.skipped.append(f"{slug} (nessun clean locale)")
                    continue
                years = [selected]
            for year in years:
                parquet = clean_parquet_path(manifest.cfg, year)
                if parquet is None:
                    result.skipped.append(f"{slug}:{year} (no clean locale)")
                    continue
                try:
                    columns = parquet_columns(parquet, alias_map)
                    if not columns:
                        result.skipped.append(f"{slug}:{year} (schema non leggibile)")
                        continue
                    profile = build_column_values_profile(parquet, columns, top_n=top_n)
                    profile.update(
                        {
                            "schema_version": 1,
                            "dataset": slug,
                            "year": year,
                            "config_path": str(manifest.yml_path),
                            "parquet": str(parquet),
                        }
                    )
                    fname = (
                        f"{slug}__column_values.json"
                        if latest_year_only
                        else f"{slug}__{year}__column_values.json"
                    )
                    out_path = out_dir / fname
                    write_json_atomic(out_path, profile)
                    result.written += 1
                    written_slugs.add(slug)
                except Exception as exc:  # noqa: BLE001 — un errore non blocca il batch
                    result.errors.append(f"{slug}:{year}: {exc}")

    result.duration_seconds = round((datetime.now(UTC) - started).total_seconds(), 2)
    return result
