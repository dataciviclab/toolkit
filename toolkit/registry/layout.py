"""Layout repo e lettura dataset.yml.

La lettura del dataset.yml è delegata al config model del toolkit
(``toolkit.core.config.load_config`` + ``load_dataset_manifest``): qui solo
navigazione del filesystem (dove stanno i dataset.yml) e normalizzazione dei
campi che servono agli artifact registry.

Chiave canonica di un dataset: ``dataset.name`` (underscore). La directory che
contiene il dataset.yml è solo un contenitore (es. ``datasets/eurostat-gdp-nuts3``)
e non è MAI usata come identità — path locali, run records e GCS usano il name.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

from toolkit.core.config import load_config
from toolkit.core.dataset_loader import load_dataset_manifest

DEFAULT_DATASET_DIRS: tuple[str, ...] = ("datasets",)


@dataclass(frozen=True)
class RepoLayout:
    """Struttura dichiarativa di un repo con dataset.yml.

    Args:
        repo_root: Root del repo.
        dataset_dirs: Dir (relative a repo_root) che contengono i dataset.yml
            (es. ``("datasets",)`` per eurostat; ``("candidates", "compose",
            "support_datasets")`` per dataset-incubator).
        source_repo: Identità del repo per gli artifact
            (es. ``dataciviclab/eurostat``).
    """

    repo_root: Path
    dataset_dirs: tuple[str, ...] = DEFAULT_DATASET_DIRS
    source_repo: str = ""

    def iter_dataset_ymls(self) -> Iterator[Path]:
        """Itera i dataset.yml presenti nelle dir configurate.

        Un solo livello di profondità (es. ``datasets/{entry}/dataset.yml``).
        Le entry senza dataset.yml sono ignorate.
        """
        for section in self.dataset_dirs:
            section_dir = self.repo_root / section
            if not section_dir.is_dir():
                continue
            for entry_dir in sorted(section_dir.iterdir()):
                yml = entry_dir / "dataset.yml"
                if yml.is_file():
                    yield yml


@dataclass(frozen=True)
class DatasetManifest:
    """Vista normalizzata di un dataset.yml (config model del toolkit).

    ``cfg`` è il ``PipelineConfig`` completo: serve ai reader (path resolver,
    run_state) che lavorano sul contratto del toolkit.
    """

    slug: str
    yml_path: Path
    base_dir: Path
    cfg: Any = None
    years: tuple[int, ...] = ()
    source_id: str = ""
    tags: tuple[str, ...] = ()
    category: str = ""
    time_coverage: dict[str, Any] = field(default_factory=dict)
    mart_tables: tuple[dict[str, Any], ...] = field(default_factory=tuple)
    mart_rules: dict[str, dict[str, Any]] = field(default_factory=dict)
    is_mart_only: bool = False
    has_clean_sql: bool = False

    @property
    def period(self) -> dict[str, int] | None:
        tc = self.time_coverage
        if tc and "start_year" in tc and "end_year" in tc:
            return {"start": int(tc["start_year"]), "end": int(tc["end_year"])}
        return None


def load_manifest(yml_path: Path) -> DatasetManifest | None:
    """Carica un dataset.yml via il config model del toolkit.

    Ritorna None se il config non è leggibile o non ha ``dataset.name``.
    """
    try:
        cfg = load_config(str(yml_path), strict_config=False)
    except Exception:
        return None
    if not cfg.dataset:
        return None

    try:
        extra = load_dataset_manifest(str(yml_path)) or {}
    except Exception:
        extra = {}

    tables = tuple({"name": t.name, "sql": t.sql} for t in cfg.mart.tables if t.name)
    rules: dict[str, dict[str, Any]] = {}
    validate = cfg.mart.validate
    if validate is not None and validate.table_rules:
        for name, rule in validate.table_rules.items():
            entry: dict[str, Any] = {}
            if rule.required_columns:
                entry["required_columns"] = list(rule.required_columns)
            if rule.primary_key:
                entry["primary_key"] = list(rule.primary_key)
            if rule.min_rows is not None:
                entry["min_rows"] = rule.min_rows
            rules[name] = entry

    return DatasetManifest(
        slug=cfg.dataset,
        yml_path=yml_path,
        base_dir=yml_path.parent,
        cfg=cfg,
        years=tuple(sorted(cfg.years)),
        source_id=cfg.source_id or "",
        tags=tuple(cfg.tags or []),
        category=cfg.category or "",
        time_coverage=extra.get("time_coverage") or {},
        mart_tables=tables,
        mart_rules=rules,
        is_mart_only=cfg.is_mart_only,
        has_clean_sql=bool(cfg.clean.sql),
    )


def iter_manifests(layout: RepoLayout) -> Iterator[DatasetManifest]:
    """Itera i manifest validi del layout (con ``dataset.name``)."""
    for yml in layout.iter_dataset_ymls():
        manifest = load_manifest(yml)
        if manifest is not None:
            yield manifest
