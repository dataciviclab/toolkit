"""Registry builder condiviso DataCivicLab.

Genera gli artifact registry del Lab — clean_catalog, pipeline_signals,
mart_catalog — riusando il runtime del toolkit (config model, path resolver,
run_state, parquet_schema) con layout e path contract parametrizzati, così ogni
repo (dataset-incubator, eurostat, dcl-bologna, ...) riusa la stessa logica.

Chiave canonica: ``dataset.name`` (underscore) — le directory del repo sono
solo contenitori per trovare il dataset.yml, mai identità del dataset.
"""

from toolkit.registry.layout import DatasetManifest, RepoLayout, iter_manifests, load_manifest
from toolkit.registry.paths import PathContract
from toolkit.registry.builders import (
    build_clean_catalog,
    build_mart_catalog,
    build_registry,
    build_signals,
)

__all__ = [
    "DatasetManifest",
    "PathContract",
    "RepoLayout",
    "build_clean_catalog",
    "build_mart_catalog",
    "build_registry",
    "build_signals",
    "iter_manifests",
    "load_manifest",
]
