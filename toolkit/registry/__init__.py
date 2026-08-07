"""Registry builder + lettore condiviso DataCivicLab.

Capacità unica del Lab per gli artifact registry:
- ``builders``: genera clean_catalog, mart_catalog, pipeline_signals, codelists
  (riusando il runtime del toolkit: config model, path resolver, run_state,
  parquet_schema) con layout e path contract parametrizzati.
- ``reader``: consulta gli artifact committati nei repo del workspace
  (CLI ``toolkit registry`` e MCP ``toolkit_registry_*``).

Chiave canonica: ``dataset.name`` (underscore) — le directory del repo sono
solo contenitori per trovare il dataset.yml, mai identità del dataset.
"""

from toolkit.registry.layout import DatasetManifest, RepoLayout, iter_manifests, load_manifest
from toolkit.registry.paths import PathContract
from toolkit.registry.reader import list_registries, show_registry
from toolkit.registry.builders import (
    build_clean_catalog,
    build_codelists,
    build_mart_catalog,
    build_registry,
    build_signals,
)

__all__ = [
    "DatasetManifest",
    "PathContract",
    "RepoLayout",
    "build_clean_catalog",
    "build_codelists",
    "build_mart_catalog",
    "build_registry",
    "build_signals",
    "iter_manifests",
    "list_registries",
    "load_manifest",
    "show_registry",
]
