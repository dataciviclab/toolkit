"""Registry builder + lettore condiviso DataCivicLab.

Capacità unica del Lab per gli artifact registry:
- ``builders``: genera l'artifact registry UNICO ``registry.json`` (schema v2:
  sezioni datasets/marts/checks/codelists) riusando il runtime del toolkit:
  config model, path resolver, run_state, parquet_schema.
- ``reader``: consulta i registry committati nei repo del workspace
  (CLI ``toolkit registry`` e MCP ``toolkit_registry_*``).

Chiave canonica: ``dataset.name`` (underscore) — le directory del repo sono
solo contenitori per trovare il dataset.yml, mai identità del dataset.
"""

from toolkit.registry.layout import (
    DatasetManifest,
    RepoLayout,
    invalidate_workspace_cache,
    iter_manifests,
    load_manifest,
)
from toolkit.registry.paths import PathContract
from toolkit.registry.reader import list_registries, show_registry
from toolkit.registry.builders import (
    build_checks,
    build_clean_catalog,
    build_codelists,
    build_mart_catalog,
    build_registry,
)

__all__ = [
    "DatasetManifest",
    "PathContract",
    "RepoLayout",
    "build_checks",
    "build_clean_catalog",
    "build_codelists",
    "build_mart_catalog",
    "build_registry",
    "invalidate_workspace_cache",
    "iter_manifests",
    "list_registries",
    "load_manifest",
    "show_registry",
]
