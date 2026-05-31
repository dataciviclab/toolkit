"""SPARQL scout — named graph discovery e schema inference.

Wrapper leggero su lab_connectors.http.sparql.
Mantiene le firme originali per backward compat con SO e altri caller.

Le funzioni di routing/orchestrazione stanno in toolkit.scout.probe.
"""

from __future__ import annotations

import logging
from typing import Any

from lab_connectors.http.sparql import (
    discover_graphs,
    execute_sparql,
    infer_schema,
)

log = logging.getLogger(__name__)


def discover_named_graphs(
    endpoint: str,
    timeout: int = 60,
    prefix: str = "",
    blacklist: list[str] | None = None,
) -> list[str]:
    """Alias per lab_connectors.http.sparql.discover_graphs."""
    return discover_graphs(
        endpoint=endpoint,
        timeout=timeout,
        prefix=prefix,
        blacklist=blacklist,
    )


def infer_graph_schema(
    endpoint: str,
    graph_uri: str,
    timeout: int = 60,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Alias per lab_connectors.http.sparql.infer_schema."""
    return infer_schema(
        endpoint=endpoint,
        graph_uri=graph_uri,
        timeout=timeout,
        limit=limit,
    )
