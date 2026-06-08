"""SPARQL scout — named graph discovery, schema inference, lightweight probe.

Wrapper leggero su lab_connectors.http.sparql.
Mantiene le firme originali per backward compat con SO e altri caller.

Le funzioni di routing/orchestrazione stanno in toolkit.scout.probe.
"""

from __future__ import annotations

import logging
from typing import Any

from lab_connectors.http.sparql import discover_graphs, execute_sparql, infer_schema

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


def fetch_sparql_count(
    endpoint: str,
    graph_uri: str | None = None,
    timeout: int = 15,
) -> int | None:
    """Conta triple su un endpoint SPARQL, opzionalmente in un named graph.

    Usa SELECT (COUNT(*)) per verificare che l'endpoint risponda e
    restituire il numero di triple. Ritorna None se l'endpoint non
    risponde o la query fallisce.

    Args:
        endpoint: URL dell'endpoint SPARQL.
        graph_uri: URI del named graph (None = default graph).
        timeout: Timeout HTTP in secondi.

    Returns:
        Numero triple (int) o None se endpoint irraggiungibile.
    """
    if graph_uri:
        query = f"SELECT (COUNT(*) AS ?c) WHERE {{ GRAPH <{graph_uri}> {{ ?s ?p ?o }} }}"
    else:
        query = "SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o }"

    try:
        bindings = execute_sparql(endpoint, query, timeout=timeout)
        if bindings and len(bindings) > 0:
            val = bindings[0].get("c", {})
            if isinstance(val, dict) and "value" in val:
                raw = val["value"]
                if raw is not None:
                    return int(raw)
        return 0
    except (RuntimeError, ValueError, Exception) as exc:
        log.debug("SPARQL count failed on %s: %s", endpoint, exc)
        return None
