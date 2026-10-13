"""SPARQL scout — lightweight count probe su endpoint.

Wrapper leggero su lab_connectors.http.sparql. Le funzioni di
routing/orchestrazione stanno in toolkit.scout.probe.
"""

from __future__ import annotations

import logging

from lab_connectors.http.sparql import execute_sparql

log = logging.getLogger(__name__)


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
