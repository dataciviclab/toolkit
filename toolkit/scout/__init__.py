"""toolkit.scout — URL probe, routing, inferenze per candidate dataset.

Strato indipendente dalla CLI, usabile da:
- CLI (scout, scout --scaffold)
- MCP tools (scout tools)
- Notebook e script interni

Moduli:
  http           → HTTP transport: probe, fetch, CKAN/SDMX fetch
  link_extractor → Shim: re-export da lab_connectors.discovery.links
  sparql         → SPARQL scout: alias su lab_connectors.http.sparql
  infer          → Inferenze: re-export da LC + toolkit-specifiche (suggest_years, infer_topics)
  probe          → Orchestrazione: probe_url(), probe_url_routed()

Le primitive pure (link extraction, classificazione) sono ora in
``lab_connectors.discovery``. Questo modulo mantiene l'orchestrazione
e le funzioni HTTP-specifiche che dipendono da toolkit.plugins.

Scaffold non e' piu' in scout. Usa toolkit.scaffold.full e toolkit.scaffold.sources.
"""

from toolkit.scout.sparql import (  # noqa: F401
    discover_named_graphs,
    fetch_sparql_count,
    infer_graph_schema,
)

from toolkit.scout.http import (  # noqa: F401
    CANDIDATE_EXTENSIONS,
    DEFAULT_TIMEOUT,
    DEFAULT_USER_AGENT,
    EXTENDED_EXTENSIONS,
    detect_ckan_in_html,
    discover_ckan_resources,
    extract_ckan_dataset_id,
    fetch_ckan_datastore_schema,
    fetch_ckan_package,
    fetch_content,
    fetch_html_body,
    fetch_sdmx_years,
    is_file_like,
    is_html_content,
    is_sdmx_url,
    probe_url_headers,
    resolve_preview_kind,
)

from toolkit.scout.link_extractor import (  # noqa: F401
    DataLink,
    LinkGroup,
    extract_candidate_links,
    extract_data_links,
    group_links,
)

from toolkit.scout.infer import (  # noqa: F401
    infer_granularity,
    infer_granularity_from_name_and_columns,
    infer_topics,
    infer_years,
    suggest_years,
)

from toolkit.scout.probe import (  # noqa: F401
    probe_url,
    probe_url_routed,
)
