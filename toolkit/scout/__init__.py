"""toolkit.scout — URL probe, routing, inferenze per candidate dataset.

Strato indipendente dalla CLI, usabile da:
- CLI (scout, scout --scaffold)
- MCP tools (scout tools)
- Source Observatory (source_check_fetch/analyze)
- Notebook e script interni

Moduli:
  http           → HTTP transport: probe, fetch, format detection, CKAN/SDMX
  link_extractor → Estrazione e raggruppamento link dati da HTML
  sparql         → SPARQL scout: named graph discovery, schema inference
  infer          → Inferenze pure: anni, granularità, topic
  probe          → Orchestrazione: probe_url(), probe_url_routed()

Scaffold non e' piu' in scout. Usa toolkit.scaffold.full e toolkit.scaffold.sources.
"""

from toolkit.scout.sparql import (  # noqa: F401
    discover_named_graphs,
    fetch_sparql_count,
    infer_graph_schema,
)

from toolkit.scout.http import (  # noqa: F401
    probe_url_headers,
    fetch_content,
    fetch_html_body,
    fetch_ckan_package,
    discover_ckan_resources,
    fetch_sdmx_years,
    is_html_content,
    is_file_like,
    is_sdmx_url,
    resolve_preview_kind,
    detect_ckan_in_html,
    extract_ckan_dataset_id,
    DEFAULT_TIMEOUT,
    DEFAULT_USER_AGENT,
    CANDIDATE_EXTENSIONS,
    EXTENDED_EXTENSIONS,
)

from toolkit.scout.link_extractor import (  # noqa: F401
    DataLink,
    LinkGroup,
    extract_data_links,
    group_links,
    extract_candidate_links,
)

from toolkit.scout.infer import (  # noqa: F401
    infer_years,
    suggest_years,
    infer_granularity,
    infer_granularity_from_name_and_columns,
    infer_topics,
)

from toolkit.scout.probe import (  # noqa: F401
    PortalProfile,
    fetch_sitemap_pages,
    probe_html_portal,
    probe_url,
    probe_url_routed,
)
