"""toolkit.scout — URL probe, routing, inferenze e scaffold per candidate dataset.

Strato indipendente dalla CLI, usabile da:
- CLI (init --url, inspect url)
- MCP tools (scout tools)
- Source Observatory (source_check_fetch/analyze)
- Notebook e script interni

Moduli:
  http      → HTTP transport: probe, fetch, format detection, CKAN/SDMX
  infer     → Inferenze pure: anni, granularità, topic, validation, SQL
  probe     → Orchestrazione: probe_url(), probe_url_routed()
  scaffold  → Generazione YAML/SQL per candidate dataset
"""

from toolkit.scout.http import (
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
    extract_candidate_links,
    DEFAULT_TIMEOUT,
    DEFAULT_USER_AGENT,
    CANDIDATE_EXTENSIONS,
    EXTENDED_EXTENSIONS,
)

from toolkit.scout.infer import (
    infer_years,
    suggest_years,
    infer_granularity,
    infer_granularity_from_name_and_columns,
    infer_topics,
)

from toolkit.scout.scaffold import (
    suggest_clean_sql,
    suggest_mart_sql,
    suggest_validation,
)

from toolkit.scout.probe import (
    probe_url,
    probe_url_routed,
)

from toolkit.scout.scaffold import (
    slugify,
    infer_ext,
    infer_filename,
    generate_full_scaffold,
)
