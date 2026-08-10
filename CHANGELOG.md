## [1.49.2] - 2026-08-10

### Fixed

- **clean_catalog preserva il run storico dall'existing** (PR #465): il blocco `run` era derivato solo dal run record locale — in CI post-merge i dataset non rieseguiti perdevano il run a ogni rigenerazione (emerso dal dispatch su comuni-master). Ora, se il run locale manca, si preserva `existing[].run` (stesso pattern di `build_signals`); il run locale vince sempre.

### Removed

- **check-gcs** (PR #465): `derive_mode="check-gcs"`/`check_gcs` e `_check_gcs_locations` rimossi da `build_clean_catalog`/`build_registry` — segnale informativo non bloccante (derive=warning), nessun consumer lo usa, DI è passato a `gsutil rsync`.

## [1.49.0] - 2026-08-08

### Changed

- **Fusion registry (ADR)** (PR #453): gli artifact registry per repo passano da 5 file separati (clean_catalog, mart_catalog, pipeline_signals, codelists, entity_graph) a **UN `registry.json`** con sezioni (datasets/marts/signals/codelists/entities) + `registry.schema.json` unico. Reader/CLI/MCP con fallback legacy sui vecchi file (i repo non migrati continuano a funzionare).
- **Drop del `gcs_manifest`** dal resolver (PR #453): `source='gcs'` legge i registry.json committati (path GCS esatti per repo). Risolve il bug `slug=parts[0]` che assegnava lo slug "eurostat" a 224 file del layout con prefisso org. `manifest_url` accettato ma non più usato (compat di firma).
- **`existing_signals`** (PR #453): `build_signals` preserva il blocco `run` dal signals committato quando il run locale manca (CI post-merge) — stesso pattern di `existing_catalog`; il run locale vince.

### Removed

- Schemi legacy `clean_catalog`/`mart_catalog`/`pipeline_signals`/`codelists`/`run.schema.json` (doppia validazione eliminata, -972 righe).

## [1.48.2] - 2026-08-07

### Fixed

- **Resolver: scan parquet multi-repo** (emerso dalle prove MCP su eurostat): `_scan_workspace_parquets` scansionava solo `dataset-incubator/` — i parquet dei repo esterni (eurostat, dcl-bologna) non erano visibili → `dataset_overview`/`describe_slug` rispondevano `parquet_not_found` anche con i parquet locali presenti. Ora scansiona tutte le dir di primo livello del workspace (stesso pattern di `_scan_committed_catalogs`). Nota: il server MCP cachea il resolver per istanza — va riavviato per riflettere il fix.

## [1.48.1] - 2026-08-07

### Fixed

- **Schema registry: enum run.status incompleto** (emerso dal wrapper DI): gli schemi `run`, `pipeline_signals`, `mart_catalog`, `clean_catalog` ammettevano solo `SUCCESS/FAILED/PENDING/SKIPPED`, ma il run_state del toolkit produce anche `RUNNING` e `DRY_RUN` → il builder rifiutava i catalog con run parziali/dry. Allineato l'enum al vocabolario reale (`toolkit/core/run_records.py`).

## [1.48.0] - 2026-08-07

### Added

- **Profilo Eurostat (ESTAT) in `SdmxSource`** (PR #450): fetch TSV wide con unpivot e flag, constraints SDMX-JSON 2.0, `fetch_codelist` con annotations NUTS, scout dataflow ESTAT + scaffold `agency: ESTAT`, `version` opzionale per sdmx. Fix `sniff_decimal` field-aware.
- **Support unificati — ADR-005** (PR #451): `support[].type` (`dataset`/`codelist`/`file`), orchestrazione ensure (skip-if-exists + materializzazione per tipo), placeholder `{support.NAME.clean|mart.TABLE|path}`, flag `--refresh-support`. `mypy toolkit/` pulito al 100%.
- **Modulo registry condiviso** (PR #452): `toolkit.registry` — builder di `clean_catalog`/`mart_catalog`/`pipeline_signals`/`codelists` (derive locale + check-gcs + existing_catalog), schemi JSON standard, `semantic_types.yaml` condiviso, reader CLI (`toolkit registry list/show`) e MCP (`registry_list/show`), find semantico nel resolver.

# Changelog

All notable changes to this project will be documented in this file.

## [1.47.2] - 2026-08-02

### Fixed

- **Validazione mart multi-year (`mart.tables[].years`)** (PR #446, issue #445): un candidate con tabelle mart multi-year non poteva completare il run — la validazione per-anno falliva (`Missing required MART tables`) e il passaggio multi-year non partiva mai. Ora:
  - la validazione per-anno esclude le tabelle con `years` (prodotte a livello dataset);
  - `run_mart_multi_year` valida le tabelle prodotte (applica `table_rules`/`required_tables`);
  - candidate con solo tabelle multi-year registrano validazione mart "skipped" (non falliscono il run);
  - il path resolver espone le tabelle multi-year a livello dataset, quindi readiness/summary non segnalano più `mart_outputs_missing`.

## [1.17.0] - 2026-05-29

### Changed

- **Centralizzati tutti i path artifact in `core/paths.py`**: validation, profile e metadata nei layer raw/clean/mart ora referenziano costanti invece di stringhe letterali (PR #298).
- **review-readiness arricchito**: ora include `validation_msgs` (primi 3 errori/warning reali per layer), profilo raw e statistiche di transizione. Integrato in `run full` e MCP `toolkit_review_readiness` (PR #296).
- **Regole validazione centralizzate**: `core/column_rules.py` come posizione canonica per `check_not_null`, `check_primary_key`, `check_ranges`, `check_max_null_pct`. Utility SQL (`q_ident`, `sql_path`, `quote_list`) in `core/sql_utils.py` (PR #295).

### Fixed

- **`list_candidates` MCP usava root hardcoded**: ora legge il campo `root` dal dataset.yml, risolvendo `has_clean=False` per la maggior parte dei candidati (PR #297).

## [1.18.0] - 2026-05-29

### Changed

- **Backward-compat cleanup**: rimossi 9 artefatti backward-compat (shim `mcp/contracts.py`, alias `build_profile_hints`, re-export `run_context`, stub CLI `cross_year` e `inspect url`, MCP aliases `toolkit_show_schema`/`toolkit_raw_profile`, `block_url_direct`, scaffold re-export da `scout`). Netto -140 righe (PR #300).
- **MCP tool names allineati a CLI**: `toolkit_show_schema` → `toolkit_inspect_schema`, `toolkit_raw_profile` → `toolkit_inspect_profile`. Help comandi `run` arricchito con docstring specifici per step (PR #299).

## [1.25.0] - 2026-06-05

### Added

- **`align_by_header` in `clean.read`**: nuovo flag che attiva l'allineamento delle righe CSV per nome colonna invece che per posizione. Colonne attese ma assenti nell'header → stringa vuota; colonne extra → ignorate; ordine diverso → riallineato. Richiede `normalize_rows_to_columns: true` e `header: true`. Risolve schema drift con colonne intermedie che appaiono/scompaiono tra anni (PR #329).

### Changed

- Bump versione: 1.24.0 → 1.25.0

## [Unreleased]

### Added

- **`sql/macros.sql` — standard DuckDB macros per il layer CLEAN** (PR #416).

## [1.45.0] - 2026-07-21

### Added

- **`sql/macros.sql` — standard DuckDB macros per il layer CLEAN** (PR #416).
  7 macro precaricate automaticamente in ogni clean.sql:
  `normalize_string`, `cast_int`, `cast_bigint`, `cast_double`,
  `normalize_italian_number`, `normalize_italian_integer`, `decode_flag`,
  `remove_dot_thousands`. Documentazione in `docs/standard-macros.md`.
- **`core/normalize.py` — utility preprocessing centralizzate**: `normalize_number` (normalizza numeri formato italiano), `normalize_columns_map` (mapping colonne via regex), e `decode_bytes`/`decode_csv_bytes` (decodifica multi-encoding con fallback). Rimpiazza 13 copie identiche nei candidate di dataset-incubator (PR #411).
- **HTTP centralizzato su `lab_connectors.http`**: `http_file`, `ckan`, `sdmx` e `inspect url` ora usano `HttpClient` invece di `requests.get` diretto, con retry, SSL fallback e timeout uniformi (PR #232, #233, #234, #235).
- `lab-connectors` aggiunto come dipendenza core (git URL in `pyproject.toml`).

### Removed

- Legacy config forms below no longer emit deprecation warnings and now fail with explicit config errors:
  - `bq`
  - `raw.source`
  - `raw.sources[].plugin`
  - `raw.sources[].id`
  - scalar `clean.read`
  - `clean.read.csv.*`
  - `clean.sql_path`
  - `mart.sql_dir`

## [1.1.0] - 2026-03-02

### Added

- Runtime boundaries documentation clarifying core, advanced and compatibility-only toolkit surfaces.
- RAW profile hints in metadata for lightweight diagnostics during normal RAW runs.
- Pytest markers and a more explicit split between fast tests and heavier smoke-like checks.

### Changed

- Reduced the runtime surface area by removing peripheral experimental helpers and non-core shims.
- Refined CLEAN input selection, DuckDB read flow and orchestration to make the RAW -> CLEAN bridge more predictable.
- Refreshed smoke and profiling documentation around the supported operational workflow.
- Clarified manifest and metadata writing so runtime artifacts better reflect actual layer outputs.

### Removed

- Deprecated core import shims that no longer belonged to the stable runtime contract.
- Frozen helper surfaces such as `gen-sql` and peripheral experimental plugins.
- Obsolete validator/helper modules that duplicated the current runtime path.

## [1.0.0] - 2026-02-28

### Added

- Typed configuration models with Pydantic v2 for `dataset.yml`.
- End-to-end smoke tests for tiny CSV and local ZIP extraction flows.
- Install and CLI smoke script for clean-environment verification.
- Configuration schema documentation with minimal and full examples.
- Centralized config deprecation policy with `DCL001` to `DCL013` warning codes.
- `--strict-config` CLI option and `config.strict` config switch.
- Explicit built-in plugin registry with strict/non-strict handling for optional plugins.
- Coverage reporting in CI with XML artifact upload and fail-under threshold.
- Release changelog.

### Changed

- `load_config()` now parses through typed config models while preserving the current consumer API.
- Validation specs for CLEAN and MART now rely on typed rule structures instead of ad hoc runtime coercion.
- CI now runs as an OS and Python matrix for Ubuntu and Windows on Python 3.10 and 3.11.
- CI now publishes `coverage.xml` artifacts and enforces minimum package coverage.
- Packaging version is now sourced from [toolkit/version.py](/c:/Users/gabry/OneDrive/Desktop/test-git/toolkit/toolkit/version.py).

### Fixed

- Boolean-like config values such as `"false"` and `"0"` no longer evaluate incorrectly as truthy.
- List-like validation fields no longer degrade into character-by-character lists when given as strings.
- CLEAN and MART validation runners no longer attempt to validate unrelated config keys against strict validation specs.
- CLI strict-config handling no longer misinterprets Typer option metadata as enabled strict mode.
- DuckDB connections in CLEAN and MART are always closed, avoiding Windows file-lock issues on produced parquet files.
- `resume` now verifies previous-layer artifacts before resuming and supports explicit restart from a chosen layer.
- Documentation and canonical examples no longer rely on deprecated `raw.source`.

### Deprecated

- `raw.source` in favor of `raw.sources`
- `raw.sources[].plugin` in favor of `raw.sources[].type`
- `raw.sources[].id` in favor of `raw.sources[].name`
- scalar `clean.read` in favor of `clean.read.source`
- `clean.read.csv.*` in favor of `clean.read.*`
