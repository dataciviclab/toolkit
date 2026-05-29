# Changelog

All notable changes to this project will be documented in this file.

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

## [Unreleased]
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
