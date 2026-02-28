# Changelog

All notable changes to this project will be documented in this file.

## [0.1.0] - 2026-02-28

### Added

- Typed configuration models with Pydantic v2 for `dataset.yml`.
- End-to-end smoke tests for tiny CSV and local ZIP extraction flows.
- Install and CLI smoke script for clean-environment verification.
- Configuration schema documentation with minimal and full examples.
- Centralized config deprecation policy with `DCL001` to `DCL008` warning codes.
- `--strict-config` CLI option and `config.strict` config switch.
- Release changelog.

### Changed

- `load_config()` now parses through typed config models while preserving the current consumer API.
- Validation specs for CLEAN and MART now rely on typed rule structures instead of ad hoc runtime coercion.
- CI now runs as an OS and Python matrix for Ubuntu and Windows on Python 3.10 and 3.11.
- Packaging version is now sourced from [toolkit/version.py](/c:/Users/gabry/OneDrive/Desktop/test-git/toolkit/toolkit/version.py).

### Fixed

- Boolean-like config values such as `"false"` and `"0"` no longer evaluate incorrectly as truthy.
- List-like validation fields no longer degrade into character-by-character lists when given as strings.
- CLEAN and MART validation runners no longer attempt to validate unrelated config keys against strict validation specs.
- CLI strict-config handling no longer misinterprets Typer option metadata as enabled strict mode.

### Deprecated

- `raw.source` in favor of `raw.sources`
- `raw.sources[].plugin` in favor of `raw.sources[].type`
- `raw.sources[].id` in favor of `raw.sources[].name`
- scalar `clean.read` in favor of `clean.read.source`
- `clean.read.csv.*` in favor of `clean.read.*`
- `clean.sql_path`
- `mart.sql_dir`
- `bq`
