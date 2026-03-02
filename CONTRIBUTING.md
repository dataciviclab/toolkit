# Contributing

## Test Suite Tiers

The test suite is intentionally stratified so release-critical coverage stays visible.

- `core`: public contract and canonical workflow
  - config and strict mode
  - path contract relative to `dataset.yml`
  - `run all`, `validate all`, `status`, `inspect paths`
  - end-to-end RAW -> CLEAN -> MART
  - run records, resume, validation layers
- `advanced`: supported but non-happy-path or secondary engine behavior
  - detailed read modes and selection logic
  - extractors and plugin registry
  - profiling details
  - artifact policy and logging helpers
- `compat`: compatibility-only behavior
  - deprecated import shims and legacy-only coverage

Useful commands:

```bash
py -m pytest -m core
py -m pytest -m "core or advanced"
py -m pytest -m compat
```

## Git Hook

Install the lightweight pre-commit guardrail before contributing:

```bash
cp scripts/pre-commit .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
```

The hook blocks commits that include generated artifacts or caches such as `_smoke_out/`, `_test_out/`, `.pytest_cache/`, `.ruff_cache/`, and `*.egg-info/`.
