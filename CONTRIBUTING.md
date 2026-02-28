# Contributing

## Git Hook

Install the lightweight pre-commit guardrail before contributing:

```bash
cp scripts/pre-commit .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
```

The hook blocks commits that include generated artifacts or caches such as `_smoke_out/`, `_test_out/`, `.pytest_cache/`, `.ruff_cache/`, and `*.egg-info/`.
