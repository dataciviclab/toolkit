# Publish Readiness Check

## What is ready

- Core CLI flow exists and is usable after install with `toolkit`.
- Main commands expose help and non-zero failures on bad input:
  - `toolkit --help`
  - `toolkit run --help`
  - `toolkit run all -c does-not-exist.yml`
- Root resolution is deterministic and documented:
  - precedence is `dataset.yml root` -> `DCL_ROOT` -> `base_dir`
  - no fallback to `cwd`
  - enforced in `toolkit/core/config.py`
  - documented in `README.md`
- Portability contract is in place:
  - metadata, manifest, validation reports, and migrated run records use root-relative POSIX paths
  - covered by tests such as `tests/test_project_example_e2e.py`, `tests/test_validate_layers.py`, `tests/test_run_context.py`
- Repo hygiene on tracked files is good:
  - `git ls-files` contains no `_smoke_out`, `_test_out`, `.pytest_cache`, `.ruff_cache`, or `*.egg-info`
- CI exists and runs lint + tests in GitHub Actions.
- Offline example project exists and is suitable for first-run verification:
  - `project-example/dataset.yml`

## Remaining risks / decisions

- The published CLI name `toolkit` only works after install; the source-tree-safe invocation is `python -m toolkit.cli.app`.
- Network-backed smoke configs under `smoke/` should remain manual checks rather than CI gates because upstream formats and availability can change.
- Logging and CLI messages are partly Italian, partly English. That is acceptable, but public-facing language should be an intentional choice.

## Recommended release checklist

### P0

- Keep `pytest -q`, `ruff check .`, and one offline CLI smoke green in CI.
- Run `python -m build` before tagging a release.
- Verify package metadata after install with `python -m pip show dataciviclab-toolkit`.

### P1

- Keep `SECURITY.md` current with the right reporting contact/process.
- Decide whether public docs should stay bilingual or move to one primary language.
- Add package metadata polish if needed:
  - homepage/docs URLs beyond GitHub
  - keywords
  - tested Python version policy

## Local Verification Commands

Windows PowerShell:

```powershell
pip install -e ".[dev]"
ruff check .
pytest -q
toolkit --help
toolkit run --help
toolkit run all -c project-example/dataset.yml
toolkit status --dataset project_example --year 2022 --config project-example/dataset.yml
git ls-files | Select-String -Pattern '(_smoke_out|_test_out|\.pytest_cache|\.ruff_cache|\.egg-info)'
```

Linux/macOS:

```bash
pip install -e ".[dev]"
ruff check .
pytest -q
toolkit --help
toolkit run --help
toolkit run all -c project-example/dataset.yml
toolkit status --dataset project_example --year 2022 --config project-example/dataset.yml
git ls-files | grep -E '(_smoke_out|_test_out|\.pytest_cache|\.ruff_cache|\.egg-info)'
```
