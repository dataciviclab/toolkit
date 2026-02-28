# Publish Readiness Check

TODO before publication: add a root `LICENSE` file and matching `pyproject.toml` license metadata. This is required before making the repository/package public.

## ✅ What is ready

- Core CLI flow exists and is usable from source with `python -m toolkit.cli.app` / `py -m toolkit.cli.app`.
- Main commands expose help and non-zero failures on bad input:
  - `py -m toolkit.cli.app --help`
  - `py -m toolkit.cli.app run --help`
  - `py -m toolkit.cli.app run all -c does-not-exist.yml`
- Root resolution is deterministic and documented:
  - precedence is `dataset.yml root` -> `DCL_ROOT` -> `base_dir`
  - no fallback to `cwd`
  - enforced in `toolkit/core/config.py`
  - documented in `README.md`
- Portability contract is largely in place:
  - metadata, manifest, validation reports, and migrated run records use root-relative POSIX paths
  - covered by tests such as `tests/test_project_example_e2e.py`, `tests/test_validate_layers.py`, `tests/test_run_context.py`
- Repo hygiene on tracked files is good:
  - `git ls-files` contains no `_smoke_out`, `_test_out`, `.pytest_cache`, `.ruff_cache`, or `*.egg-info`
- CI exists and runs lint + tests in GitHub Actions.
- Offline example project exists and is suitable for first-run verification:
  - `project-example/dataset.yml`

## ⚠️ What is risky / unclear

- The published CLI name `toolkit` only works after install; many users will try it before installing. The source-tree-safe invocation is `python -m toolkit.cli.app`.
- README before this audit had installation and structure, but not a concise clean-machine quickstart.
- `pyproject.toml` still lacks some public package metadata expected by package indexes:
  - no `license`
  - no `authors`
- Network-backed smoke configs exist under `smoke/` and are useful, but should remain manual checks rather than CI gates because upstream formats and availability can change.
- Logging and CLI messages are partly Italian, partly English. That is not wrong, but it weakens the public-facing contract if the intended audience is broader than the current team.
- Local workspace still contains untracked generated artifacts and temp folders on disk during development. They are not tracked, but they can confuse manual reviewers if left around.

## ❌ What blocks publication

- No `LICENSE` file is present at repo root.
  - This is the main publication blocker because package/repo reuse terms are undefined.
  - Choosing a license is a legal/product decision, so it should not be guessed in code.
- `pyproject.toml` is still incomplete for a polished public release because `license` and `authors` metadata are missing.
- There is no explicit security policy file (`SECURITY.md`).
  - Not always mandatory for publication, but often expected for a public toolkit repo.

## Prioritized Checklist

### P0

- Add `LICENSE` and matching `pyproject.toml` license metadata. Effort: `S`
- Add package author/maintainer metadata in `pyproject.toml`. Effort: `S`
- Keep `pytest -q`, `ruff check .`, and one offline CLI smoke green in CI. Effort: `S`

### P1

- Add `SECURITY.md` with vulnerability reporting instructions. Effort: `S`
- Decide whether public docs should be fully English or intentionally bilingual, then normalize README/CLI wording. Effort: `M`
- Add a short release checklist for build, install, smoke, and artifact hygiene. Effort: `S`

### P2

- Add package metadata polish:
  - homepage/docs URLs beyond GitHub
  - keywords
  - tested Python version policy
  Effort: `S`
- Add a dedicated docs page for path/portability contracts and backward-compatibility guarantees. Effort: `M`
- Consider a build-and-install verification step (`python -m build` + wheel install smoke) once release metadata is finalized. Effort: `M`

## Evidence Summary

- Repo hygiene:
  - `git ls-files | rg '(_smoke_out|_test_out|\.pytest_cache|\.ruff_cache|\.egg-info)'` -> no matches
- Deterministic root precedence:
  - enforced in `toolkit/core/config.py`
  - documented in `README.md`
- Portability:
  - verified by tests `tests/test_project_example_e2e.py`, `tests/test_validate_layers.py`, `tests/test_run_context.py`
- CLI UX:
  - `py -m toolkit.cli.app --help` -> success
  - `py -m toolkit.cli.app run --help` -> success
  - `py -m toolkit.cli.app run all -c does-not-exist.yml` -> non-zero exit with clear config-path error
- Packaging:
  - `pyproject.toml` includes name, version, description, dependencies, entry point, min Python
  - still missing `license` and `authors`
- CI:
  - `.github/workflows/ci.yml` exists
  - runs `ruff`, `pytest`, hygiene check
  - after this audit, also runs an offline CLI smoke outside the repo tree

## Local Verification Commands

Windows PowerShell:

```powershell
py -m pip install -e ".[dev]"
py -m ruff check .
py -m pytest -q
py -m toolkit.cli.app --help
py -m toolkit.cli.app run --help
py -m toolkit.cli.app run all -c project-example/dataset.yml
py -m toolkit.cli.app status --dataset project_example --year 2022 --config project-example/dataset.yml
git ls-files | Select-String -Pattern '(_smoke_out|_test_out|\.pytest_cache|\.ruff_cache|\.egg-info)'
```

Linux/macOS:

```bash
python -m pip install -e ".[dev]"
python -m ruff check .
python -m pytest -q
python -m toolkit.cli.app --help
python -m toolkit.cli.app run --help
python -m toolkit.cli.app run all -c project-example/dataset.yml
python -m toolkit.cli.app status --dataset project_example --year 2022 --config project-example/dataset.yml
git ls-files | grep -E '(_smoke_out|_test_out|\.pytest_cache|\.ruff_cache|\.egg-info)'
```
