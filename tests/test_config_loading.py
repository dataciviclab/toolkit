"""Tests for config loading: happy path, path resolution, env vars, repo layout."""

from pathlib import Path

import pytest

from toolkit.core.config import load_config
from toolkit.core.config_models import load_config_model


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Base YAML structure shared by most tests.
# All tests use this as foundation; overrides add/replace keys.
YAML_BASE = {
    "root": None,
    "dataset": {"name": "demo", "years": [2022]},
    "raw": {},
    "clean": {},
    "mart": {},
}


def _yml(path: Path, **overrides) -> Path:
    """Write a dataset.yml merging YAML_BASE with per-test overrides.

    Args:
        path: path to write to (yml file)
        **overrides: top-level keys to override in YAML_BASE
                    (e.g. root=".", raw={...})
                    Special keys: 'years' is merged into dataset.years.

    Returns:
        The path (unchanged, for chaining with load_config)
    """
    import copy
    import yaml

    merged = copy.deepcopy(YAML_BASE)
    # years goes inside dataset block
    if "years" in overrides:
        merged["dataset"]["years"] = overrides.pop("years")
    merged.update(overrides)
    path.write_text(yaml.safe_dump(merged, sort_keys=False), encoding="utf-8")
    return path


def _yml_str(path: Path, body: str) -> Path:
    """Write a dataset.yml from an explicit multi-line YAML string.

    Use for complex structures (mart.tables, cross_year) that are
    easier to express inline than via _yml overrides.
    """
    path.write_text(body.strip() + "\n", encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


@pytest.mark.contract
def test_load_config_ok(tmp_path: Path):
    yml = tmp_path / "dataset.yml"
    _yml(yml, years=[2022, "2023"])

    cfg = load_config(yml)
    assert cfg.dataset == "demo"
    assert cfg.years == [2022, 2023]
    assert cfg.base_dir == tmp_path
    assert cfg.root == tmp_path
    assert cfg.root_source == "base_dir_fallback"


@pytest.mark.policy
def test_load_config_parses_mart_transition_config(tmp_path: Path):
    yml = tmp_path / "dataset.yml"
    _yml(yml, years=[2024], mart={"validate": {"transition": {"max_row_drop_pct": 12.5, "warn_removed_columns": "false"}}})

    cfg = load_config(yml)
    assert cfg.mart.validate.transition is not None
    assert cfg.mart.validate.transition.model_dump(exclude_none=True, exclude_unset=True) == {
        "max_row_drop_pct": 12.5,
        "warn_removed_columns": False,
    }


@pytest.mark.policy
def test_load_config_parses_clean_promotion_config(tmp_path: Path):
    yml = tmp_path / "dataset.yml"
    _yml(yml, years=[2024], clean={"validate": {"promotion": {"max_row_drop_pct": 8.5, "warn_removed_columns": "false"}}})

    cfg = load_config(yml)
    assert cfg.clean.validate.promotion is not None
    assert cfg.clean.validate.promotion.model_dump(exclude_none=True, exclude_unset=True) == {
        "max_row_drop_pct": 8.5,
        "warn_removed_columns": False,
    }


# ---------------------------------------------------------------------------
# Validation / error cases
# ---------------------------------------------------------------------------


@pytest.mark.contract
def test_load_config_model_rejects_invalid_mart_transition_bool(tmp_path: Path):
    yml = tmp_path / "dataset.yml"
    _yml(yml, mart={"validate": {"transition": {"warn_removed_columns": "maybe"}}})

    with pytest.raises(ValueError) as e:
        load_config_model(yml)

    assert "mart.validate.transition.warn_removed_columns" in str(e.value)


@pytest.mark.contract
def test_load_config_missing_dataset_name(tmp_path: Path):
    yml = tmp_path / "dataset.yml"
    _yml_str(yml,
        "dataset:\n"
        "  years: [2022]\n"
        "raw: {}\n"
        "clean: {}\n"
        "mart: {}",
    )

    with pytest.raises(ValueError) as e:
        load_config(yml)

    assert "dataset.name" in str(e.value)


@pytest.mark.contract
def test_load_config_rejects_duplicate_support_names(tmp_path: Path):
    yml = tmp_path / "dataset.yml"
    _yml_str(yml,
        "root: './out'\n"
        "dataset:\n"
        "  name: demo\n"
        "  years: [2022]\n"
        "raw: {}\n"
        "clean: {}\n"
        "mart: {}\n"
        "support:\n"
        "  - name: scuole\n"
        "    config: './support_a.yml'\n"
        "    years: [2024]\n"
        "  - name: scuole\n"
        "    config: './support_b.yml'\n"
        "    years: [2025]",
    )

    with pytest.raises(ValueError) as e:
        load_config(yml)

    assert "support[].name values must be unique" in str(e.value)


# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------


@pytest.mark.policy
def test_load_config_resolves_relative_paths_from_dataset_dir(tmp_path: Path):
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    (project_dir / "sql" / "mart").mkdir(parents=True)
    (project_dir / "sql" / "cross").mkdir(parents=True)

    yml = project_dir / "dataset.yml"
    _yml_str(yml,
        "root: './out'\n"
        "dataset:\n"
        "  name: demo\n"
        "  years: [2022]\n"
        "raw:\n"
        "  sources:\n"
        "    - type: local_file\n"
        "      args:\n"
        "        path: 'data/raw.csv'\n"
        "clean:\n"
        "  sql: 'sql/clean.sql'\n"
        "mart:\n"
        "  tables:\n"
        "    - name: demo_mart\n"
        "      sql: 'sql/mart/demo.sql'\n"
        "    - name: demo_multi_year\n"
        "      sql: 'sql/multi_year/demo_multi.sql'\n"
        "      years: [2022]\n"
        "      source_layer: clean",
    )

    cfg = load_config(yml)

    assert cfg.base_dir == project_dir.resolve()
    assert cfg.root == (project_dir / "out").resolve()
    assert cfg.root_source == "yml"
    assert cfg.raw.sources[0].args["path"] == (project_dir / "data" / "raw.csv").resolve()
    assert cfg.clean.sql == (project_dir / "sql" / "clean.sql").resolve()
    assert cfg.mart.tables[0].sql == (project_dir / "sql" / "mart" / "demo.sql").resolve()
    # multi-year mart table path resolution (assorbe ex cross_year)
    multi_year_table = cfg.mart.tables[1]
    assert multi_year_table.name == "demo_multi_year"
    assert multi_year_table.sql == (project_dir / "sql" / "multi_year" / "demo_multi.sql").resolve()
    assert multi_year_table.years == [2022]
    assert multi_year_table.source_layer == "clean"


@pytest.mark.policy
def test_load_config_resolves_support_config_paths_from_dataset_dir(tmp_path: Path):
    project_dir = tmp_path / "project"
    support_dir = tmp_path / "support"
    project_dir.mkdir()
    support_dir.mkdir()

    yml = project_dir / "dataset.yml"
    _yml_str(yml,
        "root: './out'\n"
        "dataset:\n"
        "  name: demo\n"
        "  years: [2022]\n"
        "raw: {}\n"
        "clean: {}\n"
        "mart: {}\n"
        "support:\n"
        "  - name: scuole\n"
        "    config: '../support/dataset.yml'\n"
        "    years: [2024]",
    )

    cfg = load_config(yml)

    assert len(cfg.support) == 1
    s = cfg.support[0]
    assert s.name == "scuole"
    assert s.config == (support_dir / "dataset.yml").resolve()
    assert s.years == [2024]


@pytest.mark.policy
def test_load_config_does_not_transform_non_whitelisted_path_like_fields(tmp_path: Path):
    project_dir = tmp_path / "project"
    project_dir.mkdir()

    yml = project_dir / "dataset.yml"
    _yml_str(yml,
        "root: './out'\n"
        "dataset:\n"
        "  name: demo\n"
        "  years: [2022]\n"
        "raw:\n"
        "  sources:\n"
        "    - type: local_file\n"
        "      args:\n"
        "        path: 'data/raw.csv'\n"
        "        filename: 'nested/raw.csv'\n"
        "clean:\n"
        "  sql: 'sql/clean.sql'\n"
        "  note_path: 'docs/clean.md'\n"
        "mart:\n"
        "  tables:\n"
        "    - name: demo_mart\n"
        "      sql: 'sql/mart/demo.sql'\n"
        "  label_path: 'labels/mart.txt'",
    )

    cfg = load_config(yml)

    assert cfg.raw.sources[0].args["path"] == (project_dir / "data" / "raw.csv").resolve()
    assert cfg.raw.sources[0].args["filename"] == "nested/raw.csv"
    assert cfg.clean.note_path == "docs/clean.md"
    assert cfg.mart.label_path == "labels/mart.txt"


@pytest.mark.policy
def test_load_config_preserves_year_template_in_raw_local_file_path(tmp_path: Path):
    project_dir = tmp_path / "project"
    project_dir.mkdir()

    yml = project_dir / "dataset.yml"
    _yml_str(yml,
        "root: './out'\n"
        "dataset:\n"
        "  name: demo\n"
        "  years: [2022, 2023]\n"
        "raw:\n"
        "  sources:\n"
        "    - type: local_file\n"
        "      args:\n"
        "        path: 'data/raw_{year}.csv'\n"
        "        filename: 'raw_{year}.csv'\n"
        "clean: {}\n"
        "mart: {}",
    )

    cfg = load_config(yml)

    assert cfg.raw.sources[0].args["path"] == str((project_dir / "data" / "raw_{year}.csv").resolve())
    assert cfg.raw.sources[0].args["filename"] == "raw_{year}.csv"


# ---------------------------------------------------------------------------
# Env var / fallback resolution
# ---------------------------------------------------------------------------


@pytest.mark.policy
def test_load_config_uses_dcl_root_when_root_missing(tmp_path: Path, monkeypatch):
    project_dir = tmp_path / "project"
    project_dir.mkdir()

    yml = project_dir / "dataset.yml"
    _yml(yml)

    out_base = tmp_path / "dcl-root"
    monkeypatch.setenv("DCL_ROOT", str(out_base))

    cfg = load_config(yml)

    assert cfg.root == out_base.resolve()
    assert cfg.root_source == "env:DCL_ROOT"


@pytest.mark.policy
def test_load_config_uses_toolkit_outdir_for_managed_smoke_root(tmp_path: Path, monkeypatch):
    project_dir = tmp_path / "project"
    project_dir.mkdir()

    yml = project_dir / "dataset.yml"
    _yml(yml, root="./_smoke_out")

    out_base = tmp_path / "toolkit-out"
    monkeypatch.setenv("TOOLKIT_OUTDIR", str(out_base))

    cfg = load_config(yml)

    assert cfg.root == out_base.resolve()
    assert cfg.root_source == "env:TOOLKIT_OUTDIR"


@pytest.mark.policy
def test_load_config_uses_base_dir_when_root_missing_and_dcl_root_missing(tmp_path: Path, monkeypatch):
    project_dir = tmp_path / "project"
    project_dir.mkdir()

    yml = project_dir / "dataset.yml"
    _yml(yml)

    monkeypatch.delenv("DCL_ROOT", raising=False)

    cfg = load_config(yml)

    assert cfg.root == project_dir.resolve()
    assert cfg.root_source == "base_dir_fallback"


# ---------------------------------------------------------------------------
# Repo root guard
# ---------------------------------------------------------------------------


REPO_LAYOUT_CASES = [
    # (dataset_rel, root_value)
    ("candidates/demo_dataset", "../../out"),
    ("candidates/demo_dataset/sources/demo_source", "../../../../out"),
    ("support_datasets/demo_support", "../../out"),
]


@pytest.mark.policy
@pytest.mark.parametrize(("dataset_rel", "root_value"), REPO_LAYOUT_CASES)
def test_load_config_resolves_repo_out_for_dataset_incubator_layouts(
    tmp_path: Path,
    dataset_rel: str,
    root_value: str,
):
    repo_root = tmp_path / "dataset-incubator"
    dataset_dir = repo_root / Path(dataset_rel)
    dataset_dir.mkdir(parents=True, exist_ok=True)
    yml = dataset_dir / "dataset.yml"
    _yml_str(yml,
        f'root: "{root_value}"\n'
        "dataset:\n"
        "  name: demo\n"
        "  years: [2022]\n"
        "raw: {}\n"
        "clean: {}\n"
        "mart: {}",
    )

    cfg = load_config(yml, repo_root=repo_root)

    assert cfg.root == (repo_root / "out").resolve()
    assert cfg.root_source == "yml"


@pytest.mark.policy
def test_load_config_accepts_absolute_root_within_repo_when_repo_root_is_provided(tmp_path: Path):
    repo_root = tmp_path / "dataset-incubator"
    dataset_dir = repo_root / "candidates" / "demo_dataset"
    dataset_dir.mkdir(parents=True, exist_ok=True)
    allowed_root = (repo_root / "out").resolve()
    yml = dataset_dir / "dataset.yml"
    _yml_str(yml,
        f'root: "{allowed_root.as_posix()}"\n'
        "dataset:\n"
        "  name: demo\n"
        "  years: [2022]\n"
        "raw: {}\n"
        "clean: {}\n"
        "mart: {}",
    )

    cfg = load_config(yml, repo_root=repo_root)

    assert cfg.root == allowed_root
    assert cfg.root_source == "yml"


@pytest.mark.policy
def test_load_config_rejects_root_outside_repo_when_repo_root_is_provided(tmp_path: Path):
    repo_root = tmp_path / "dataset-incubator"
    dataset_dir = repo_root / "candidates" / "demo_dataset"
    dataset_dir.mkdir(parents=True, exist_ok=True)
    outside_root = tmp_path / "outside"
    yml = dataset_dir / "dataset.yml"
    _yml_str(yml,
        f'root: "{outside_root.as_posix()}"\n'
        "dataset:\n"
        "  name: demo\n"
        "  years: [2022]\n"
        "raw: {}\n"
        "clean: {}\n"
        "mart: {}",
    )

    with pytest.raises(ValueError) as exc:
        load_config(yml, repo_root=repo_root)

    assert "root resolves outside repo_root" in str(exc.value)
    assert str(outside_root.resolve()) in str(exc.value)
    assert str(repo_root.resolve()) in str(exc.value)


@pytest.mark.policy
def test_load_config_allows_root_outside_repo_without_repo_root_guard(tmp_path: Path):
    repo_root = tmp_path / "dataset-incubator"
    dataset_dir = repo_root / "candidates" / "demo_dataset"
    dataset_dir.mkdir(parents=True, exist_ok=True)
    yml = dataset_dir / "dataset.yml"
    _yml_str(yml,
        "root: '../../../outside'\n"
        "dataset:\n"
        "  name: demo\n"
        "  years: [2022]\n"
        "raw: {}\n"
        "clean: {}\n"
        "mart: {}",
    )

    cfg = load_config(yml)

    assert cfg.root == (tmp_path / "outside").resolve()


# ---------------------------------------------------------------------------
# Strict / project example
# ---------------------------------------------------------------------------


@pytest.mark.contract
def test_project_example_config_parses_in_strict_mode():
    model = load_config_model(Path("project-example") / "dataset.yml", strict_config=True)

    assert model.dataset.name == "project_example"
    assert len(model.raw.sources) == 1


# ---------------------------------------------------------------------------
# mart.required_tables auto-fill
# (Complex mart.tables sections don't fit _yml helper; keep inline)
# ---------------------------------------------------------------------------


@pytest.mark.contract
def test_mart_required_tables_auto_filled_from_tables(tmp_path: Path):
    """When required_tables is omitted, it defaults to all table names from tables."""
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    yml = project_dir / "dataset.yml"
    _yml_str(yml,
        "dataset:\n"
        "  name: demo\n"
        "  years: [2022]\n"
        "raw: {}\n"
        "clean: {}\n"
        "mart:\n"
        "  tables:\n"
        "    - name: table_a\n"
        "      sql: sql/mart/a.sql\n"
        "    - name: table_b\n"
        "      sql: sql/mart/b.sql",
    )

    cfg = load_config(yml)
    assert cfg.mart.tables[0].name == "table_a"
    assert cfg.mart.tables[1].name == "table_b"
    assert cfg.mart.required_tables == ["table_a", "table_b"]


@pytest.mark.contract
def test_mart_required_tables_explicit_empty_auto_fills(tmp_path: Path):
    """required_tables: [] auto-fills to all table names."""
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    yml = project_dir / "dataset.yml"
    _yml_str(yml,
        "dataset:\n"
        "  name: demo\n"
        "  years: [2022]\n"
        "raw: {}\n"
        "clean: {}\n"
        "mart:\n"
        "  tables:\n"
        "    - name: table_a\n"
        "      sql: sql/mart/a.sql\n"
        "  required_tables: []",
    )

    cfg = load_config(yml)
    assert cfg.mart.required_tables == ["table_a"]


@pytest.mark.contract
def test_mart_required_tables_explicit_subset(tmp_path: Path):
    """Explicit required_tables list is used as-is."""
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    yml = project_dir / "dataset.yml"
    _yml_str(yml,
        "dataset:\n"
        "  name: demo\n"
        "  years: [2022]\n"
        "raw: {}\n"
        "clean: {}\n"
        "mart:\n"
        "  tables:\n"
        "    - name: table_a\n"
        "      sql: sql/mart/a.sql\n"
        "    - name: table_b\n"
        "      sql: sql/mart/b.sql\n"
        "  required_tables:\n"
        "    - table_a",
    )

    cfg = load_config(yml)
    assert cfg.mart.required_tables == ["table_a"]
