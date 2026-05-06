"""Tests for config loading: happy path, path resolution, env vars, repo layout."""

from pathlib import Path

import pytest

from toolkit.core.config import load_config
from toolkit.core.config_models import load_config_model


@pytest.mark.contract
def test_load_config_ok(tmp_path: Path):
    yml = tmp_path / "dataset.yml"
    yml.write_text(
        """
root: null
dataset:
  name: demo
  years: [2022, "2023"]
raw: {}
clean: {}
mart: {}
""".strip(),
        encoding="utf-8",
    )

    cfg = load_config(yml)
    assert cfg.dataset == "demo"
    assert cfg.years == [2022, 2023]
    assert cfg.base_dir == tmp_path
    assert cfg.root == tmp_path
    assert cfg.root_source == "base_dir_fallback"


@pytest.mark.policy
def test_load_config_parses_mart_transition_config(tmp_path: Path):
    yml = tmp_path / "dataset.yml"
    yml.write_text(
        """
root: null
dataset:
  name: demo
  years: [2024]
raw: {}
clean: {}
mart:
  validate:
    transition:
      max_row_drop_pct: 12.5
      warn_removed_columns: "false"
""".strip(),
        encoding="utf-8",
    )

    cfg = load_config(yml)

    assert cfg.mart["validate"]["transition"] == {
        "max_row_drop_pct": 12.5,
        "warn_removed_columns": False,
    }


@pytest.mark.policy
def test_load_config_parses_clean_promotion_config(tmp_path: Path):
    yml = tmp_path / "dataset.yml"
    yml.write_text(
        """
root: null
dataset:
  name: demo
  years: [2024]
raw: {}
clean:
  validate:
    promotion:
      max_row_drop_pct: 8.5
      warn_removed_columns: "false"
mart: {}
""".strip(),
        encoding="utf-8",
    )

    cfg = load_config(yml)

    assert cfg.clean["validate"]["promotion"] == {
        "max_row_drop_pct": 8.5,
        "warn_removed_columns": False,
    }


@pytest.mark.contract
def test_load_config_model_rejects_invalid_mart_transition_bool(tmp_path: Path):
    yml = tmp_path / "dataset.yml"
    yml.write_text(
        """
root: null
dataset:
  name: demo
  years: [2024]
raw: {}
clean: {}
mart:
  validate:
    transition:
      warn_removed_columns: "maybe"
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError) as e:
        load_config_model(yml)

    assert "mart.validate.transition.warn_removed_columns" in str(e.value)


@pytest.mark.contract
def test_load_config_missing_dataset_name(tmp_path: Path):
    yml = tmp_path / "dataset.yml"
    yml.write_text(
        """
dataset:
  years: [2022]
raw: {}
clean: {}
mart: {}
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError) as e:
        load_config(yml)

    assert "dataset.name" in str(e.value)


@pytest.mark.policy
def test_load_config_resolves_relative_paths_from_dataset_dir(tmp_path: Path):
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    (project_dir / "sql" / "mart").mkdir(parents=True)
    (project_dir / "sql" / "cross").mkdir(parents=True)

    yml = project_dir / "dataset.yml"
    yml.write_text(
        """
root: "./out"
dataset:
  name: demo
  years: [2022]
raw:
  sources:
    - type: local_file
      args:
        path: "data/raw.csv"
clean:
  sql: "sql/clean.sql"
mart:
  tables:
    - name: demo_mart
      sql: "sql/mart/demo.sql"
cross_year:
  tables:
    - name: demo_cross
      sql: "sql/cross/demo_cross.sql"
      source_layer: clean
""".strip(),
        encoding="utf-8",
    )

    cfg = load_config(yml)

    assert cfg.base_dir == project_dir.resolve()
    assert cfg.root == (project_dir / "out").resolve()
    assert cfg.root_source == "yml"
    assert cfg.raw.sources[0].args["path"] == (project_dir / "data" / "raw.csv").resolve()
    assert cfg.clean.sql == (project_dir / "sql" / "clean.sql").resolve()
    assert cfg.mart.tables[0].sql == (project_dir / "sql" / "mart" / "demo.sql").resolve()
    assert cfg.cross_year.tables[0].sql == (project_dir / "sql" / "cross" / "demo_cross.sql").resolve()


@pytest.mark.policy
def test_load_config_resolves_support_config_paths_from_dataset_dir(tmp_path: Path):
    project_dir = tmp_path / "project"
    support_dir = tmp_path / "support"
    project_dir.mkdir()
    support_dir.mkdir()

    yml = project_dir / "dataset.yml"
    yml.write_text(
        """
root: "./out"
dataset:
  name: demo
  years: [2022]
raw: {}
clean: {}
mart: {}
support:
  - name: scuole
    config: "../support/dataset.yml"
    years: [2024]
""".strip(),
        encoding="utf-8",
    )

    cfg = load_config(yml)

    assert cfg.support == [
        {
            "name": "scuole",
            "config": (support_dir / "dataset.yml").resolve(),
            "years": [2024],
        }
    ]


@pytest.mark.contract
def test_load_config_rejects_duplicate_support_names(tmp_path: Path):
    yml = tmp_path / "dataset.yml"
    yml.write_text(
        """
root: "./out"
dataset:
  name: demo
  years: [2022]
raw: {}
clean: {}
mart: {}
support:
  - name: scuole
    config: "./support_a.yml"
    years: [2024]
  - name: scuole
    config: "./support_b.yml"
    years: [2025]
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError) as e:
        load_config(yml)

    assert "support[].name values must be unique" in str(e.value)


@pytest.mark.policy
def test_load_config_does_not_transform_non_whitelisted_path_like_fields(tmp_path: Path):
    project_dir = tmp_path / "project"
    project_dir.mkdir()

    yml = project_dir / "dataset.yml"
    yml.write_text(
        """
root: "./out"
dataset:
  name: demo
  years: [2022]
raw:
  sources:
    - type: local_file
      args:
        path: "data/raw.csv"
        filename: "nested/raw.csv"
clean:
  sql: "sql/clean.sql"
  note_path: "docs/clean.md"
mart:
  tables:
    - name: demo_mart
      sql: "sql/mart/demo.sql"
  label_path: "labels/mart.txt"
""".strip(),
        encoding="utf-8",
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
    yml.write_text(
        """
root: "./out"
dataset:
  name: demo
  years: [2022, 2023]
raw:
  sources:
    - type: local_file
      args:
        path: "data/raw_{year}.csv"
        filename: "raw_{year}.csv"
clean: {}
mart: {}
""".strip(),
        encoding="utf-8",
    )

    cfg = load_config(yml)

    assert cfg.raw.sources[0].args["path"] == str((project_dir / "data" / "raw_{year}.csv").resolve())
    assert cfg.raw.sources[0].args["filename"] == "raw_{year}.csv"


@pytest.mark.policy
def test_load_config_uses_dcl_root_when_root_missing(tmp_path: Path, monkeypatch):
    project_dir = tmp_path / "project"
    project_dir.mkdir()

    yml = project_dir / "dataset.yml"
    yml.write_text(
        """
dataset:
  name: demo
  years: [2022]
raw: {}
clean: {}
mart: {}
""".strip(),
        encoding="utf-8",
    )

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
    yml.write_text(
        """
root: "./_smoke_out"
dataset:
  name: demo
  years: [2022]
raw: {}
clean: {}
mart: {}
""".strip(),
        encoding="utf-8",
    )

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
    yml.write_text(
        """
dataset:
  name: demo
  years: [2022]
raw: {}
clean: {}
mart: {}
""".strip(),
        encoding="utf-8",
    )

    monkeypatch.delenv("DCL_ROOT", raising=False)

    cfg = load_config(yml)

    assert cfg.root == project_dir.resolve()
    assert cfg.root_source == "base_dir_fallback"


@pytest.mark.policy
@pytest.mark.parametrize(
    ("dataset_rel", "root_value"),
    [
        ("candidates/demo_dataset", "../../out"),
        ("candidates/demo_dataset/sources/demo_source", "../../../../out"),
        ("support_datasets/demo_support", "../../out"),
    ],
)
def test_load_config_resolves_repo_out_for_dataset_incubator_layouts(
    tmp_path: Path,
    dataset_rel: str,
    root_value: str,
):
    repo_root = tmp_path / "dataset-incubator"
    dataset_dir = repo_root / Path(dataset_rel)
    dataset_dir.mkdir(parents=True, exist_ok=True)
    yml = dataset_dir / "dataset.yml"
    yml.write_text(
        f"""
root: "{root_value}"
dataset:
  name: demo
  years: [2022]
raw: {{}}
clean: {{}}
mart: {{}}
""".strip(),
        encoding="utf-8",
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
    yml.write_text(
        f"""
root: "{allowed_root.as_posix()}"
dataset:
  name: demo
  years: [2022]
raw: {{}}
clean: {{}}
mart: {{}}
""".strip(),
        encoding="utf-8",
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
    yml.write_text(
        f"""
root: "{outside_root.as_posix()}"
dataset:
  name: demo
  years: [2022]
raw: {{}}
clean: {{}}
mart: {{}}
""".strip(),
        encoding="utf-8",
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
    yml.write_text(
        """
root: "../../../outside"
dataset:
  name: demo
  years: [2022]
raw: {}
clean: {}
mart: {}
""".strip(),
        encoding="utf-8",
    )

    cfg = load_config(yml)

    assert cfg.root == (tmp_path / "outside").resolve()


@pytest.mark.contract
def test_project_example_config_parses_in_strict_mode():
    model = load_config_model(Path("project-example") / "dataset.yml", strict_config=True)

    assert model.dataset.name == "project_example"
    assert len(model.raw.sources) == 1


@pytest.mark.contract
def test_mart_required_tables_auto_filled_from_tables(tmp_path: Path):
    """When required_tables is omitted, it defaults to all table names from tables."""
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    yml = project_dir / "dataset.yml"
    yml.write_text(
        """
dataset:
  name: demo
  years: [2022]
raw: {}
clean: {}
mart:
  tables:
    - name: table_a
      sql: sql/mart/a.sql
    - name: table_b
      sql: sql/mart/b.sql
""".strip(),
        encoding="utf-8",
    )
    cfg = load_config(yml)
    assert cfg.mart.tables[0].name == "table_a"
    assert cfg.mart.tables[1].name == "table_b"
    # Auto-filled because not specified
    assert cfg.mart.required_tables == ["table_a", "table_b"]


@pytest.mark.contract
def test_mart_required_tables_explicit_empty_auto_fills(tmp_path: Path):
    """When required_tables is set to [], it auto-fills to all table names.

    Note: YAML cannot distinguish "field absent" from "field set to []".
    The semantic is: if you declare tables, you want them required.
    To opt out, simply don't declare any tables in mart.tables.
    """
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    yml = project_dir / "dataset.yml"
    yml.write_text(
        """
dataset:
  name: demo
  years: [2022]
raw: {}
clean: {}
mart:
  tables:
    - name: table_a
      sql: sql/mart/a.sql
  required_tables: []
""".strip(),
        encoding="utf-8",
    )
    cfg = load_config(yml)
    # Empty required_tables (absent or explicit []) auto-fills to all table names
    assert cfg.mart.required_tables == ["table_a"]


@pytest.mark.contract
def test_mart_required_tables_explicit_subset(tmp_path: Path):
    """When required_tables is explicitly set, it is used as-is (no auto-fill)."""
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    yml = project_dir / "dataset.yml"
    yml.write_text(
        """
dataset:
  name: demo
  years: [2022]
raw: {}
clean: {}
mart:
  tables:
    - name: table_a
      sql: sql/mart/a.sql
    - name: table_b
      sql: sql/mart/b.sql
  required_tables:
    - table_a
""".strip(),
        encoding="utf-8",
    )
    cfg = load_config(yml)
    # Explicit list is used
    assert cfg.mart.required_tables == ["table_a"]
