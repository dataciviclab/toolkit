from pathlib import Path

import logging
import pytest

from toolkit.core.config import ensure_str_list, load_config, parse_bool
from toolkit.core.config_models import load_config_model


def _bind_config_logger(caplog, monkeypatch):
    module_logger = logging.getLogger("toolkit.core.config")
    monkeypatch.setattr(module_logger, "handlers", [caplog.handler])
    monkeypatch.setattr(module_logger, "propagate", False)
    module_logger.setLevel(logging.WARNING)
    caplog.set_level(logging.WARNING, logger="toolkit.core.config")


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
    assert cfg.raw["sources"][0]["args"]["path"] == (project_dir / "data" / "raw.csv").resolve()
    assert cfg.clean["sql"] == (project_dir / "sql" / "clean.sql").resolve()
    assert cfg.mart["tables"][0]["sql"] == (project_dir / "sql" / "mart" / "demo.sql").resolve()
    assert cfg.cross_year["tables"][0]["sql"] == (project_dir / "sql" / "cross" / "demo_cross.sql").resolve()


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

    assert cfg.raw["sources"][0]["args"]["path"] == (project_dir / "data" / "raw.csv").resolve()
    assert cfg.raw["sources"][0]["args"]["filename"] == "nested/raw.csv"
    assert cfg.clean["note_path"] == "docs/clean.md"
    assert cfg.mart["label_path"] == "labels/mart.txt"


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

    assert cfg.raw["sources"][0]["args"]["path"] == str((project_dir / "data" / "raw_{year}.csv").resolve())
    assert cfg.raw["sources"][0]["args"]["filename"] == "raw_{year}.csv"


def test_load_config_logs_normalized_whitelist_fields(tmp_path: Path, caplog, monkeypatch):
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    (project_dir / "sql" / "mart").mkdir(parents=True)

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
        path: "data/raw_a.csv"
clean:
  sql: "sql/clean.sql"
mart:
  tables:
    - name: demo_mart
      sql: "sql/mart/demo.sql"
""".strip(),
        encoding="utf-8",
    )

    module_logger = logging.getLogger("toolkit.core.config")
    monkeypatch.setattr(module_logger, "handlers", [caplog.handler])
    monkeypatch.setattr(module_logger, "propagate", False)
    module_logger.setLevel(logging.DEBUG)
    caplog.set_level(logging.DEBUG, logger="toolkit.core.config")

    with caplog.at_level(logging.DEBUG, logger="toolkit.core.config"):
        cfg = load_config(yml)

    assert cfg.root == (project_dir / "out").resolve()
    assert cfg.raw["sources"][0]["args"]["path"] == (project_dir / "data" / "raw_a.csv").resolve()
    assert cfg.clean["sql"] == (project_dir / "sql" / "clean.sql").resolve()
    assert cfg.mart["tables"][0]["sql"] == (project_dir / "sql" / "mart" / "demo.sql").resolve()

    assert "Normalized config paths:" in caplog.text
    assert "root=" in caplog.text
    assert "raw.sources[0].args.path=" in caplog.text
    assert "clean.sql=" in caplog.text
    assert "mart.tables[0].sql=" in caplog.text


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


def test_load_config_rejects_legacy_clean_read_csv_shape(tmp_path: Path):
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    yml = project_dir / "dataset.yml"
    yml.write_text(
        """
dataset:
  name: demo
  years: [2022]
raw: {}
clean:
  read:
    source: auto
    csv:
      columns:
        amount: DOUBLE
      delim: ";"
mart: {}
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError) as exc:
        load_config(yml)

    assert "clean.read.csv" in str(exc.value)


def test_load_config_canonical_clean_read_has_no_deprecation_warning(tmp_path: Path, caplog):
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    yml = project_dir / "dataset.yml"
    yml.write_text(
        """
dataset:
  name: demo
  years: [2022]
raw: {}
clean:
  read:
    source: auto
    columns:
      amount: DOUBLE
    delim: ";"
mart: {}
""".strip(),
        encoding="utf-8",
    )

    with caplog.at_level(logging.WARNING, logger="toolkit.core.config"):
        cfg = load_config(yml)

    assert cfg.clean["read"] == {
        "source": "auto",
        "columns": {"amount": "DOUBLE"},
        "delim": ";",
    }
    assert "clean.read.csv.* is deprecated" not in caplog.text


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (True, True),
        (False, False),
        ("true", True),
        ("false", False),
        ("1", True),
        ("0", False),
        (1, True),
        (0, False),
        ("yes", True),
        ("no", False),
    ],
)
def test_parse_bool_accepts_supported_boolean_like_values(value, expected):
    assert parse_bool(value, "field") is expected


def test_parse_bool_rejects_unsupported_value():
    with pytest.raises(ValueError):
        parse_bool("maybe", "field")


def test_ensure_str_list_accepts_single_string_and_list():
    assert ensure_str_list("col_a", "field") == ["col_a"]
    assert ensure_str_list(["col_a", "col_b"], "field") == ["col_a", "col_b"]


def test_ensure_str_list_rejects_non_string_items():
    with pytest.raises(ValueError):
        ensure_str_list(["col_a", 2], "field")


def test_load_config_normalizes_bool_and_string_list_fields(tmp_path: Path):
    yml = tmp_path / "dataset.yml"
    yml.write_text(
        """
dataset:
  name: demo
  years: [2022]
raw:
  sources:
    - type: http_file
      primary: "false"
clean:
  required_columns: comune
  validate:
    primary_key: id
    not_null: valore
mart:
  required_tables: mart_ok
  validate:
    table_rules:
      mart_ok:
        required_columns: regione
        not_null: totale
        primary_key: key_id
validation:
  fail_on_error: "false"
output:
  legacy_aliases: "0"
""".strip(),
        encoding="utf-8",
    )

    cfg = load_config(yml)

    assert cfg.validation["fail_on_error"] is False
    assert cfg.output["legacy_aliases"] is False
    assert cfg.raw["sources"][0]["primary"] is False
    assert cfg.clean["required_columns"] == ["comune"]
    assert cfg.clean["validate"]["primary_key"] == ["id"]
    assert cfg.clean["validate"]["not_null"] == ["valore"]
    assert cfg.mart["required_tables"] == ["mart_ok"]
    assert cfg.mart["validate"]["table_rules"]["mart_ok"]["required_columns"] == ["regione"]
    assert cfg.mart["validate"]["table_rules"]["mart_ok"]["not_null"] == ["totale"]
    assert cfg.mart["validate"]["table_rules"]["mart_ok"]["primary_key"] == ["key_id"]


def test_load_config_rejects_removed_bq_field(tmp_path: Path):
    yml = tmp_path / "dataset.yml"
    yml.write_text(
        """
dataset:
  name: demo
  years: [2022]
raw: {}
bq:
  dataset: ignored
clean: {}
mart: {}
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError) as exc:
        load_config(yml)

    assert "bq is no longer supported; remove field" in str(exc.value)


def test_load_config_rejects_clean_sql_path(tmp_path: Path):
    yml = tmp_path / "dataset.yml"
    yml.write_text(
        """
dataset:
  name: demo
  years: [2022]
raw: {}
clean:
  sql_path: sql/legacy_clean.sql
mart: {}
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError) as exc:
        load_config(yml)

    assert "clean.sql_path" in str(exc.value)


def test_load_config_rejects_mart_sql_dir(tmp_path: Path):
    yml = tmp_path / "dataset.yml"
    yml.write_text(
        """
dataset:
  name: demo
  years: [2022]
raw: {}
clean: {}
mart:
  sql_dir: sql/mart
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError) as exc:
        load_config(yml)

    assert "mart.sql_dir" in str(exc.value)


def test_load_config_model_rejects_legacy_raw_source_plugin_id_shape(tmp_path: Path):
    yml = tmp_path / "dataset.yml"
    yml.write_text(
        """
dataset:
  name: demo
  years: [2022]
raw:
  source:
    id: src_legacy
    plugin: local_file
    args:
      path: data/raw.csv
clean: {}
mart: {}
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError) as exc:
        load_config_model(yml)

    assert "raw.sources" in str(exc.value) or "raw.source" in str(exc.value)


def test_load_config_model_rejects_legacy_raw_sources_plugin_id_fields(tmp_path: Path):
    yml = tmp_path / "dataset.yml"
    yml.write_text(
        """
dataset:
  name: demo
  years: [2022]
raw:
  sources:
    - id: src_legacy
      plugin: local_file
      args:
        path: data/raw.csv
clean: {}
mart: {}
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError) as exc:
        load_config_model(yml)

    assert "raw.sources.0" in str(exc.value)


def test_load_config_rejects_legacy_clean_read_scalar_form(tmp_path: Path):
    yml = tmp_path / "dataset.yml"
    yml.write_text(
        """
dataset:
  name: demo
  years: [2022]
clean:
  read: auto
mart: {}
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError) as exc:
        load_config(yml)

    assert "clean.read" in str(exc.value)


def test_project_example_config_parses_in_strict_mode():
    model = load_config_model(Path("project-example") / "dataset.yml", strict_config=True)

    assert model.dataset.name == "project_example"
    assert len(model.raw.sources) == 1


def test_load_config_warns_on_unknown_top_level_keys_in_non_strict_mode(tmp_path: Path, caplog, monkeypatch):
    yml = tmp_path / "dataset.yml"
    yml.write_text(
        """
dataset:
  name: demo
  years: [2022]
raw: {}
clean: {}
mart: {}
unknown_top: true
""".strip(),
        encoding="utf-8",
    )

    _bind_config_logger(caplog, monkeypatch)

    with caplog.at_level(logging.WARNING, logger="toolkit.core.config"):
        cfg = load_config(yml)

    assert cfg.dataset == "demo"
    assert "DCL009" in caplog.text
    assert "unknown top-level config keys detected: unknown_top" in caplog.text


def test_load_config_model_rejects_unknown_top_level_keys_in_strict_mode(tmp_path: Path):
    yml = tmp_path / "dataset.yml"
    yml.write_text(
        """
dataset:
  name: demo
  years: [2022]
raw: {}
clean: {}
mart: {}
unknown_top: true
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError) as exc:
        load_config_model(yml, strict_config=True)

    assert "DCL009" in str(exc.value)
    assert "unknown_top" in str(exc.value)


@pytest.mark.parametrize(
    ("section", "yaml_text", "code", "extra_key"),
    [
        (
            "raw",
            """
dataset:
  name: demo
  years: [2022]
raw:
  unexpected_flag: true
clean: {}
mart: {}
""".strip(),
            "DCL010",
            "unexpected_flag",
        ),
        (
            "clean",
            """
dataset:
  name: demo
  years: [2022]
raw: {}
clean:
  unexpected_flag: true
mart: {}
""".strip(),
            "DCL011",
            "unexpected_flag",
        ),
        (
            "mart",
            """
dataset:
  name: demo
  years: [2022]
raw: {}
clean: {}
mart:
  unexpected_flag: true
""".strip(),
            "DCL012",
            "unexpected_flag",
        ),
    ],
)
def test_load_config_warns_on_unknown_section_keys_in_non_strict_mode(
    tmp_path: Path,
    caplog,
    monkeypatch,
    section: str,
    yaml_text: str,
    code: str,
    extra_key: str,
):
    yml = tmp_path / "dataset.yml"
    yml.write_text(yaml_text, encoding="utf-8")

    _bind_config_logger(caplog, monkeypatch)

    with caplog.at_level(logging.WARNING, logger="toolkit.core.config"):
        cfg = load_config(yml)

    assert getattr(cfg, section) is not None
    assert code in caplog.text
    assert extra_key in caplog.text


@pytest.mark.parametrize(
    ("yaml_text", "code", "extra_key"),
    [
        (
            """
dataset:
  name: demo
  years: [2022]
raw:
  unexpected_flag: true
clean: {}
mart: {}
""".strip(),
            "DCL010",
            "unexpected_flag",
        ),
        (
            """
dataset:
  name: demo
  years: [2022]
raw: {}
clean:
  unexpected_flag: true
mart: {}
""".strip(),
            "DCL011",
            "unexpected_flag",
        ),
        (
            """
dataset:
  name: demo
  years: [2022]
raw: {}
clean: {}
mart:
  unexpected_flag: true
""".strip(),
            "DCL012",
            "unexpected_flag",
        ),
    ],
)
def test_load_config_model_rejects_unknown_section_keys_in_strict_mode(
    tmp_path: Path,
    yaml_text: str,
    code: str,
    extra_key: str,
):
    yml = tmp_path / "dataset.yml"
    yml.write_text(yaml_text, encoding="utf-8")

    with pytest.raises(ValueError) as exc:
        load_config_model(yml, strict_config=True)

    assert code in str(exc.value)
    assert extra_key in str(exc.value)


@pytest.mark.parametrize(
    ("yaml_text", "expected"),
    [
        (
            """
dataset:
  name: demo
  years: [2022]
raw:
  sources:
    - type: http_file
      client: "bad"
clean: {}
mart: {}
""".strip(),
            "raw.sources.0.client",
        ),
        (
            """
dataset:
  name: demo
  years: [2022]
raw:
  extractor:
    type: identity
    args: []
clean: {}
mart: {}
""".strip(),
            "raw.extractor.args",
        ),
    ],
)
def test_load_config_model_rejects_wrong_shape_for_typed_subsections(
    tmp_path: Path,
    yaml_text: str,
    expected: str,
):
    yml = tmp_path / "dataset.yml"
    yml.write_text(yaml_text, encoding="utf-8")

    with pytest.raises(ValueError) as exc:
        load_config_model(yml, strict_config=True)

    assert expected in str(exc.value)


@pytest.mark.parametrize(
    ("yaml_text", "expected"),
    [
        (
            """
dataset:
  name: demo
  years: [2022]
raw:
  sources: {}
clean: {}
mart: {}
""".strip(),
            "raw.sources",
        ),
        (
            """
dataset:
  name: demo
  years: [2022]
raw:
  sources:
    - type: local_file
      args: []
clean: {}
mart: {}
""".strip(),
            "raw.sources.0.args",
        ),
        (
            """
root: 123
dataset:
  name: demo
  years: [2022]
raw: {}
clean: {}
mart: {}
""".strip(),
            "root must be a string path or null",
        ),
        (
            """
dataset:
  name: demo
  years: [2022]
raw: {}
clean: {}
mart: {}
output:
  artifacts: standard
  unsupported_flag: true
""".strip(),
            "output.unsupported_flag",
        ),
        (
            """
dataset:
  name: demo
  years: [2022]
raw: {}
clean: {}
mart: {}
validation:
  fail_on_error: true
  unknown_flag: false
""".strip(),
            "validation.unknown_flag",
        ),
        (
            """
dataset:
  name: demo
  years: [2022]
raw: {}
clean:
  validate:
    primary_key: id
    extra_rule: true
mart: {}
""".strip(),
            "clean.validate.extra_rule",
        ),
        (
            """
dataset:
  name: demo
  years: [2022]
raw: {}
clean: {}
mart:
  validate:
    table_rules: {}
    extra_rule: true
""".strip(),
            "mart.validate.extra_rule",
        ),
        (
            """
dataset:
  name: demo
  years: [2022]
raw:
  sources:
    - type: http_file
      primary: maybe
clean: {}
mart: {}
""".strip(),
            "raw.sources.0.primary",
        ),
    ],
)
def test_load_config_model_errors_are_explicit(tmp_path: Path, yaml_text: str, expected: str):
    yml = tmp_path / "dataset.yml"
    yml.write_text(yaml_text, encoding="utf-8")

    with pytest.raises(ValueError) as exc:
        load_config_model(yml)

    assert expected in str(exc.value)


def test_load_config_model_accepts_boolean_and_string_list_legacy_inputs(tmp_path: Path):
    yml = tmp_path / "dataset.yml"
    yml.write_text(
        """
dataset:
  name: demo
  years: [2022]
raw:
  sources:
    - type: http_file
      primary: "false"
clean:
  required_columns: comune
  validate:
    primary_key: id
    not_null: valore
mart:
  required_tables: mart_ok
  validate:
    table_rules:
      mart_ok:
        required_columns: regione
        not_null: totale
        primary_key: key_id
validation:
  fail_on_error: "false"
output:
  legacy_aliases: "0"
""".strip(),
        encoding="utf-8",
    )

    model = load_config_model(yml)

    assert model.validation.fail_on_error is False
    assert model.output.legacy_aliases is False
    assert model.raw.sources[0].primary is False
    assert model.clean.required_columns == ["comune"]
    assert model.clean.validate.primary_key == ["id"]
    assert model.clean.validate.not_null == ["valore"]
    assert model.mart.required_tables == ["mart_ok"]
    assert model.mart.validate.table_rules["mart_ok"].required_columns == ["regione"]
