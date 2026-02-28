from pathlib import Path

import logging
import pytest

from toolkit.core.config import ensure_str_list, load_config, parse_bool
from toolkit.core.config_models import load_config_model


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

    yml = project_dir / "dataset.yml"
    yml.write_text(
        """
root: "./out"
dataset:
  name: demo
  years: [2022]
raw:
  source:
    type: local_file
    args:
      path: "data/raw.csv"
clean:
  sql: "sql/clean.sql"
mart:
  tables:
    - name: demo_mart
      sql: "sql/mart/demo.sql"
""".strip(),
        encoding="utf-8",
    )

    cfg = load_config(yml)

    assert cfg.base_dir == project_dir.resolve()
    assert cfg.root == (project_dir / "out").resolve()
    assert cfg.root_source == "yml"
    assert cfg.raw["source"]["args"]["path"] == (project_dir / "data" / "raw.csv").resolve()
    assert cfg.clean["sql"] == (project_dir / "sql" / "clean.sql").resolve()
    assert cfg.mart["tables"][0]["sql"] == (project_dir / "sql" / "mart" / "demo.sql").resolve()


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
  source:
    type: local_file
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

    assert cfg.raw["source"]["args"]["path"] == (project_dir / "data" / "raw.csv").resolve()
    assert cfg.raw["source"]["args"]["filename"] == "nested/raw.csv"
    assert cfg.clean["note_path"] == "docs/clean.md"
    assert cfg.mart["label_path"] == "labels/mart.txt"


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
  sql_dir: "sql/mart"
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
    assert cfg.mart["sql_dir"] == (project_dir / "sql" / "mart").resolve()
    assert cfg.mart["tables"][0]["sql"] == (project_dir / "sql" / "mart" / "demo.sql").resolve()

    assert "Normalized config paths:" in caplog.text
    assert "root=" in caplog.text
    assert "raw.sources[0].args.path=" in caplog.text
    assert "clean.sql=" in caplog.text
    assert "mart.sql_dir=" in caplog.text
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


def test_load_config_normalizes_legacy_clean_read_csv_and_warns(tmp_path: Path, caplog):
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

    module_logger = logging.getLogger("toolkit.core.config")
    module_logger.handlers = [caplog.handler]
    module_logger.propagate = True
    module_logger.setLevel(logging.WARNING)

    with caplog.at_level(logging.WARNING, logger="toolkit.core.config"):
        cfg = load_config(yml)

    assert cfg.clean["read"] == {
        "source": "auto",
        "columns": {"amount": "DOUBLE"},
        "delim": ";",
    }
    assert "clean.read.csv.* is deprecated" in caplog.text
    assert "Migrate to clean.read.source / clean.read.columns" in caplog.text


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


def test_load_config_warns_on_zombie_fields(tmp_path: Path, caplog):
    yml = tmp_path / "dataset.yml"
    yml.write_text(
        """
dataset:
  name: demo
  years: [2022]
raw: {}
clean:
  sql_path: sql/legacy_clean.sql
mart:
  sql_dir: sql/mart
bq:
  dataset: ignored
""".strip(),
        encoding="utf-8",
    )

    with caplog.at_level(logging.WARNING, logger="toolkit.core.config"):
        load_config(yml)

    assert "clean.sql_path is ignored" in caplog.text
    assert "mart.sql_dir is ignored" in caplog.text
    assert "bq is currently ignored" in caplog.text


def test_load_config_model_normalizes_legacy_aliases_to_canonical_shape(tmp_path: Path):
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
clean:
  read: auto
mart: {}
""".strip(),
        encoding="utf-8",
    )

    model = load_config_model(yml)

    assert len(model.raw.sources) == 1
    assert model.raw.sources[0].name == "src_legacy"
    assert model.raw.sources[0].type == "local_file"
    assert model.clean.read is not None
    assert model.clean.read.source == "auto"


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
