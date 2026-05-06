"""Tests for config: legacy field handling, deprecation warnings, strict mode validation."""

from pathlib import Path

import logging
import pytest

from toolkit.core.config import load_config
from toolkit.core.config_models import load_config_model


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

YAML_BASE = {
    "dataset": {"name": "demo", "years": [2022]},
    "raw": {},
    "clean": {},
    "mart": {},
}


def _yml(path: Path, **overrides) -> Path:
    """Write a dataset.yml merging YAML_BASE with per-test overrides.

    Top-level keys in overrides replace their YAML_BASE counterparts.
    """
    import copy
    import yaml

    merged = copy.deepcopy(YAML_BASE)
    merged.update(overrides)
    path.write_text(yaml.safe_dump(merged, sort_keys=False), encoding="utf-8")
    return path


def _yml_str(path: Path, body: str) -> Path:
    """Write a dataset.yml from an explicit multi-line YAML string.

    Use for complex nested structures that are easier to express inline.
    """
    path.write_text(body.strip() + "\n", encoding="utf-8")
    return path


def _bind_config_logger(caplog, monkeypatch):
    module_logger = logging.getLogger("toolkit.core.config")
    monkeypatch.setattr(module_logger, "handlers", [caplog.handler])
    monkeypatch.setattr(module_logger, "propagate", False)
    module_logger.setLevel(logging.WARNING)
    caplog.set_level(logging.WARNING, logger="toolkit.core.config")


def test_load_config_rejects_legacy_clean_read_csv_shape(tmp_path: Path):
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    yml = project_dir / "dataset.yml"
    _yml_str(
        yml,
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
""",
    )

    with pytest.raises(ValueError) as exc:
        load_config(yml)

    assert "clean.read.csv" in str(exc.value)


def test_load_config_canonical_clean_read_has_no_deprecation_warning(tmp_path: Path, caplog):
    yml = tmp_path / "dataset.yml"
    _yml_str(
        yml,
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
""",
    )

    with caplog.at_level(logging.WARNING, logger="toolkit.core.config"):
        cfg = load_config(yml)

    assert cfg.clean["read"] == {
        "source": "auto",
        "columns": {"amount": "DOUBLE"},
        "delim": ";",
    }
    assert "clean.read.csv.* is deprecated" not in caplog.text


def test_load_config_normalizes_bool_and_string_list_fields(tmp_path: Path):
    yml = tmp_path / "dataset.yml"
    _yml_str(
        yml,
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
""",
    )

    cfg = load_config(yml)

    assert cfg.validation.fail_on_error is False
    assert cfg.output.legacy_aliases is False
    assert cfg.raw.sources[0].primary is False
    assert cfg.clean.required_columns == ["comune"]
    assert cfg.clean.validate.primary_key == ["id"]
    assert cfg.clean.validate.not_null == ["valore"]
    assert cfg.mart.required_tables == ["mart_ok"]
    assert cfg.mart.validate.table_rules["mart_ok"].required_columns == ["regione"]
    assert cfg.mart.validate.table_rules["mart_ok"].not_null == ["totale"]
    assert cfg.mart.validate.table_rules["mart_ok"].primary_key == ["key_id"]


def test_load_config_rejects_removed_bq_field(tmp_path: Path):
    yml = tmp_path / "dataset.yml"
    _yml(yml, bq={"dataset": "ignored"})

    with pytest.raises(ValueError) as exc:
        load_config(yml)

    assert "bq is no longer supported; remove field" in str(exc.value)


def test_load_config_rejects_clean_sql_path(tmp_path: Path):
    yml = tmp_path / "dataset.yml"
    _yml(yml, clean={"sql_path": "sql/legacy_clean.sql"})

    with pytest.raises(ValueError) as exc:
        load_config(yml)

    assert "clean.sql_path" in str(exc.value)


def test_load_config_rejects_mart_sql_dir(tmp_path: Path):
    yml = tmp_path / "dataset.yml"
    _yml(yml, mart={"sql_dir": "sql/mart"})

    with pytest.raises(ValueError) as exc:
        load_config(yml)

    assert "mart.sql_dir" in str(exc.value)


def test_load_config_model_rejects_legacy_raw_source_plugin_id_shape(tmp_path: Path):
    yml = tmp_path / "dataset.yml"
    _yml_str(
        yml,
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
""",
    )

    with pytest.raises(ValueError) as exc:
        load_config_model(yml)

    assert "raw.sources" in str(exc.value) or "raw.source" in str(exc.value)


def test_load_config_model_rejects_legacy_raw_sources_plugin_id_fields(tmp_path: Path):
    yml = tmp_path / "dataset.yml"
    _yml_str(
        yml,
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
""",
    )

    with pytest.raises(ValueError) as exc:
        load_config_model(yml)

    assert "raw.sources.0" in str(exc.value)


def test_load_config_rejects_legacy_clean_read_scalar_form(tmp_path: Path):
    yml = tmp_path / "dataset.yml"
    _yml(yml, clean={"read": "auto"})

    with pytest.raises(ValueError) as exc:
        load_config(yml)

    assert "clean.read" in str(exc.value)


def test_load_config_warns_on_unknown_top_level_keys_in_non_strict_mode(tmp_path: Path, caplog, monkeypatch):
    yml = tmp_path / "dataset.yml"
    _yml(yml, unknown_top=True)

    _bind_config_logger(caplog, monkeypatch)

    with caplog.at_level(logging.WARNING, logger="toolkit.core.config"):
        cfg = load_config(yml)

    assert cfg.dataset == "demo"
    assert "DCL009" in caplog.text
    assert "unknown top-level config keys detected: unknown_top" in caplog.text


def test_load_config_model_rejects_unknown_top_level_keys_in_strict_mode(tmp_path: Path):
    yml = tmp_path / "dataset.yml"
    _yml(yml, unknown_top=True)

    with pytest.raises(ValueError) as exc:
        load_config_model(yml, strict_config=True)

    assert "DCL009" in str(exc.value)
    assert "unknown_top" in str(exc.value)


def test_load_config_model_rejects_non_mapping_config_block(tmp_path: Path):
    yml = tmp_path / "dataset.yml"
    _yml(yml, config=True)

    with pytest.raises(ValueError) as exc:
        load_config_model(yml)

    assert "config must be a mapping object if provided" in str(exc.value)


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
    _yml_str(
        yml,
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
""",
    )

    model = load_config_model(yml)

    assert model.validation.fail_on_error is False
    assert model.output.legacy_aliases is False
    assert model.raw.sources[0].primary is False
    assert model.clean.required_columns == ["comune"]
    assert model.clean.validate.primary_key == ["id"]
    assert model.clean.validate.not_null == ["valore"]
    assert model.mart.required_tables == ["mart_ok"]
