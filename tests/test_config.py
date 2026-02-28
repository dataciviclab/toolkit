from pathlib import Path

import logging
import pytest

from toolkit.core.config import load_config


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
