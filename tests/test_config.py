from pathlib import Path

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