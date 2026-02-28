from pathlib import Path

from toolkit.core.metadata import config_hash_for_year


def test_config_hash_for_year_is_stable_across_working_directories(tmp_path: Path, monkeypatch) -> None:
    base_dir = tmp_path / "project"
    base_dir.mkdir(parents=True, exist_ok=True)
    (base_dir / "dataset.yml").write_text(
        "dataset:\n  name: demo\n  years: [2024]\nclean:\n  sql: sql/clean.sql\n",
        encoding="utf-8",
    )

    first_cwd = tmp_path / "cwd-one"
    second_cwd = tmp_path / "cwd-two"
    first_cwd.mkdir()
    second_cwd.mkdir()

    monkeypatch.chdir(first_cwd)
    first = config_hash_for_year(base_dir, 2024)

    monkeypatch.chdir(second_cwd)
    second = config_hash_for_year(base_dir, 2024)

    assert first is not None
    assert first == second


def test_config_hash_for_year_changes_when_dataset_yml_changes(tmp_path: Path) -> None:
    base_dir = tmp_path / "project"
    base_dir.mkdir(parents=True, exist_ok=True)
    config_path = base_dir / "dataset.yml"
    config_path.write_text(
        "dataset:\n  name: demo\n  years: [2024]\nclean:\n  sql: sql/clean.sql\n",
        encoding="utf-8",
    )

    original = config_hash_for_year(base_dir, 2024)

    config_path.write_text(
        "dataset:\n  name: demo_changed\n  years: [2024]\nclean:\n  sql: sql/clean.sql\n",
        encoding="utf-8",
    )

    updated = config_hash_for_year(base_dir, 2024)

    assert original is not None
    assert updated is not None
    assert original != updated
