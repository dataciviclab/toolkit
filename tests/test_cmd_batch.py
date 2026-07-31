"""Tests for batch helper functions."""

from pathlib import Path

import pytest

from toolkit.cli.cmd_run import _read_config_list

pytestmark = pytest.mark.pure_unit


class TestReadConfigList:
    def test_file_not_found(self) -> None:
        with pytest.raises(FileNotFoundError) as exc_info:
            _read_config_list(Path("/nonexistent/path/configs.txt"))
        assert "Config list not found" in str(exc_info.value)

    def test_empty_file_raises(self, tmp_path: pytest.TempPathFactory) -> None:
        configs_file = tmp_path / "empty.txt"
        configs_file.write_text("", encoding="utf-8")
        with pytest.raises(ValueError) as exc_info:
            _read_config_list(configs_file)
        assert "No config paths found" in str(exc_info.value)

    def test_only_comments_raises(self, tmp_path: pytest.TempPathFactory) -> None:
        configs_file = tmp_path / "comments.txt"
        configs_file.write_text("# comment\n# another\n", encoding="utf-8")
        with pytest.raises(ValueError) as exc_info:
            _read_config_list(configs_file)
        assert "No config paths found" in str(exc_info.value)

    def test_only_blank_lines_raises(self, tmp_path: pytest.TempPathFactory) -> None:
        configs_file = tmp_path / "blanks.txt"
        configs_file.write_text("   \n\n  \n", encoding="utf-8")
        with pytest.raises(ValueError) as exc_info:
            _read_config_list(configs_file)
        assert "No config paths found" in str(exc_info.value)

    def test_single_absolute_path(self, tmp_path: pytest.TempPathFactory) -> None:
        configs_file = tmp_path / "single.txt"
        real_file = tmp_path / "real.yml"
        real_file.write_text("")
        configs_file.write_text(f"{real_file.absolute()}\n", encoding="utf-8")
        result = _read_config_list(configs_file)
        assert result == [real_file.absolute()]

    def test_relative_path_resolved_from_configs_parent(
        self, tmp_path: pytest.TempPathFactory
    ) -> None:
        configs_file = tmp_path / "configs.txt"
        dataset_dir = tmp_path / "datasets"
        dataset_dir.mkdir()
        dataset_file = dataset_dir / "dataset.yml"
        dataset_file.write_text("", encoding="utf-8")

        configs_file.write_text("datasets/dataset.yml\n", encoding="utf-8")
        result = _read_config_list(configs_file)
        assert result == [dataset_file.resolve()]

    def test_relative_path_resolved_from_cwd_first(
        self, tmp_path: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Se il path relativo esiste sia in CWD che nel parent del batch,
        viene usato quello della CWD."""
        # Setup: crea due file con lo stesso path relativo
        batch_parent = tmp_path / "batches"
        batch_parent.mkdir()
        configs_file = batch_parent / "configs.txt"

        # File in CWD (tmp_path / "project" / "dataset.yml")
        cwd_project = tmp_path / "project"
        cwd_project.mkdir()
        cwd_file = cwd_project / "dataset.yml"
        cwd_file.write_text("from_cwd", encoding="utf-8")

        # File in batch_parent (tmp_path / "batches" / "project" / "dataset.yml")
        batch_project = batch_parent / "project"
        batch_project.mkdir()
        batch_file = batch_project / "dataset.yml"
        batch_file.write_text("from_batch_parent", encoding="utf-8")

        configs_file.write_text("project/dataset.yml\n", encoding="utf-8")

        # Cambia CWD a tmp_path e verifica che prenda cwd_file
        monkeypatch.chdir(tmp_path)
        result = _read_config_list(configs_file)
        assert len(result) == 1
        assert result[0] == cwd_file.resolve()
        assert result[0].read_text() == "from_cwd"

    def test_relative_path_fallback_to_batch_parent(
        self, tmp_path: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Se il path relativo NON esiste in CWD, viene usato il parent del batch."""
        batch_parent = tmp_path / "batches"
        batch_parent.mkdir()
        configs_file = batch_parent / "configs.txt"

        # File solo nel batch_parent
        batch_project = batch_parent / "project"
        batch_project.mkdir()
        batch_file = batch_project / "dataset.yml"
        batch_file.write_text("from_batch_parent", encoding="utf-8")

        configs_file.write_text("project/dataset.yml\n", encoding="utf-8")

        # CWD è tmp_path dove NON esiste project/dataset.yml
        monkeypatch.chdir(tmp_path)
        result = _read_config_list(configs_file)
        assert len(result) == 1
        assert result[0] == batch_file.resolve()
        assert result[0].read_text() == "from_batch_parent"

    def test_multiple_paths_with_comments_and_blanks(
        self, tmp_path: pytest.TempPathFactory
    ) -> None:
        configs_file = tmp_path / "multi.txt"
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "a.yml").write_text("")
        (d2 / "b.yml").write_text("")

        configs_file.write_text(
            f"# first\n{d1.absolute() / 'a.yml'}\n\n  \n{d2.absolute() / 'b.yml'}\n# end",
            encoding="utf-8",
        )
        result = _read_config_list(configs_file)
        assert len(result) == 2

    def test_blank_lines_and_spaces_skipped(self, tmp_path: pytest.TempPathFactory) -> None:
        configs_file = tmp_path / "skipped.txt"
        d1 = tmp_path / "ds1"
        d1.mkdir()
        (d1 / "a.yml").write_text("")

        configs_file.write_text(
            f"   \n{d1.absolute() / 'a.yml'}\n     \n# comment\n",
            encoding="utf-8",
        )
        result = _read_config_list(configs_file)
        assert len(result) == 1
