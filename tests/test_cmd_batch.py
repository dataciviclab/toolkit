"""Tests for toolkit/cli/cmd_batch.py — helper functions and failure cases."""

from pathlib import Path

import pytest

from toolkit.cli.cmd_batch import _format_duration, _format_years, _read_config_list


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

    def test_relative_path_resolved_from_configs_parent(self, tmp_path: pytest.TempPathFactory) -> None:
        configs_file = tmp_path / "configs.txt"
        dataset_dir = tmp_path / "datasets"
        dataset_dir.mkdir()
        dataset_file = dataset_dir / "dataset.yml"
        dataset_file.write_text("", encoding="utf-8")

        configs_file.write_text("datasets/dataset.yml\n", encoding="utf-8")
        result = _read_config_list(configs_file)
        assert result == [dataset_file.resolve()]

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


class TestFormatYears:
    def test_none(self) -> None:
        assert _format_years(None) == "-"

    def test_empty_list(self) -> None:
        assert _format_years([]) == "-"

    def test_single_year(self) -> None:
        assert _format_years([2023]) == "2023"

    def test_multiple_years(self) -> None:
        assert _format_years([2021, 2022, 2023]) == "2021,2022,2023"

    def test_returns_string(self) -> None:
        assert isinstance(_format_years([2020]), str)


class TestFormatDuration:
    def test_none_returns_dash(self) -> None:
        assert _format_duration(None) == "-"

    def test_seconds_formatted(self) -> None:
        assert _format_duration(1.234) == "1.234s"

    def test_zero(self) -> None:
        assert _format_duration(0.0) == "0.000s"

    def test_rounds_to_3_decimals(self) -> None:
        assert _format_duration(1.23456789) == "1.235s"

    def test_returns_string(self) -> None:
        assert isinstance(_format_duration(1.0), str)
