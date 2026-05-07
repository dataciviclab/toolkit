from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from toolkit.cli.app import app


def _write_dataset_config(base_dir: Path) -> Path:
    config_path = base_dir / "dataset.yml"
    config_path.write_text(
        "\n".join(
            [
                'root: "./_out"',
                "",
                "dataset:",
                '  name: "schema_diff_example"',
                "  years: [2022, 2023]",
                "",
                "raw:",
                "  sources:",
                '    - name: "dummy"',
                '      type: "local_file"',
                "      args:",
                '        path: "input.csv"',
                '        filename: "input_{year}.csv"',
                "",
                "clean:",
                '  sql: "sql/clean.sql"',
                "  validate: {}",
                "",
                "mart:",
                "  tables: []",
                "  validate: {}",
                "",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return config_path


def _write_raw_year(
    root: Path,
    year: int,
    file_name: str,
    header_line: str,
    *,
    delim: str = ",",
) -> None:
    raw_dir = root / "data" / "raw" / "schema_diff_example" / str(year)
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / file_name).write_text(header_line + "\n1,2,3\n", encoding="utf-8")
    (raw_dir / "manifest.json").write_text(
        json.dumps({"primary_output_file": file_name}),
        encoding="utf-8",
    )
    (raw_dir / "metadata.json").write_text(
        json.dumps(
            {
                "profile_hints": {
                    "file_used": file_name,
                    "encoding_suggested": "utf-8",
                    "delim_suggested": delim,
                    "decimal_suggested": ".",
                    "skip_suggested": 0,
                    "header_line": header_line,
                    "columns_preview": header_line.split(delim) if header_line else [],
                    "warnings": [],
                }
            }
        ),
        encoding="utf-8",
    )


def test_inspect_schema_diff_reports_multi_year_changes(tmp_path: Path, monkeypatch) -> None:
    config_path = _write_dataset_config(tmp_path)
    root = tmp_path / "_out"
    _write_raw_year(root, 2022, "input_2022.csv", "anno,comune,imponibile")
    _write_raw_year(root, 2023, "input_2023.csv", "anno,comune,imponibile,contribuenti")

    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["inspect", "schema-diff", "--config", str(config_path), "--strict-config"])

    assert result.exit_code == 0, result.output
    assert "dataset: schema_diff_example" in result.output
    assert "year: 2022" in result.output
    assert "year: 2023" in result.output
    assert "2022 -> 2023:" in result.output
    assert "counts: 3 -> 4" in result.output
    assert "added_columns:" in result.output
    assert "contribuenti" in result.output


@pytest.mark.policy
def test_inspect_schema_diff_skips_binary_file(tmp_path: Path, monkeypatch) -> None:
    """XLSX files produce garbage header_line — schema diff must skip comparison."""
    config_path = _write_dataset_config(tmp_path)
    root = tmp_path / "_out"
    # 2022: CSV (normal)
    _write_raw_year(root, 2022, "input_2022.csv", "anno,comune,imponibile")
    # 2023: fake XLSX binary in metadata
    raw_dir_2023 = root / "data" / "raw" / "schema_diff_example" / "2023"
    raw_dir_2023.mkdir(parents=True, exist_ok=True)
    (raw_dir_2023 / "input_2023.xlsx").write_bytes(b"PK\x03\x04\x14\x00\x00")
    (raw_dir_2023 / "manifest.json").write_text(
        json.dumps({"primary_output_file": "input_2023.xlsx"}),
        encoding="utf-8",
    )
    (raw_dir_2023 / "metadata.json").write_text(
        json.dumps(
            {
                "profile_hints": {
                    "file_used": "input_2023.xlsx",
                    "is_binary_file": "xlsx",
                    "warnings": ["binary_file_detected: xlsx — use sheet_name in dataset.yml"],
                }
            }
        ),
        encoding="utf-8",
    )

    runner = CliRunner()
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(
        app, ["inspect", "schema-diff", "--config", str(config_path), "--json", "--strict-config"]
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["entries"][0]["is_binary_file"] is None
    assert payload["entries"][1]["is_binary_file"] == "xlsx"
    assert payload["comparisons"][0]["skipped"] is True
    assert payload["comparisons"][0]["reason"] == "binary_file_not_comparable"


def test_inspect_schema_diff_json_degrades_when_raw_is_missing(tmp_path: Path, monkeypatch) -> None:
    config_path = _write_dataset_config(tmp_path)

    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(
        app,
        ["inspect", "schema-diff", "--config", str(config_path), "--json", "--strict-config"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["dataset"] == "schema_diff_example"
    assert payload["years"] == [2022, 2023]
    assert len(payload["entries"]) == 2
    assert payload["entries"][0]["raw_exists"] is False
    assert payload["entries"][0]["primary_output_file"] is None
    assert payload["entries"][0]["columns_count"] == 0
    assert payload["comparisons"][0]["changed"] is False
