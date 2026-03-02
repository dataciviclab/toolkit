from __future__ import annotations

from pathlib import Path

from toolkit.profile.raw import build_profile_hints


def test_build_profile_hints_for_standard_csv(tmp_path: Path) -> None:
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("id,name,value\n1,Alice,10\n2,Bob,20\n", encoding="utf-8")

    hints = build_profile_hints(csv_path)

    assert hints["file_used"] == "sample.csv"
    assert hints["encoding_suggested"] == "utf-8"
    assert hints["delim_suggested"] == ","
    assert hints["decimal_suggested"] is None
    assert hints["skip_suggested"] == 0
    assert hints["header_line"] == "id,name,value"
    assert hints["columns_preview"] == ["id", "name", "value"]
    assert hints["warnings"] == []


def test_build_profile_hints_detects_preamble_line(tmp_path: Path) -> None:
    csv_path = tmp_path / "preamble.csv"
    csv_path.write_text(
        "Applied filters: year is 2024\nid;name;value\n1;Alice;10\n",
        encoding="utf-8",
    )

    hints = build_profile_hints(csv_path)

    assert hints["delim_suggested"] == ";"
    assert hints["skip_suggested"] == 1
    assert hints["header_line"] == "id;name;value"
    assert hints["columns_preview"] == ["id", "name", "value"]
    assert any("header_preamble_detected" in warning for warning in hints["warnings"])
