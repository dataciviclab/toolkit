import json
from tests.helpers import write_parquet
from pathlib import Path
import re
from types import SimpleNamespace

import pytest

from toolkit.raw.validate import validate_raw_output
from toolkit.clean.validate import validate_clean, run_clean_validation
from toolkit.core.config_models import TransitionConfig
from toolkit.core.validation import check_transitions
from toolkit.mart.validate import run_mart_validation, validate_mart
from toolkit.core.validation import write_validation_json


def _assert_portable_json_report(path: Path, *, root: Path, field: str, expected: str):
    payload = json.loads(path.read_text(encoding="utf-8"))
    serialized = json.dumps(payload, ensure_ascii=False)

    assert not re.search(r"[A-Za-z]:\\\\", serialized)
    assert '": "/' not in serialized
    assert '": "\\\\' not in serialized
    assert str(root.resolve()) not in serialized
    assert payload["summary"][field] == expected


@pytest.mark.policy
def test_validate_raw_detects_html_in_csv(tmp_path: Path):
    out_dir = tmp_path / "raw"
    out_dir.mkdir(parents=True, exist_ok=True)

    # finto CSV che in realtà è HTML (pagina di errore)
    bad = out_dir / "data.csv"
    bad.write_text("<html><body>error</body></html>", encoding="utf-8")

    files_written = [{"file": "data.csv", "bytes": bad.stat().st_size, "sha256": "fake"}]
    res = validate_raw_output(out_dir, files_written)

    assert res.ok is False
    assert any("contain HTML" in e for e in res.errors)


@pytest.mark.policy
def test_validate_clean_ok_and_missing_required(tmp_path: Path):
    p = tmp_path / "clean.parquet"
    write_parquet(p, "CREATE TABLE t AS SELECT 1 AS a, 'x' AS b")

    ok = validate_clean(p, required=["a", "b"])
    assert ok.ok is True
    assert ok.summary["row_count"] == 1

    bad = validate_clean(p, required=["missing_col"])
    assert bad.ok is False
    assert any("Missing required columns" in e for e in bad.errors)


@pytest.mark.policy
def test_validate_clean_report_uses_root_relative_path(tmp_path: Path):
    root = tmp_path / "root"
    parquet = root / "data" / "clean" / "demo" / "2024" / "demo_2024_clean.parquet"
    parquet.parent.mkdir(parents=True, exist_ok=True)
    write_parquet(parquet, "CREATE TABLE t AS SELECT 1 AS a, 'x' AS b")

    result = validate_clean(parquet, required=["a", "b"], root=root)
    report = write_validation_json(parquet.parent / "_validate" / "clean_validation.json", result)

    _assert_portable_json_report(
        report,
        root=root,
        field="path",
        expected="data/clean/demo/2024/demo_2024_clean.parquet",
    )


@pytest.mark.policy
def test_validate_mart_required_tables(tmp_path: Path):
    d = tmp_path / "mart"
    d.mkdir(parents=True, exist_ok=True)

    write_parquet(d / "foo.parquet", "CREATE TABLE t AS SELECT 1 AS k")

    res = validate_mart(d, required_tables=["foo", "bar"])
    assert res.ok is False
    assert any("Missing required MART tables" in e for e in res.errors)


@pytest.mark.policy
def test_validate_mart_min_rows_rule(tmp_path: Path):
    d = tmp_path / "mart"
    d.mkdir(parents=True, exist_ok=True)

    write_parquet(d / "foo.parquet", "CREATE TABLE t AS SELECT 1 AS k")

    bad = validate_mart(d, table_rules={"foo": {"min_rows": 2}})
    assert bad.ok is False
    assert any("[foo] row_count too small: 1 < 2" in e for e in bad.errors)

    ok = validate_mart(d, table_rules={"foo": {"min_rows": 1}})
    assert ok.ok is True


@pytest.mark.policy
def test_validate_mart_warns_on_orphan_table_rules_against_declared_tables(tmp_path: Path):
    d = tmp_path / "mart"
    d.mkdir(parents=True, exist_ok=True)

    write_parquet(d / "foo.parquet", "CREATE TABLE t AS SELECT 1 AS k")

    result = validate_mart(
        d,
        declared_tables=["foo"],
        table_rules={"bar": {"min_rows": 1}},
    )

    assert result.ok is True
    assert any("not declared in mart.tables" in warning for warning in result.warnings)
    assert result.summary["declared_tables"] == ["foo"]
    assert result.summary["orphan_table_rules"] == ["bar"]


@pytest.mark.policy
def test_validate_mart_report_uses_root_relative_dir(tmp_path: Path):
    root = tmp_path / "root"
    mart_dir = root / "data" / "mart" / "demo" / "2024"
    mart_dir.mkdir(parents=True, exist_ok=True)
    write_parquet(mart_dir / "foo.parquet", "CREATE TABLE t AS SELECT 1 AS k")

    result = validate_mart(mart_dir, required_tables=["foo"], root=root)
    report = write_validation_json(mart_dir / "_validate" / "mart_validation.json", result)

    _assert_portable_json_report(
        report,
        root=root,
        field="dir",
        expected="data/mart/demo/2024",
    )


@pytest.mark.policy
def test_validate_mart_max_null_pct_rule(tmp_path: Path):
    d = tmp_path / "mart"
    d.mkdir(parents=True, exist_ok=True)

    # Two rows: one has NULL (50% nulls > 10% threshold)
    write_parquet(
        d / "foo.parquet",
        "CREATE TABLE t AS SELECT * FROM (VALUES (1), (NULL)) v(valore)",
    )

    bad = validate_mart(d, table_rules={"foo": {"max_null_pct": {"valore": 0.1}}})
    assert bad.ok is False
    assert any("null_pct too high" in e for e in bad.errors)

    ok = validate_mart(d, table_rules={"foo": {"max_null_pct": {"valore": 0.6}}})
    assert ok.ok is True


@pytest.mark.policy
def test_check_transitions_warns_on_row_drop_over_threshold_and_removed_columns() -> None:
    transition_profiles = [
        {
            "target_name": "mart_demo",
            "source_row_count": 100,
            "target_row_count": 70,
            "removed_columns": ["col_a", "col_b"],
        }
    ]

    report = check_transitions(
        transition_profiles,
        TransitionConfig(max_row_drop_pct=20, warn_removed_columns=True),
    )

    assert report["warnings_count"] == 2
    assert len(report["warnings"]) == 2
    assert report["profiles_count"] == 1
    assert any("row drop 30.0%" in warning for warning in report["warning_messages"])
    assert any("columns removed from clean" in warning for warning in report["warning_messages"])
    assert any(item["kind"] == "row_drop_pct" for item in report["warnings"])
    assert any(item["kind"] == "removed_columns" for item in report["warnings"])


@pytest.mark.policy
def test_check_transitions_respects_optional_threshold_and_removed_columns_toggle() -> None:
    transition_profiles = [
        {
            "target_name": "mart_demo",
            "source_row_count": 100,
            "target_row_count": 70,
            "removed_columns": ["col_a"],
        }
    ]

    no_threshold = check_transitions(
        transition_profiles,
        TransitionConfig(max_row_drop_pct=None, warn_removed_columns=False),
    )
    assert no_threshold["warning_messages"] == []
    assert no_threshold["warnings"] == []

    removed_only = check_transitions(
        transition_profiles,
        TransitionConfig(max_row_drop_pct=None, warn_removed_columns=True),
    )
    assert len(removed_only["warnings"]) == 1
    assert removed_only["warnings"][0]["kind"] == "removed_columns"
    assert "columns removed from clean" in removed_only["warning_messages"][0]


@pytest.mark.policy
def test_run_mart_validation_merges_transition_warnings_into_report(tmp_path: Path):
    root = tmp_path / "root"
    mart_dir = root / "data" / "mart" / "demo" / "2024"
    mart_dir.mkdir(parents=True, exist_ok=True)
    write_parquet(mart_dir / "mart_demo.parquet", "CREATE TABLE t AS SELECT 1 AS k")

    (mart_dir / "metadata.json").write_text(
        json.dumps(
            {
                "outputs": [{"name": "mart_demo", "path": "mart_demo.parquet"}],
                "transition_profiles": [
                    {
                        "target_name": "mart_demo",
                        "source_row_count": 100,
                        "target_row_count": 70,
                        "removed_columns": ["legacy_col"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    from tests.helpers import make_config

    cfg = make_config(
        root=root,
        base_dir=root,
        dataset="demo",
        mart={
            "tables": [{"name": "mart_demo", "sql": "sql/mart_demo.sql"}],
            "required_tables": ["mart_demo"],
            "validate": {
                "transition": {
                    "max_row_drop_pct": 20,
                    "warn_removed_columns": True,
                }
            },
        },
    )

    summary = run_mart_validation(cfg, 2024, logger=SimpleNamespace(info=lambda *args, **kwargs: None))

    assert summary["passed"] is True
    assert summary["warnings_count"] == 2

    report = json.loads((mart_dir / "_validate" / "mart_validation.json").read_text(encoding="utf-8"))
    assert len(report["warnings"]) == 2
    assert any("row drop 30.0%" in warning for warning in report["warnings"])
    assert any("columns removed from clean" in warning for warning in report["warnings"])
    assert report["transition"]["profiles_count"] == 1
    assert report["transition"]["warnings_count"] == 2
    assert report["transition"]["config"] == {
        "max_row_drop_pct": 20.0,
        "warn_removed_columns": True,
    }
    assert any(item["kind"] == "row_drop_pct" for item in report["transition"]["warnings"])
    assert any(item["kind"] == "removed_columns" for item in report["transition"]["warnings"])



@pytest.mark.policy



@pytest.mark.policy
def test_run_clean_validation_uses_columns_raw_from_raw_profile(tmp_path: Path):
    """Regression test for issue #145: raw_col_count must come from columns_raw in
    raw_profile.json, not from _profile_raw_input which may read the CSV with
    broken read_params_used and get placeholder names (column00, column01, ...)."""
    root = tmp_path / "root"
    dataset = "demo"
    year = 2024

    raw_dir = root / "data" / "raw" / dataset / str(year)
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / "data.csv").write_text("a,b,c\n1,2,3\n", encoding="utf-8")

    profile_dir = raw_dir / "_profile"
    profile_dir.mkdir(parents=True, exist_ok=True)
    real_columns = ["col_alpha", "col_beta", "col_gamma"]
    (profile_dir / "raw_profile.json").write_text(
        json.dumps(
            {
                "columns_raw": real_columns,
                "columns_norm": [c.lower() for c in real_columns],
                "row_count": 1,
            }
        ),
        encoding="utf-8",
    )

    clean_dir = root / "data" / "clean" / dataset / str(year)
    clean_dir.mkdir(parents=True, exist_ok=True)
    write_parquet(clean_dir / f"{dataset}_{year}_clean.parquet", "CREATE TABLE t AS SELECT 1 AS col_alpha, 2 AS col_beta")

    (clean_dir / "metadata.json").write_text(
        json.dumps(
            {
                "input_files": ["data.csv"],
                "read_params_used": {"header": False},
                "output_profile": {
                    "columns": [
                        {"name": "col_alpha", "type": "INTEGER"},
                        {"name": "col_beta", "type": "INTEGER"},
                    ],
                    "row_count": 1,
                },
                "outputs": [],
            }
        ),
        encoding="utf-8",
    )

    from tests.helpers import make_config

    cfg = make_config(root=root, base_dir=root, dataset=dataset)

    summary = run_clean_validation(cfg, year, logger=SimpleNamespace(info=lambda *args, **kwargs: None))

    assert summary["stats"]["raw_cols"] == len(real_columns)
    assert summary["stats"]["col_drop_count"] == len(real_columns) - 2

    # raw_probe_source must be "raw_profile" when profile exists
    assert summary["stats"].get("raw_probe_source") == "raw_profile"


@pytest.mark.policy
def test_run_clean_validation_raw_probe_source_legacy_autodetect(tmp_path: Path):
    """When no profile exists and a CSV raw file is present, validation falls back
    to read_csv(auto_detect=true) and sets raw_probe_source = 'legacy_autodetect'."""
    root = tmp_path / "root"
    dataset = "demo"
    year = 2024

    raw_dir = root / "data" / "raw" / dataset / str(year)
    raw_dir.mkdir(parents=True, exist_ok=True)
    # No profile created — simulating a candidate that hasn't run init
    (raw_dir / "data.csv").write_text("col1,col2,col3\nval1,val2,val3\n", encoding="utf-8")

    clean_dir = root / "data" / "clean" / dataset / str(year)
    clean_dir.mkdir(parents=True, exist_ok=True)
    write_parquet(clean_dir / f"{dataset}_{year}_clean.parquet", "CREATE TABLE t AS SELECT 1 AS col1, 2 AS col2")

    (clean_dir / "metadata.json").write_text(
        json.dumps(
            {
                "input_files": ["data.csv"],
                "read_params_used": {},
                "output_profile": {
                    "columns": [
                        {"name": "col1", "type": "INTEGER"},
                        {"name": "col2", "type": "INTEGER"},
                    ],
                    "row_count": 1,
                },
                "outputs": [],
            }
        ),
        encoding="utf-8",
    )

    from tests.helpers import make_config

    cfg = make_config(root=root, base_dir=root, dataset=dataset)
    logger = SimpleNamespace(info=lambda *args, **kwargs: None)

    result = run_clean_validation(cfg, year, logger=logger)

    # With no profile, the probe must fall back to legacy autodetect
    assert result["stats"].get("raw_probe_source") == "legacy_autodetect"
    # Warning must mention the fallback reason
    # build_validation_summary only includes the raw stats, not the full result.warnings
    # So we check the warning was emitted by inspecting via the ValidationResult if accessible,
    # or by verifying that the fallback path was taken (raw_probe_source = legacy_autodetect)
    warning_texts = " ".join(_read_warnings_from_validation_report(clean_dir))
    assert "falling back to read_csv(auto_detect=true)" in warning_texts


@pytest.mark.policy
def test_run_clean_validation_raw_probe_source_unavailable_when_no_raw_file(
    tmp_path: Path,
):
    """When neither profile nor raw file exist, raw_probe_source = 'unavailable'."""
    root = tmp_path / "root"
    dataset = "demo"
    year = 2024

    raw_dir = root / "data" / "raw" / dataset / str(year)
    raw_dir.mkdir(parents=True, exist_ok=True)
    # No profile, no raw file

    clean_dir = root / "data" / "clean" / dataset / str(year)
    clean_dir.mkdir(parents=True, exist_ok=True)
    write_parquet(clean_dir / f"{dataset}_{year}_clean.parquet", "CREATE TABLE t AS SELECT 1 AS col1")

    (clean_dir / "metadata.json").write_text(
        json.dumps(
            {
                "input_files": ["data.csv"],
                "read_params_used": {},
                "output_profile": {
                    "columns": [{"name": "col1", "type": "INTEGER"}],
                    "row_count": 1,
                },
                "outputs": [],
            }
        ),
        encoding="utf-8",
    )

    from tests.helpers import make_config

    cfg = make_config(root=root, base_dir=root, dataset=dataset)
    logger = SimpleNamespace(info=lambda *args, **kwargs: None)

    result = run_clean_validation(cfg, year, logger=logger)

    assert result["stats"].get("raw_probe_source") == "unavailable"


def _read_warnings_from_validation_report(clean_dir: Path) -> list[str]:
    """Read warnings from the written validation JSON."""
    report_path = clean_dir / "_validate" / "clean_validation.json"
    if report_path.exists():
        data = json.loads(report_path.read_text(encoding="utf-8"))
        return data.get("warnings", [])
    return []


@pytest.mark.policy
def test_ensure_dict_preserves_validate_alias() -> None:
    """Verify ensure_dict converts validate_config -> validate (by_alias=True)."""
    from toolkit.core.config import ensure_dict
    from toolkit.core.config_models import ToolkitConfigModel

    model = ToolkitConfigModel(
        base_dir=Path("/tmp"),
        root=Path("/tmp/out"),
        root_source="test",
        dataset={"name": "test", "years": [2024]},
        clean={
            "sql": "sql/clean.sql",
            "validate": {
                "primary_key": "id",
                "not_null": "val",
                "ranges": {"a": {"min": 0, "max": 100}},
            },
        },
        mart={
            "tables": [{"name": "m1", "sql": "sql/mart/m1.sql"}],
            "validate": {
                "transition": {"max_row_drop_pct": 10},
            },
        },
    )

    clean_dict = ensure_dict(model.clean)
    assert "validate" in clean_dict, (
        f"expected 'validate' key in clean_dict, got keys: {list(clean_dict.keys())}"
    )
    v = clean_dict["validate"]
    assert v["primary_key"] == ["id"], f"validate.primary_key={v.get('primary_key')}"
    assert v["not_null"] == ["val"]
    assert v["ranges"]["a"]["min"] == 0

    mart_dict = ensure_dict(model.mart)
    assert "validate" in mart_dict, (
        f"expected 'validate' key in mart_dict, got keys: {list(mart_dict.keys())}"
    )
    mv = mart_dict["validate"]
    assert mv["transition"]["max_row_drop_pct"] == 10
