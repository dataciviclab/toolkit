from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from toolkit.core.support import (
    flatten_support_template_ctx,
    resolve_support_payloads,
    _support_expected_mart_outputs,
)


# --- Helpers ---


@dataclass
class FakeConfig:
    """Minimal config-like object for unit-testing _support_expected_mart_outputs."""

    root: Path
    dataset: str
    years: list[int]
    mart: dict[str, Any]


def _make_support_dataset(
    tmp_path: Path,
    name: str = "support_ds",
    years: list[int] | None = None,
    mart_tables: list[str] | None = None,
    create_mart_outputs: bool = False,
) -> Path:
    """Create a minimal support dataset project with config, mart dir, and optional parquet outputs."""
    years = years or [2024]
    mart_tables = mart_tables or ["table_a"]

    ds_dir = tmp_path / name
    ds_dir.mkdir(parents=True, exist_ok=True)

    years_str = ", ".join(str(y) for y in years)
    tables_yml = "\n".join(f"    - name: {t}\n      sql: sql/mart/{t}.sql" for t in mart_tables)

    yml_content = (
        f"root: {ds_dir}\n"
        f"dataset:\n"
        f"  name: {name}\n"
        f"  years: [{years_str}]\n"
        f"mart:\n"
        f"  tables:\n"
        f"{tables_yml}\n"
    )
    config_path = ds_dir / "dataset.yml"
    config_path.write_text(yml_content, encoding="utf-8")

    if create_mart_outputs:
        for year in years:
            mart_dir = ds_dir / "data" / "mart" / name / str(year)
            mart_dir.mkdir(parents=True, exist_ok=True)
            for table in mart_tables:
                (mart_dir / f"{table}.parquet").write_bytes(b"")

    return config_path


def _make_support_entry(
    config_path: Path, name: str = "my_support", years: list[int] | None = None
) -> dict:
    return {"name": name, "config": str(config_path), "years": years or [2024]}


# --- _support_expected_mart_outputs ---


class TestSupportExpectedMartOutputs:
    def test_single_table(self, tmp_path: Path):
        ds_dir = tmp_path / "ds"
        ds_dir.mkdir()
        cfg = FakeConfig(
            root=ds_dir,
            dataset="ds",
            years=[2024],
            mart={"tables": [{"name": "table_a", "sql": "sql/a.sql"}]},
        )
        outputs = _support_expected_mart_outputs(cfg, 2024)
        assert len(outputs) == 1
        assert outputs[0].name == "table_a.parquet"
        assert str(2024) in str(outputs[0])

    def test_multiple_tables(self, tmp_path: Path):
        ds_dir = tmp_path / "ds"
        ds_dir.mkdir()
        cfg = FakeConfig(
            root=ds_dir,
            dataset="ds",
            years=[2024],
            mart={
                "tables": [
                    {"name": "alpha", "sql": "sql/a.sql"},
                    {"name": "beta", "sql": "sql/b.sql"},
                ]
            },
        )
        outputs = _support_expected_mart_outputs(cfg, 2024)
        assert len(outputs) == 2
        assert outputs[0].name == "alpha.parquet"
        assert outputs[1].name == "beta.parquet"

    def test_no_tables_returns_empty(self, tmp_path: Path):
        ds = tmp_path / "fake"
        cfg = FakeConfig(root=ds, dataset="ds", years=[2024], mart={"tables": []})
        outputs = _support_expected_mart_outputs(cfg, 2024)
        assert outputs == []

    def test_malformed_table_entry_skipped(self, tmp_path: Path):
        ds = tmp_path / "fake"
        cfg = FakeConfig(root=ds, dataset="ds", years=[2024], mart={"tables": ["not_a_dict", {}]})
        outputs = _support_expected_mart_outputs(cfg, 2024)
        assert outputs == []


# --- resolve_support_payloads: happy paths ---


class TestResolveSupportPayloadsHappy:
    def test_single_support_single_year(self, tmp_path: Path):
        config_path = _make_support_dataset(tmp_path, create_mart_outputs=True)
        entries = [_make_support_entry(config_path)]
        result = resolve_support_payloads(entries, require_exists=True)

        assert len(result) == 1
        payload = result[0]
        assert payload["name"] == "my_support"
        assert payload["dataset"] == "support_ds"
        assert payload["years"] == [2024]
        assert len(payload["years_resolved"]) == 1
        yr = payload["years_resolved"][0]
        assert yr["all_outputs_exist"] is True
        assert payload["mart"] is not None
        assert payload["mart"].endswith("table_a.parquet")

    def test_single_support_multiple_years(self, tmp_path: Path):
        config_path = _make_support_dataset(tmp_path, years=[2023, 2024], create_mart_outputs=True)
        entries = [_make_support_entry(config_path, years=[2023, 2024])]
        result = resolve_support_payloads(entries, require_exists=True)

        assert len(result) == 1
        payload = result[0]
        assert payload["years"] == [2023, 2024]
        assert len(payload["years_resolved"]) == 2
        yr0 = payload["years_resolved"][0]
        assert yr0["year"] == 2023
        yr1 = payload["years_resolved"][1]
        assert yr1["year"] == 2024

    def test_multiple_support_datasets(self, tmp_path: Path):
        config_a = _make_support_dataset(tmp_path, name="support_a", create_mart_outputs=True)
        config_b = _make_support_dataset(tmp_path, name="support_b", create_mart_outputs=True)
        entries = [
            _make_support_entry(config_a, name="alpha"),
            _make_support_entry(config_b, name="beta"),
        ]
        result = resolve_support_payloads(entries, require_exists=True)

        assert len(result) == 2
        names = [r["name"] for r in result]
        assert names == ["alpha", "beta"]

    def test_require_exists_false_allows_missing_outputs(self, tmp_path: Path):
        """With require_exists=False, missing parquet files should not raise."""
        config_path = _make_support_dataset(tmp_path, create_mart_outputs=False)
        entries = [_make_support_entry(config_path)]
        result = resolve_support_payloads(entries, require_exists=False)

        assert len(result) == 1
        payload = result[0]
        assert len(payload["years_resolved"]) == 1
        yr = payload["years_resolved"][0]
        assert len(yr["outputs"]) == 1
        assert yr["existing_outputs"] == []
        assert yr["all_outputs_exist"] is False

    def test_none_entries_returns_empty(self):
        result = resolve_support_payloads(None, require_exists=True)
        assert result == []

    def test_empty_entries_returns_empty(self):
        result = resolve_support_payloads([], require_exists=True)
        assert result == []


# --- resolve_support_payloads: error paths ---


class TestResolveSupportPayloadsErrors:
    def test_missing_mart_outputs_raises_on_require_exists(self, tmp_path: Path):
        config_path = _make_support_dataset(tmp_path, create_mart_outputs=False)
        entries = [_make_support_entry(config_path)]
        with pytest.raises(FileNotFoundError, match="output mancante"):
            resolve_support_payloads(entries, require_exists=True)

    def test_partial_mart_outputs_raises_on_require_exists(self, tmp_path: Path):
        """Only some of the expected tables exist."""
        config_path = _make_support_dataset(
            tmp_path, mart_tables=["table_a", "table_b"], create_mart_outputs=False
        )
        ds_dir = config_path.parent
        mart_dir = ds_dir / "data" / "mart" / "support_ds" / "2024"
        mart_dir.mkdir(parents=True)
        (mart_dir / "table_a.parquet").write_bytes(b"")

        entries = [_make_support_entry(config_path)]
        with pytest.raises(FileNotFoundError, match="output mancante"):
            resolve_support_payloads(entries, require_exists=True)

    def test_error_message_includes_name_and_config_path(self, tmp_path: Path):
        config_path = _make_support_dataset(tmp_path, create_mart_outputs=False)
        entries = [_make_support_entry(config_path, name="ref_lookup")]
        with pytest.raises(FileNotFoundError) as exc_info:
            resolve_support_payloads(entries, require_exists=True)
        msg = str(exc_info.value)
        assert "ref_lookup" in msg
        assert str(config_path) in msg

    def test_error_message_includes_year(self, tmp_path: Path):
        config_path = _make_support_dataset(tmp_path, years=[2023, 2024], create_mart_outputs=False)
        entries = [_make_support_entry(config_path, years=[2023, 2024])]
        with pytest.raises(FileNotFoundError) as exc_info:
            resolve_support_payloads(entries, require_exists=True)
        msg = str(exc_info.value)
        assert "2023" in msg

    def test_multiple_years_first_missing_raises(self, tmp_path: Path):
        """If second year has outputs but first doesn't, should raise on first."""
        config_path = _make_support_dataset(tmp_path, years=[2023, 2024], create_mart_outputs=False)
        ds_dir = config_path.parent
        mart_dir_2024 = ds_dir / "data" / "mart" / "support_ds" / "2024"
        mart_dir_2024.mkdir(parents=True)
        (mart_dir_2024 / "table_a.parquet").write_bytes(b"")

        entries = [_make_support_entry(config_path, years=[2023, 2024])]
        with pytest.raises(FileNotFoundError, match="2023"):
            resolve_support_payloads(entries, require_exists=True)


# --- flatten_support_template_ctx ---


class TestFlattenSupportTemplateCtx:
    def test_single_payload(self):
        payloads = [
            {
                "name": "my_support",
                "outputs": ["/path/table_a.parquet"],
                "mart": "/path/table_a.parquet",
            }
        ]
        ctx = flatten_support_template_ctx(payloads)
        assert ctx["support.my_support.outputs"] == ["/path/table_a.parquet"]
        assert ctx["support.my_support.mart"] == "/path/table_a.parquet"

    def test_multiple_payloads(self):
        payloads = [
            {
                "name": "alpha",
                "outputs": ["/a/table.parquet"],
                "mart": "/a/table.parquet",
            },
            {
                "name": "beta",
                "outputs": ["/b/t1.parquet", "/b/t2.parquet"],
                "mart": "/b/t1.parquet",
            },
        ]
        ctx = flatten_support_template_ctx(payloads)
        assert ctx["support.alpha.outputs"] == ["/a/table.parquet"]
        assert ctx["support.alpha.mart"] == "/a/table.parquet"
        assert ctx["support.beta.outputs"] == ["/b/t1.parquet", "/b/t2.parquet"]
        assert ctx["support.beta.mart"] == "/b/t1.parquet"

    def test_empty_payloads(self):
        ctx = flatten_support_template_ctx([])
        assert ctx == {}

    def test_mart_is_none_when_no_outputs(self):
        payloads = [
            {
                "name": "empty_support",
                "outputs": [],
                "mart": None,
            }
        ]
        ctx = flatten_support_template_ctx(payloads)
        assert ctx["support.empty_support.outputs"] == []
        assert ctx["support.empty_support.mart"] is None


# --- Integration: resolve + flatten ---


class TestResolveAndFlatten:
    def test_end_to_end(self, tmp_path: Path):
        config_path = _make_support_dataset(tmp_path, create_mart_outputs=True)
        entries = [_make_support_entry(config_path)]
        resolved = resolve_support_payloads(entries, require_exists=True)
        ctx = flatten_support_template_ctx(resolved)

        assert "support.my_support.outputs" in ctx
        assert "support.my_support.mart" in ctx
        assert len(ctx["support.my_support.outputs"]) == 1
        assert ctx["support.my_support.mart"].endswith("table_a.parquet")

    def test_no_flatten_on_empty_resolve(self):
        resolved = resolve_support_payloads(None, require_exists=True)
        ctx = flatten_support_template_ctx(resolved)
        assert ctx == {}
