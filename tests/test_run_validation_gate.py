from __future__ import annotations

import json
from pathlib import Path

import pytest

from toolkit.cli import cmd_run


def _write_config(path: Path, *, fail_on_error: bool) -> None:
    sql_dir = path.parent / "sql" / "mart"
    sql_dir.mkdir(parents=True, exist_ok=True)
    (path.parent / "sql" / "clean.sql").write_text("select 1 as value", encoding="utf-8")
    (sql_dir / "mart_example.sql").write_text("select * from clean_input", encoding="utf-8")
    path.write_text(
        "\n".join(
            [
                f'root: "{(path.parent / "out").as_posix()}"',
                "dataset:",
                '  name: "test_dataset"',
                "  years: [2022]",
                "raw: {}",
                "clean:",
                '  sql: "sql/clean.sql"',
                "mart:",
                "  tables:",
                '    - name: "mart_example"',
                '      sql: "sql/mart/mart_example.sql"',
                "validation:",
                f'  fail_on_error: {"true" if fail_on_error else "false"}',
            ]
        ),
        encoding="utf-8",
    )


def _read_run_record(root: Path) -> dict[str, object]:
    runs_dir = root / "data" / "_runs" / "test_dataset" / "2022"
    records = list(runs_dir.glob("*.json"))
    assert len(records) == 1
    return json.loads(records[0].read_text(encoding="utf-8"))


def _ok_summary() -> dict[str, object]:
    return {
        "passed": True,
        "errors_count": 0,
        "warnings_count": 0,
        "checks": [],
    }


def _failed_summary() -> dict[str, object]:
    return {
        "passed": False,
        "errors_count": 1,
        "warnings_count": 0,
        "checks": [{"name": "errors", "status": "failed", "details": "errors=1"}],
    }


def test_run_stops_after_failed_validation_when_fail_on_error_true(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "dataset.yml"
    _write_config(config_path, fail_on_error=True)

    calls = {"mart": 0}

    monkeypatch.setattr(cmd_run, "run_raw", lambda *args, **kwargs: None)
    monkeypatch.setattr(cmd_run, "run_clean", lambda *args, **kwargs: None)
    monkeypatch.setattr(cmd_run, "run_mart", lambda *args, **kwargs: calls.__setitem__("mart", calls["mart"] + 1))
    monkeypatch.setattr(cmd_run, "run_raw_validation", lambda *args, **kwargs: _ok_summary())
    monkeypatch.setattr(cmd_run, "run_clean_validation", lambda *args, **kwargs: _failed_summary())
    monkeypatch.setattr(cmd_run, "run_mart_validation", lambda *args, **kwargs: _ok_summary())

    with pytest.raises(cmd_run.ValidationGateError):
        cmd_run.run(step="all", config=str(config_path))

    assert calls["mart"] == 0

    record = _read_run_record(tmp_path / "out")
    assert record["status"] == "FAILED"
    assert record["validations"]["clean"]["passed"] is False


def test_run_continues_after_failed_validation_when_fail_on_error_false(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "dataset.yml"
    _write_config(config_path, fail_on_error=False)

    calls = {"mart": 0}

    monkeypatch.setattr(cmd_run, "run_raw", lambda *args, **kwargs: None)
    monkeypatch.setattr(cmd_run, "run_clean", lambda *args, **kwargs: None)
    monkeypatch.setattr(cmd_run, "run_mart", lambda *args, **kwargs: calls.__setitem__("mart", calls["mart"] + 1))
    monkeypatch.setattr(cmd_run, "run_raw_validation", lambda *args, **kwargs: _ok_summary())
    monkeypatch.setattr(cmd_run, "run_clean_validation", lambda *args, **kwargs: _failed_summary())
    monkeypatch.setattr(cmd_run, "run_mart_validation", lambda *args, **kwargs: _ok_summary())

    cmd_run.run(step="all", config=str(config_path))

    assert calls["mart"] == 1

    record = _read_run_record(tmp_path / "out")
    assert record["status"] == "SUCCESS_WITH_WARNINGS"
    assert record["validations"]["clean"]["passed"] is False


def _write_config_with_min_rows(path: Path, *, min_rows: int) -> None:
    """Config con min_rows per clean e mart, pochi dati raw."""
    sql_dir = path.parent / "sql" / "mart"
    sql_dir.mkdir(parents=True, exist_ok=True)
    (path.parent / "sql" / "clean.sql").write_text(
        "select 42 as value, 'a' as label", encoding="utf-8"
    )
    (sql_dir / "mart_example.sql").write_text(
        "select * from clean_input where label = 'a'", encoding="utf-8"
    )
    path.write_text(
        "\n".join(
            [
                f'root: "{(path.parent / "out").as_posix()}"',
                "dataset:",
                '  name: "test_dataset"',
                "  years: [2022]",
                "raw: {}",
                "clean:",
                '  sql: "sql/clean.sql"',
                "  validate:",
                f"    min_rows: {min_rows}",
                "mart:",
                "  tables:",
                '    - name: "mart_example"',
                '      sql: "sql/mart/mart_example.sql"',
                "  validate:",
                "    table_rules:",
                "      mart_example:",
                f"        min_rows: {min_rows}",
                "validation:",
                '  fail_on_error: false',
            ]
        ),
        encoding="utf-8",
    )


def _render_dummy_clean_dir(clean_dir: Path, rows: int = 5) -> None:
    """Crea parquet + metadata.json + raw profile per test di validazione."""
    # Clean parquet
    clean_dir.mkdir(parents=True, exist_ok=True)
    parquet = clean_dir / "test_dataset_2022_clean.parquet"
    import duckdb
    con = duckdb.connect()
    con.execute(f"COPY (SELECT 42 as value, 'a' as label FROM range({rows})) TO '{parquet}' (FORMAT PARQUET)")
    con.close()
    # metadata.json con output_profile per validate_promotion
    (clean_dir / "metadata.json").write_text(
        json.dumps({
            "dataset": "test_dataset",
            "year": 2022,
            "outputs": ["test_dataset_2022_clean.parquet"],
            "output_profile": {"row_count": rows, "columns": [{"name": "value", "type": "INTEGER"}, {"name": "label", "type": "VARCHAR"}]},
        }),
        encoding="utf-8",
    )
    # Raw dir con CSV minimo per validate_promotion
    raw_dir = clean_dir.parent.parent.parent / "raw" / "test_dataset" / "2022"
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / "test_2022.csv").write_text("value,label\n42,a\n", encoding="utf-8")
    # Profilo raw per validate_promotion
    profile_dir = raw_dir / "_profile"
    profile_dir.mkdir(parents=True, exist_ok=True)
    (profile_dir / "raw_profile.json").write_text(
        json.dumps({"row_count": rows, "columns_raw": ["value", "label"]}),
        encoding="utf-8",
    )


def _make_full_config(tmp_path: Path, root: str | None = None) -> tuple[Path, object]:
    """Crea un config completo per test (dataset + raw + clean + mart).

    Ritorna (config_path, cfg_object). Il cfg_object ha attributi
    .dataset, .root, .base_dir, .clean, .mart per run_clean_validation.
    """
    config_path = tmp_path / "dataset.yml"
    _write_config_with_min_rows(config_path, min_rows=1000)

    from toolkit.cli.cmd_run import load_cfg_and_logger
    cfg, _ = load_cfg_and_logger(str(config_path))
    return config_path, cfg


@pytest.mark.regression
def test_clean_validation_skips_min_rows_in_sample_mode(tmp_path: Path) -> None:
    """run_clean_validation con sample_mode=True ignora min_rows."""
    config_path, cfg = _make_full_config(tmp_path)

    clean_dir = config_path.parent / "out" / "data" / "clean" / "test_dataset" / "2022"
    _render_dummy_clean_dir(clean_dir, rows=5)

    import logging
    logger = logging.getLogger("test")

    # sample_mode=True -> min_rows ignorato, passa
    result = cmd_run.run_clean_validation(cfg, 2022, logger, sample_mode=True)
    assert result.get("passed") is True, f"Expected passed in sample mode, got: {result}"

    # sample_mode=False -> min_rows attivo, fallisce
    result = cmd_run.run_clean_validation(cfg, 2022, logger, sample_mode=False)
    assert result.get("passed") is False, f"Expected failed in normal mode, got: {result}"


@pytest.mark.regression
def test_mart_validation_skips_min_rows_in_sample_mode(tmp_path: Path) -> None:
    """run_mart_validation con sample_mode=True ignora table_rules.*.min_rows."""
    config_path, cfg = _make_full_config(tmp_path)

    # Mart dir con parquet e metadata
    mart_dir = config_path.parent / "out" / "data" / "mart" / "test_dataset" / "2022"
    mart_dir.mkdir(parents=True, exist_ok=True)
    import duckdb
    con = duckdb.connect()
    con.execute("COPY (SELECT 42 as value, 'a' as label FROM range(5)) TO '" + str(mart_dir / "mart_example.parquet") + "' (FORMAT PARQUET)")
    con.close()
    (mart_dir / "metadata.json").write_text(
        '{"dataset": "test_dataset", "year": 2022, "outputs": ["mart_example.parquet"]}',
        encoding="utf-8",
    )

    import logging
    logger = logging.getLogger("test")

    # sample_mode=True -> min_rows ignorato, passa
    result = cmd_run.run_mart_validation(cfg, 2022, logger, sample_mode=True)
    assert result.get("passed") is True, f"Expected passed in sample mode, got: {result}"

    # sample_mode=False -> min_rows attivo, fallisce
    result = cmd_run.run_mart_validation(cfg, 2022, logger, sample_mode=False)
    assert result.get("passed") is False, f"Expected failed in normal mode, got: {result}"


@pytest.mark.regression
def test_run_full_second_validation_block_uses_sample_mode(tmp_path: Path, monkeypatch) -> None:
    """Il blocco di validazione finale in run_full() riceve sample_mode=True con --sample-rows."""
    config_path = tmp_path / "dataset.yml"
    _write_config_with_min_rows(config_path, min_rows=1000)

    sample_mode_passed = {"clean": False, "mart": False}

    def _tracking_clean_validation(cfg, year, logger, *, sample_mode=False):
        sample_mode_passed["clean"] = sample_mode
        return _ok_summary()

    def _tracking_mart_validation(cfg, year, logger, *, sample_mode=False):
        sample_mode_passed["mart"] = sample_mode
        return _ok_summary()

    monkeypatch.setattr(cmd_run, "run_raw", lambda *args, **kwargs: None)
    monkeypatch.setattr(cmd_run, "run_clean", lambda *args, **kwargs: None)
    monkeypatch.setattr(cmd_run, "run_mart", lambda *args, **kwargs: None)
    monkeypatch.setattr(cmd_run, "run_raw_validation", lambda *args, **kwargs: _ok_summary())
    monkeypatch.setattr(cmd_run, "run_clean_validation", _tracking_clean_validation)
    monkeypatch.setattr(cmd_run, "run_mart_validation", _tracking_mart_validation)
    monkeypatch.setattr(cmd_run, "_review_readiness", lambda *args, **kwargs: {"readiness": "ready", "check_count": 0, "ok_count": 0, "fail_count": 0})

    # Esegue run full COME se chiamato da CI con --sample-rows 1000
    # (tutti i parametri espliciti per bypassare i default Typer che
    #  altrimenti restituiscono oggetti OptionInfo invece di None)
    cmd_run.run_full(
        config=str(config_path),
        smoke=False,
        sample_rows=1000,
        sample_bytes=None,
        years=None,
        root=None,
        json_output=False,
        dry_run=False,
        strict_config=False,
    )

    assert sample_mode_passed["clean"] is True, "run_full deve passare sample_mode=True alla validazione clean"
    assert sample_mode_passed["mart"] is True, "run_full deve passare sample_mode=True alla validazione mart"
