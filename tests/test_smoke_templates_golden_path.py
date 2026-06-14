from __future__ import annotations

from pathlib import Path

import pytest

from toolkit.cli.cmd_run import run as run_cmd
from toolkit.cli.cmd_validate import validate as validate_cmd


@pytest.mark.contract
def test_smoke_offline_golden_path(smoke_offline: Path) -> None:
    """Golden path su ogni smoke offline: run all + validate all + artifact check.

    Sostituisce i test golden path uno-per-uno con un test parametrizzato
    su tutti gli smoke ``local_file``.
    """
    config_path = smoke_offline / "dataset.yml"
    assert config_path.exists(), f"dataset.yml non trovato in {smoke_offline}"

    # Run completa
    run_cmd(step="all", config=str(config_path))
    validate_cmd(step="all", config=str(config_path))

    # Verifica artifact di base: devono esistere per ogni layer
    _assert_artifacts_exist(smoke_offline)


def _assert_artifacts_exist(smoke_dir: Path) -> None:
    """Verifica che la struttura minima degli artifact sia presente per uno smoke."""
    root = smoke_dir / "_smoke_out"
    name = smoke_dir.name

    # Scopriamo anno(i) e mart tables dal dataset.yml
    import yaml

    with open(smoke_dir / "dataset.yml", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    years = cfg["dataset"].get("years", [])
    mart_tables = cfg.get("mart", {}).get("tables", [])

    for year in years:
        year_str = str(year)
        # RAW
        raw_dir = root / "data" / "raw" / name / year_str
        assert raw_dir.is_dir(), f"RAW dir mancante: {raw_dir}"
        assert (raw_dir / "raw_validation.json").exists(), (
            f"raw_validation.json mancante in {raw_dir}"
        )

        # CLEAN
        clean_dir = root / "data" / "clean" / name / year_str
        assert clean_dir.is_dir(), f"CLEAN dir mancante: {clean_dir}"
        parquets = list(clean_dir.glob("*.parquet"))
        assert len(parquets) >= 1, f"Nessun parquet CLEAN in {clean_dir}"
        assert (clean_dir / "metadata.json").exists(), f"metadata.json mancante in {clean_dir}"

        # MART
        mart_dir = root / "data" / "mart" / name / year_str
        assert mart_dir.is_dir(), f"MART dir mancante: {mart_dir}"
        for table in mart_tables:
            table_name = table["name"]
            assert (mart_dir / f"{table_name}.parquet").exists(), (
                f"Mart table {table_name}.parquet mancante in {mart_dir}"
            )
        assert (mart_dir / "_validate" / "mart_validation.json").exists(), (
            f"mart_validation.json mancante in {mart_dir}"
        )
