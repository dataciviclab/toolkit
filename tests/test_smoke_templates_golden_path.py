from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from toolkit.cli.cmd_run import run as run_cmd
from toolkit.cli.cmd_validate import validate as validate_cmd
from tests.helpers_assert_paths import (
    assert_file_replaceable,
    assert_golden_path_artifacts,
    assert_metadata_file,
    assert_no_absolute_paths,
)


@pytest.mark.contract
def test_smoke_offline_golden_path(smoke_offline: Path) -> None:
    """Golden path su 3 template offline: run all + validate all + artifact.

    Copre source type ``local_file`` e ``http_file`` (con server locale).
    Sostituisce i test golden path uno-per-uno con un test parametrizzato.
    """
    config_path = smoke_offline / "dataset.yml"
    assert config_path.exists(), f"dataset.yml non trovato in {smoke_offline}"

    # Run completa
    run_cmd(step="all", config=str(config_path))
    validate_cmd(step="all", config=str(config_path))

    # Leggi configurazione per sapere anni e tabelle mart
    with open(config_path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    years = cfg["dataset"].get("years", [])
    mart_tables = [t["name"] for t in cfg.get("mart", {}).get("tables", [])]
    name = smoke_offline.name
    root = smoke_offline / "_smoke_out"

    # Verifica struttura artifact completa
    assert_golden_path_artifacts(root, name, years, mart_tables)

    # Verifica portabilità metadata: clean e mart non devono contenere
    # path assoluti. La metadata raw può contenere path sorgente legittimi.
    for year in years:
        for layer in ("clean", "mart"):
            meta = assert_metadata_file(root, name, layer, year)
            assert_no_absolute_paths(meta, root)

    # Verifica che un parquet clean sia sostituibile
    first_year = years[0]
    clean_parquet = (
        root / "data" / "clean" / name / str(first_year) / f"{name}_{first_year}_clean.parquet"
    )
    if clean_parquet.exists():
        assert_file_replaceable(clean_parquet)
