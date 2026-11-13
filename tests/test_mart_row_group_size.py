"""Test: `mart.tables[].row_group_size` finisce nel parquet di output.

Il row group è l'unità minima che un lettore scarica da un parquet remoto
(GCS/HTTP): con il default DuckDB (122.880 righe) una tabella da 22.000 righe
finisce in **un** blocco e ogni lookup per chiave legge tutto il file.
`row_group_size` è l'unico modo per dare granularità a quelle tabelle.

Il test verifica l'effetto sul file scritto, non l'opzione SQL: due tabelle
identiche, una con il campo e una senza, devono avere un numero di row group
diverso.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest
import yaml

from toolkit.cli.cmd_run import run as run_cmd
from toolkit.core.config import MartTableConfig

pytestmark = [pytest.mark.contract, pytest.mark.core]

_SQL_5000 = "sql/mart/righe_5000.sql"


def _row_groups(path: Path) -> int:
    con = duckdb.connect(":memory:")
    try:
        return con.execute(
            f"SELECT COUNT(DISTINCT row_group_id) FROM parquet_metadata('{path}')"
        ).fetchone()[0]
    finally:
        con.close()


def test_config_valida_row_group_size() -> None:
    cfg = MartTableConfig.from_dict({"name": "t", "sql": "sql/t.sql", "row_group_size": 4096})
    assert cfg.row_group_size == 4096
    assert MartTableConfig.from_dict({"name": "t", "sql": "sql/t.sql"}).row_group_size is None


def test_config_rifiuta_row_group_size_sotto_il_minimo() -> None:
    """DuckDB alza in silenzio a 2048: accettare 256 sarebbe un no-op silenzioso."""
    with pytest.raises(ValueError, match="STANDARD_VECTOR_SIZE"):
        MartTableConfig.from_dict({"name": "t", "sql": "sql/t.sql", "row_group_size": 256})


def test_row_group_size_nel_parquet_scritto(project_example: Path) -> None:
    sql_dir = project_example / "sql" / "mart"
    (sql_dir / "righe_5000.sql").write_text(
        "SELECT i AS id, repeat('x', 200) AS testo FROM range(5000) t(i)\n",
        encoding="utf-8",
    )

    config_path = project_example / "dataset.yml"
    data = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    data["mart"]["tables"].append(
        {"name": "con_row_group", "sql": _SQL_5000, "row_group_size": 2048}
    )
    data["mart"]["tables"].append({"name": "senza_row_group", "sql": _SQL_5000})
    config_path.write_text(
        yaml.dump(data, default_flow_style=False, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )

    run_cmd(step="all", config=str(config_path))

    mart_dir = project_example / "_smoke_out" / "data" / "mart" / "project_example" / "2022"
    con_row_group = _row_groups(mart_dir / "con_row_group.parquet")
    senza_row_group = _row_groups(mart_dir / "senza_row_group.parquet")

    assert senza_row_group == 1, "controllo: 5.000 righe stanno in un row group con il default"
    assert con_row_group == 3, f"attesi 3 row group da 2048 righe, trovati {con_row_group}"
