"""Smoke test: parquet_preview su bucket GCS pubblici via S3.

Questo file NON e' in CORE_TESTS ne in ADVANCED_TESTS (conftest.py).
Richiede connettivita' esterna. Eseguire esplicitamente:

    pytest tests/test_s3_parquet_smoke.py -v
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.smoke


@pytest.mark.smoke
def test_parquet_preview_s3_public_bucket() -> None:
    """parquet_preview su bucket GCS pubblico: schema, count, preview, SQL."""
    from toolkit.core.duckdb_shape import (
        parquet_preview,
        parquet_row_count,
        parquet_schema,
    )

    url = "s3://dataciviclab-clean/catalog_inventory/catalog_inventory_latest.parquet"
    p = Path(url)

    # Schema
    schema = parquet_schema(p)
    assert len(schema) > 0, "S3 schema vuoto"

    # Row count
    count = parquet_row_count(p)
    assert count is not None and count > 0, f"S3 count: {count}"

    # Preview
    result = parquet_preview(p, limit=3)
    assert result["column_count"] > 0
    assert result["row_count"] == count
    assert len(result["preview"]) == 3

    # SQL
    sql_result = parquet_preview(
        p,
        sql="SELECT source_id, COUNT(*) AS n FROM data GROUP BY source_id ORDER BY n DESC LIMIT 3",
    )
    assert sql_result["column_count"] == 2
    assert len(sql_result["preview"]) == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
