"""Shared helpers for MCP schema operations — re-exported from cli/inspect.

Tutti gli helper sono in ``toolkit.cli.inspect._helpers``.
Qui MCP li re-esporta wrappando gli errori con ``ToolkitClientError``
dove serve, o semplicemente re-indirizzando per backward compat.

Lazy import per evitare circular import con cli/inspect/ -> cli/cmd_*.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from lab_connectors.mcp.errors import ErrorCode

from toolkit.mcp.errors import ToolkitClientError


def _cli_helpers():
    """Lazy import to avoid circular dependency with cli/inspect."""
    from toolkit.cli.inspect._helpers import (  # noqa: F401
        _check_run_record_coherence,
        _exists,
        _read_parquet_preview,
        _read_parquet_row_count,
        _read_validation_content,
        _schema_from_parquet,
        _sql_literal,
        _validation_summary_for_layer,
    )
    return (
        _check_run_record_coherence,
        _exists,
        _read_parquet_preview,
        _read_parquet_row_count,
        _read_validation_content,
        _schema_from_parquet,
        _sql_literal,
        _validation_summary_for_layer,
    )


# --- Pure re-exports (no error wrapping needed) ---


def _sql_literal(value: str) -> str:
    return _cli_helpers()[6](value)


def _read_parquet_row_count(parquet_path: Path | None) -> int | None:
    return _cli_helpers()[3](parquet_path)


def _exists(path: str | None) -> bool:
    return _cli_helpers()[1](path)


def _read_validation_content(path: str | None) -> dict[str, Any] | None:
    return _cli_helpers()[4](path)


def _check_run_record_coherence(
    run_record: dict[str, Any] | None, layers: dict[str, Any]
) -> list[dict[str, str]]:
    return _cli_helpers()[0](run_record, layers)


def _validation_summary_for_layer(
    layer_dir: Path, validation_filename: str
) -> dict[str, Any] | None:
    return _cli_helpers()[7](layer_dir, validation_filename)


# --- Wrapped with ToolkitClientError ---


def _schema_from_parquet(parquet_path: Path) -> dict[str, Any]:
    """Return schema (columns + count) of a parquet file via DuckDB.

    Wraps CLI implementation with MCP error handling.
    """
    try:
        return _cli_helpers()[5](parquet_path)
    except FileNotFoundError as exc:
        raise ToolkitClientError(str(exc), code=ErrorCode.PARQUET_NOT_FOUND) from exc
    except RuntimeError as exc:
        raise ToolkitClientError(str(exc), code=ErrorCode.DUCKDB_ERROR) from exc


def _read_parquet_preview(parquet_path: Path, limit: int = 10) -> dict[str, Any]:
    """Return schema + row preview of a parquet file via DuckDB.

    Wraps CLI implementation with MCP error handling.
    """
    try:
        return _cli_helpers()[2](parquet_path, limit=limit)
    except FileNotFoundError as exc:
        raise ToolkitClientError(str(exc), code=ErrorCode.PARQUET_NOT_FOUND) from exc
    except RuntimeError as exc:
        raise ToolkitClientError(str(exc), code=ErrorCode.DUCKDB_ERROR) from exc
