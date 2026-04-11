from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import duckdb
from toolkit.core.config import load_config


TOOLKIT_ROOT = Path(__file__).resolve().parents[2]
WORKSPACE_ROOT = Path(os.environ.get("DATACIVICLAB_WORKSPACE", str(TOOLKIT_ROOT.parent))).expanduser()
TOOLKIT_PYTHON = Path(os.environ.get("DATACIVICLAB_TOOLKIT_PYTHON", sys.executable))


class ToolkitClientError(RuntimeError):
    pass


def _safe_path(config_path: str) -> Path:
    path = Path(config_path).expanduser()
    if not path.is_absolute():
        path = (WORKSPACE_ROOT / path).resolve()
    if not path.exists():
        raise ToolkitClientError(f"Config non trovata: {path}")
    return path


def _load_cfg(config_path: str) -> tuple[Path, Any]:
    config = _safe_path(config_path)
    try:
        cfg = load_config(str(config), strict_config=False)
    except Exception as exc:
        raise ToolkitClientError(f"Load config fallito per {config}: {exc}") from exc
    return config, cfg


def _toolkit_json(args: list[str]) -> dict[str, Any]:
    cmd = [str(TOOLKIT_PYTHON), "-m", "toolkit.cli.app", *args]
    env = os.environ.copy()
    env.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        result = subprocess.run(
            cmd,
            cwd=str(TOOLKIT_ROOT),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=env,
            check=False,
        )
    except Exception as exc:
        raise ToolkitClientError(f"Esecuzione toolkit CLI fallita: {exc}") from exc

    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        stdout = (result.stdout or "").strip()
        detail = stderr or stdout or f"exit code {result.returncode}"
        raise ToolkitClientError(f"toolkit CLI fallita: {detail}")

    try:
        return json.loads(result.stdout)
    except Exception as exc:
        raise ToolkitClientError("toolkit CLI non ha restituito JSON valido") from exc


def inspect_paths(config_path: str, year: int | None = None) -> dict[str, Any]:
    config = _safe_path(config_path)
    args = ["inspect", "paths", "--config", str(config), "--json"]
    if year is not None:
        args.extend(["--year", str(year)])
    return _toolkit_json(args)


def _sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _schema_from_parquet(parquet_path: Path) -> dict[str, Any]:
    if not parquet_path.exists():
        raise ToolkitClientError(f"Parquet non trovato: {parquet_path}")
    relation = f"read_parquet('{_sql_literal(str(parquet_path))}')"
    try:
        with duckdb.connect(database=":memory:") as conn:
            conn.execute("PRAGMA disable_progress_bar")
            describe_rows = conn.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
    except Exception as exc:
        raise ToolkitClientError(f"Lettura schema parquet fallita per {parquet_path}: {exc}") from exc

    columns = [{"name": row[0], "type": row[1]} for row in describe_rows]
    return {"path": str(parquet_path), "column_count": len(columns), "columns": columns}


def show_schema(config_path: str, layer: str = "clean", year: int | None = None) -> dict[str, Any]:
    config, _cfg = _load_cfg(config_path)
    safe_layer = (layer or "clean").strip().lower()
    if safe_layer not in {"raw", "clean", "mart"}:
        raise ToolkitClientError("layer deve essere uno tra: raw, clean, mart")

    if safe_layer == "raw":
        try:
            payload = _toolkit_json(["inspect", "schema-diff", "--config", str(config), "--json"])
        except Exception as exc:
            raise ToolkitClientError(f"show_schema(raw) fallito per {config}: {exc}") from exc
        # `schema-diff` restituisce l'intero payload raw; il filtro per anno resta qui lato client.
        entries = [e for e in payload.get("entries", []) if year is None or e.get("year") == year]
        return {
            "dataset": payload.get("dataset"),
            "layer": "raw",
            "year": year,
            "entry_count": len(entries),
            "entries": entries,
        }

    paths = inspect_paths(str(config), year)
    if safe_layer == "clean":
        parquet_path = Path(paths["paths"]["clean"]["output"])
        payload = _schema_from_parquet(parquet_path)
    else:
        outputs = paths["paths"]["mart"].get("outputs") or []
        if not outputs:
            raise ToolkitClientError("Nessun output mart risolto dal toolkit")
        parquet_path = Path(outputs[0])
        payload = _schema_from_parquet(parquet_path)
        payload["available_outputs"] = outputs
        if len(outputs) > 1:
            payload["warning"] = (
                "Sono presenti piu' output mart; lo schema mostrato riguarda solo il primo output."
            )

    payload.update(
        {
            "dataset": paths.get("dataset"),
            "year": paths.get("year"),
            "layer": safe_layer,
            "config_path": str(config),
        }
    )
    return payload


def run_state(config_path: str, year: int | None = None) -> dict[str, Any]:
    config = _safe_path(config_path)
    paths = inspect_paths(str(config), year)
    run_dir = Path(paths["paths"]["run_dir"])
    run_files = sorted(run_dir.glob("*.json")) if run_dir.exists() else []
    latest_run = paths.get("latest_run")
    latest_payload = None
    if latest_run and latest_run.get("path"):
        latest_path = Path(latest_run["path"])
        if latest_path.exists():
            latest_payload = json.loads(latest_path.read_text(encoding="utf-8"))
    years_seen = (
        sorted({p.parent.name for p in run_dir.parent.glob("*/*.json") if p.parent.name.isdigit()})
        if run_dir.parent.exists()
        else []
    )
    return {
        "dataset": paths.get("dataset"),
        "config_path": str(config),
        "requested_year": year,
        "run_dir": str(run_dir),
        "run_dir_exists": run_dir.exists(),
        "run_file_count": len(run_files),
        "years_seen": years_seen,
        "latest_run": latest_run,
        "latest_run_record": latest_payload,
    }


def _exists(path: str | None) -> bool:
    if not path:
        return False
    return Path(path).exists()


def summary(config_path: str, year: int | None = None) -> dict[str, Any]:
    config = _safe_path(config_path)
    paths = inspect_paths(str(config), year)
    raw_paths = paths["paths"]["raw"]
    clean_paths = paths["paths"]["clean"]
    mart_paths = paths["paths"]["mart"]
    run_dir = Path(paths["paths"]["run_dir"])

    raw_dir = Path(raw_paths["dir"])
    clean_dir = Path(clean_paths["dir"])
    mart_dir = Path(mart_paths["dir"])
    mart_outputs = list(mart_paths.get("outputs") or [])
    missing_mart_outputs = [output for output in mart_outputs if not _exists(output)]

    primary_output_file = (paths.get("raw_hints") or {}).get("primary_output_file")
    primary_output_path = str(raw_dir / primary_output_file) if primary_output_file else None

    latest_run = paths.get("latest_run") or {}
    latest_run_path = latest_run.get("path")

    run_files = sorted(run_dir.glob("*.json")) if run_dir.exists() else []

    warnings: list[str] = []
    if primary_output_file and not _exists(primary_output_path):
        warnings.append("raw_output_missing")
    if not _exists(clean_paths.get("output")):
        warnings.append("clean_output_missing")
    if mart_outputs and missing_mart_outputs:
        warnings.append("mart_outputs_missing")
    if latest_run_path and not _exists(latest_run_path):
        warnings.append("latest_run_record_missing")

    return {
        "dataset": paths.get("dataset"),
        "config_path": str(config),
        "year": paths.get("year"),
        "layers": {
            "raw": {
                "dir": str(raw_dir),
                "dir_exists": raw_dir.exists(),
                "manifest_exists": _exists(raw_paths.get("manifest")),
                "metadata_exists": _exists(raw_paths.get("metadata")),
                "primary_output_file": primary_output_file,
                "primary_output_exists": _exists(primary_output_path),
                "suggested_read_exists": (paths.get("raw_hints") or {}).get(
                    "suggested_read_exists"
                ),
            },
            "clean": {
                "dir": str(clean_dir),
                "dir_exists": clean_dir.exists(),
                "output": clean_paths.get("output"),
                "output_exists": _exists(clean_paths.get("output")),
                "manifest_exists": _exists(clean_paths.get("manifest")),
                "metadata_exists": _exists(clean_paths.get("metadata")),
            },
            "mart": {
                "dir": str(mart_dir),
                "dir_exists": mart_dir.exists(),
                "outputs": mart_outputs,
                "output_count": len(mart_outputs),
                "output_exists_count": len(mart_outputs) - len(missing_mart_outputs),
                "missing_outputs": missing_mart_outputs,
                "manifest_exists": _exists(mart_paths.get("manifest")),
                "metadata_exists": _exists(mart_paths.get("metadata")),
            },
        },
        "run": {
            "run_dir": str(run_dir),
            "run_dir_exists": run_dir.exists(),
            "run_file_count": len(run_files),
            "latest_run": latest_run or None,
            "latest_run_record_exists": _exists(latest_run_path),
        },
        "warnings": warnings,
    }
