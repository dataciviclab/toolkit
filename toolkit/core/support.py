from __future__ import annotations

from pathlib import Path
from typing import Any

from toolkit.core.config import load_config
from toolkit.core.paths import layer_year_dir


def _support_expected_mart_outputs(cfg, year: int) -> list[Path]:
    tables = cfg.mart.get("tables") or []
    mart_dir = layer_year_dir(cfg.root, "mart", cfg.dataset, year)
    outputs: list[Path] = []
    for table in tables:
        if not isinstance(table, dict):
            continue
        name = table.get("name")
        if not name:
            continue
        outputs.append(mart_dir / f"{name}.parquet")
    return outputs


def resolve_support_payloads(
    support_entries: list[dict[str, Any]] | None,
    *,
    require_exists: bool,
) -> list[dict[str, Any]]:
    resolved: list[dict[str, Any]] = []
    for entry in support_entries or []:
        name = str(entry["name"])
        config_path = Path(entry["config"])
        years = [int(year) for year in entry.get("years") or []]
        support_cfg = load_config(config_path)

        year_payloads: list[dict[str, Any]] = []
        all_outputs: list[str] = []
        for year in years:
            expected_paths = _support_expected_mart_outputs(support_cfg, year)
            output_paths = [str(path) for path in expected_paths]
            existing_paths = [str(path) for path in expected_paths if path.exists()]
            if require_exists and not existing_paths:
                raise FileNotFoundError(
                    "Support dataset output mancante: "
                    f"{name} ({config_path}) anno {year}. "
                    "Esegui prima il MART del support dataset o correggi support[].years."
                )
            year_payloads.append(
                {
                    "year": year,
                    "dataset": support_cfg.dataset,
                    "config_path": str(config_path),
                    "mart_dir": str(layer_year_dir(support_cfg.root, "mart", support_cfg.dataset, year)),
                    "outputs": output_paths,
                    "existing_outputs": existing_paths,
                    "all_outputs_exist": len(output_paths) > 0 and len(existing_paths) == len(output_paths),
                }
            )
            all_outputs.extend(existing_paths if require_exists else output_paths)

        resolved.append(
            {
                "name": name,
                "config_path": str(config_path),
                "dataset": support_cfg.dataset,
                "years": years,
                "years_resolved": year_payloads,
                "outputs": all_outputs,
                "mart": all_outputs[0] if all_outputs else None,
            }
        )
    return resolved


def flatten_support_template_ctx(payloads: list[dict[str, Any]]) -> dict[str, Any]:
    ctx: dict[str, Any] = {}
    for payload in payloads:
        name = payload["name"]
        ctx[f"support.{name}.outputs"] = payload["outputs"]
        ctx[f"support.{name}.mart"] = payload["mart"]
    return ctx
