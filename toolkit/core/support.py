from __future__ import annotations

from pathlib import Path
from typing import Any

from toolkit.core.config import SupportDatasetConfig, load_config
from toolkit.core.paths import layer_year_dir


def _support_expected_mart_outputs(cfg, year: int) -> list[Path]:
    table_names = [t.name for t in cfg.mart.tables if t.name]
    mart_dir = layer_year_dir(cfg.root, "mart", cfg.dataset, year)
    return [mart_dir / f"{name}.parquet" for name in table_names]


def resolve_support_payloads(
    support_entries: list[dict[str, Any]] | None,
    *,
    require_exists: bool,
    smoke: bool = False,
) -> list[dict[str, Any]]:
    resolved: list[dict[str, Any]] = []
    for entry in support_entries or []:
        name = str(entry["name"])
        config_path = Path(entry["config"])
        years = [int(year) for year in entry.get("years") or []]
        # Smoke mode: output del support e' in {root}/smoke/data/... (root override eseguito in run_full)
        if smoke:
            _sup0 = load_config(config_path)
            support_cfg = load_config(config_path, root_override=_sup0.root / "smoke")
        else:
            support_cfg = load_config(config_path)

        year_payloads: list[dict[str, Any]] = []
        all_outputs: list[str] = []
        for year in years:
            expected_paths = _support_expected_mart_outputs(support_cfg, year)
            output_paths = [str(path) for path in expected_paths]
            existing_paths = [str(path) for path in expected_paths if path.exists()]
            all_outputs_exist = len(output_paths) > 0 and len(existing_paths) == len(output_paths)
            if require_exists and not output_paths:
                raise ValueError(
                    "Support dataset MART non configurato: "
                    f"{name} ({config_path}) anno {year}. "
                    "Il dataset di supporto deve dichiarare almeno una tabella in mart.tables."
                )
            if require_exists and not all_outputs_exist:
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
                    "mart_dir": str(
                        layer_year_dir(support_cfg.root, "mart", support_cfg.dataset, year)
                    ),
                    "outputs": output_paths,
                    "existing_outputs": existing_paths,
                    "all_outputs_exist": all_outputs_exist,
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


def resolve_transitive_supports(
    support_entries: list[SupportDatasetConfig],
    *,
    _visited: set[Path] | None = None,
    _path: list[str] | None = None,
) -> list[SupportDatasetConfig]:
    """Resolve transitive support dipendenze in un flat DAG eseguibile.

    Ordine topologico: le dipendenze più profonde (annidate) vengono prima.
    Deduplica per config path risolto (stesso file YAML eseguito una volta sola).
    Rileva cicli e solleva ValueError.

    Returns:
        Lista piatta di ``SupportDatasetConfig`` in ordine di esecuzione
        (deepest-first). Include sia le entry dirette che quelle transitive.

    Raises:
        ValueError: se una config di supporto non è caricabile o viene rilevato
        un ciclo nelle dipendenze.
    """
    if _visited is None:
        _visited = set()
    if _path is None:
        _path = []

    result: list[SupportDatasetConfig] = []

    for entry in support_entries or []:
        config_path = Path(str(entry.config)).resolve()

        # Già processato (dedup — stesso file YAML)
        if config_path in _visited:
            continue

        # Ciclo: questo config_path è già nello stack ricorsivo
        path_str = str(config_path)
        if path_str in _path:
            cycle = " → ".join(_path + [path_str])
            raise ValueError(
                f"Circular support dependency detected: {cycle}. "
                "Verifica support[].config references in dataset.yml."
            )

        # Carica config del support per controllare se ha nested support
        try:
            cfg = load_config(str(config_path))
        except Exception as exc:
            raise ValueError(
                f"Cannot load support config '{entry.name}' ({entry.config}): {exc}"
            ) from exc

        # Ricorsione: nested support prima (deepest-first)
        if cfg.support:
            nested = resolve_transitive_supports(
                cfg.support,
                _visited=_visited,
                _path=_path + [path_str],
            )
            result.extend(nested)

        _visited.add(config_path)
        result.append(entry)

    return result
