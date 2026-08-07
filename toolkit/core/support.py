from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path
from typing import Any

from toolkit.core.config import load_config
from toolkit.core.exceptions import DownloadError
from toolkit.core.paths import layer_year_dir

# Tipi di support (ADR-005): dataset (default), codelist, file.
SUPPORT_TYPES = ("dataset", "codelist", "file")

# Convenzione layer materializzazione codelist: out/data/support/{name}/{name}.parquet
SUPPORT_LAYER = "support"


def _support_expected_mart_outputs(cfg, year: int) -> list[Path]:
    table_names = [t.name for t in cfg.mart.tables if t.name]
    mart_dir = layer_year_dir(cfg.root, "mart", cfg.dataset, year)
    return [mart_dir / f"{name}.parquet" for name in table_names]


def _support_expected_clean_output(cfg, year: int) -> Path:
    """Parquet CLEAN del support dataset (ADR-005: output attesi = clean + mart)."""
    clean_dir = layer_year_dir(cfg.root, "clean", cfg.dataset, year)
    return clean_dir / f"{cfg.dataset}_{year}_clean.parquet"


def _codelist_output_path(root: Path, name: str) -> Path:
    """Parquet canonico di una codelist materializzata (ADR-005)."""
    return root / "data" / SUPPORT_LAYER / name / f"{name}.parquet"


def _entry_type(entry: dict[str, Any]) -> str:
    stype = str(entry.get("type") or "dataset")
    if stype not in SUPPORT_TYPES:
        raise ValueError(
            f"support[].type must be one of {SUPPORT_TYPES}, got {stype!r} "
            f"for support '{entry.get('name')}'"
        )
    return stype


def resolve_support_payloads(
    support_entries: list[dict[str, Any]] | None,
    *,
    require_exists: bool,
    smoke: bool = False,
    root: Path | None = None,
) -> list[dict[str, Any]]:
    """Risolve gli output attesi di ogni support entry, per tipo (ADR-005).

    - ``type: dataset``: carica il config del support e risolve clean + tutte
      le tabelle mart per anno (comportamento storico esteso al clean).
    - ``type: codelist``: parquet canonico in ``{root}/data/support/{name}/``
      (richiede ``root`` = root del candidate).
    - ``type: file``: path dichiarato (relativo normalizzato sul root).

    Con ``require_exists=True`` gli output mancanti sollevano errore esplicito
    (mart/clean del support non ancora eseguito, o codelist/file non
    materializzati).
    """
    resolved: list[dict[str, Any]] = []
    for entry in support_entries or []:
        name = str(entry["name"])
        stype = _entry_type(entry)

        if stype == "dataset":
            resolved.append(_resolve_dataset_entry(entry, name, require_exists, smoke))
        elif stype == "codelist":
            resolved.append(_resolve_codelist_entry(entry, name, require_exists, root))
        else:  # file
            resolved.append(_resolve_file_entry(entry, name, require_exists, root))
    return resolved


def _resolve_dataset_entry(
    entry: dict[str, Any], name: str, require_exists: bool, smoke: bool
) -> dict[str, Any]:
    if "config" not in entry:
        raise ValueError(f"support dataset '{name}' requires 'config'")
    config_path = Path(entry["config"])
    years = [int(year) for year in entry.get("years") or []]
    if smoke:
        _sup0 = load_config(config_path)
        support_cfg = load_config(config_path, root_override=_sup0.root / "smoke")
    else:
        support_cfg = load_config(config_path)

    year_payloads: list[dict[str, Any]] = []
    all_outputs: list[str] = []
    for year in years:
        mart_paths = _support_expected_mart_outputs(support_cfg, year)
        clean_path = _support_expected_clean_output(support_cfg, year)
        expected_paths = [clean_path, *mart_paths]
        output_paths = [str(path) for path in expected_paths]
        existing_paths = [str(path) for path in expected_paths if path.exists()]
        all_outputs_exist = len(output_paths) > 0 and len(existing_paths) == len(output_paths)
        if require_exists and not mart_paths:
            raise ValueError(
                "Support dataset MART non configurato: "
                f"{name} ({config_path}) anno {year}. "
                "Il dataset di supporto deve dichiarare almeno una tabella in mart.tables."
            )
        if require_exists and not all_outputs_exist:
            missing = [p for p in expected_paths if not p.exists()]
            raise FileNotFoundError(
                "Support dataset output mancante: "
                f"{name} ({config_path}) anno {year}: {missing[0]}. "
                "Esegui prima il run del support dataset o correggi support[].years."
            )
        year_payloads.append(
            {
                "year": year,
                "dataset": support_cfg.dataset,
                "config_path": str(config_path),
                "mart_dir": str(
                    layer_year_dir(support_cfg.root, "mart", support_cfg.dataset, year)
                ),
                "clean": str(clean_path),
                "outputs": output_paths,
                "existing_outputs": existing_paths,
                "all_outputs_exist": all_outputs_exist,
            }
        )
        all_outputs.extend(existing_paths if require_exists else output_paths)

    mart_by_table: dict[str, str] = {}
    for table in support_cfg.mart.tables or []:
        if not table.name:
            continue
        year = years[0] if years else 0
        mart_dir = layer_year_dir(support_cfg.root, "mart", support_cfg.dataset, year)
        mart_by_table[table.name] = str(mart_dir / f"{table.name}.parquet")

    return {
        "name": name,
        "type": "dataset",
        "config_path": str(config_path),
        "dataset": support_cfg.dataset,
        "years": years,
        "years_resolved": year_payloads,
        "outputs": all_outputs,
        "mart": (str(mart_paths[0]) if mart_paths else None),
        "mart_by_table": mart_by_table,
        "clean": (year_payloads[0]["clean"] if year_payloads else None),
        "path": None,
        "all_outputs_exist": all(yp["all_outputs_exist"] for yp in year_payloads)
        if year_payloads
        else False,
    }


def _resolve_codelist_entry(
    entry: dict[str, Any], name: str, require_exists: bool, root: Path | None
) -> dict[str, Any]:
    if root is None:
        raise ValueError(f"support codelist '{name}' requires the candidate root")
    if not entry.get("id"):
        raise ValueError(f"support codelist '{name}' requires 'id'")
    target = _codelist_output_path(root, name)
    all_outputs_exist = target.exists()
    if require_exists and not all_outputs_exist:
        raise FileNotFoundError(
            f"Support codelist '{name}' non materializzata: {target}. "
            "Esegui il run del candidate (materializza prima) o --refresh-support."
        )
    return {
        "name": name,
        "type": "codelist",
        "config_path": None,
        "dataset": None,
        "years": [],
        "years_resolved": [],
        "outputs": [str(target)],
        "existing_outputs": [str(target)] if all_outputs_exist else [],
        "all_outputs_exist": all_outputs_exist,
        "mart": None,
        "mart_by_table": {},
        "clean": None,
        "path": str(target),
    }


def _resolve_file_entry(
    entry: dict[str, Any], name: str, require_exists: bool, root: Path | None
) -> dict[str, Any]:
    if not entry.get("path"):
        raise ValueError(f"support file '{name}' requires 'path'")
    if root is None:
        raise ValueError(f"support file '{name}' requires the candidate root")
    raw_path = Path(entry["path"])
    target = raw_path if raw_path.is_absolute() else Path(root) / raw_path
    all_outputs_exist = target.exists()
    if require_exists and not all_outputs_exist:
        raise FileNotFoundError(
            f"Support file '{name}' mancante: {target}. "
            "Esegui il run del candidate (materializza prima) o --refresh-support."
        )
    return {
        "name": name,
        "type": "file",
        "config_path": None,
        "dataset": None,
        "years": [],
        "years_resolved": [],
        "outputs": [str(target)],
        "existing_outputs": [str(target)] if all_outputs_exist else [],
        "all_outputs_exist": all_outputs_exist,
        "mart": None,
        "mart_by_table": {},
        "clean": None,
        "path": str(target),
    }


def materialize_support(
    entry: dict[str, Any], *, root: Path | None = None, smoke: bool = False
) -> Path | None:
    """Materializza un support non-dataset (codelist/file) sul disco.

    `type: dataset` NON è materializzabile qui: il run del support avviene
    nell'orchestrazione (cmd_run), che ha il config completo.

    Ritorna il path materializzato (o None per il tipo file, che scrive dove
    dichiarato dal command).
    """
    name = str(entry["name"])
    stype = _entry_type(entry)
    if stype == "dataset":
        raise ValueError(f"support dataset '{name}' si materializza via run, non qui")
    if stype == "codelist":
        if root is None:
            raise ValueError(f"support codelist '{name}' requires root (candidate)")
        return materialize_codelist_to(entry, root, smoke=smoke)
    _materialize_file(entry, name)
    return None


def _materialize_file(entry: dict[str, Any], name: str) -> None:
    command = entry.get("command")
    if not command:
        raise DownloadError(f"support file '{name}' mancante e senza 'command' per rigenerarlo")
    if os.environ.get("TOOLKIT_ALLOW_SCRIPT_SOURCE") != "1":
        raise DownloadError(
            "support file materialization requires TOOLKIT_ALLOW_SCRIPT_SOURCE=1 "
            f"(command per '{name}')"
        )
    result = subprocess.run(command, shell=True, capture_output=True, text=True, timeout=600)
    if result.returncode != 0:
        stderr_preview = result.stderr[:500] if result.stderr else "(no stderr)"
        raise DownloadError(
            f"Support file command failed (exit {result.returncode}): {stderr_preview}"
        )


def materialize_codelist_to(entry: dict[str, Any], root: Path, *, smoke: bool = False) -> Path:
    """Fetch codelist (SDMX) e scrive il parquet canonico nel support layer."""
    from toolkit.plugins.sdmx import SdmxSource

    provider = str(entry.get("provider") or "sdmx")
    if provider != "sdmx":
        raise DownloadError(
            f"support codelist '{entry.get('name')}': provider {provider!r} non supportato"
        )
    agency = str(entry.get("agency") or "ESTAT")
    codelist_id = str(entry["id"])
    name = str(entry["name"])

    src = SdmxSource(timeout=60, retries=2)
    result = src.fetch_codelist(codelist_id, agency=agency)

    target = _codelist_output_path(root, name)
    target.parent.mkdir(parents=True, exist_ok=True)
    _write_codelist_parquet(result, target)
    return target


def _write_codelist_parquet(result: dict[str, object], target: Path) -> None:
    """Serializza {codes, annotations} in parquet: code, label_en + annotation types."""
    import duckdb

    codes: dict[str, str] = result.get("codes") or {}
    annotations: dict[str, dict[str, str]] = result.get("annotations") or {}

    ann_types: list[str] = []
    for ann in annotations.values():
        for key in ann:
            if key not in ann_types:
                ann_types.append(key)

    rows: list[tuple[str, str, tuple[str, ...]]] = []
    for code, label in sorted(codes.items()):
        ann = annotations.get(code, {})
        rows.append((code, label, tuple(ann.get(t, "") for t in ann_types)))

    import tempfile

    with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False, encoding="utf-8") as fh:
        import csv

        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow(["code", "label_en", *ann_types])
        for code, label, ann_values in rows:
            writer.writerow([code, label, *ann_values])
        tmp_csv = fh.name

    try:
        duckdb.sql(
            f"COPY (SELECT * FROM read_csv('{tmp_csv}', auto_detect=true, all_varchar=true)) "
            f"TO '{target}' (FORMAT PARQUET)"
        )
    finally:
        Path(tmp_csv).unlink(missing_ok=True)


def check_support_path_drift(
    raw_sql: str,
    support_payloads: list[dict[str, Any]],
    sql_label: str = "",
) -> list[str]:
    """
    Scans raw SQL (pre-template-render) for hardcoded path references to
    support datasets that bypass the ``{support.NAME.*}`` placeholder
    contract (mart, clean, mart.TABLE, path, outputs).

    For each declared support entry, verifies that if the support dataset
    slug appears in a path-like string literal, a ``{support.NAME.*}``
    placeholder is also present in the SQL.

    Returns a list of warning messages (empty = no drift detected).
    """
    warnings: list[str] = []

    # Build name -> dataset slug map from resolved payloads
    name_slug_map: dict[str, str] = {}
    for payload in support_payloads:
        slug = payload.get("dataset") or payload.get("name")
        if slug:
            name_slug_map[payload["name"]] = slug

    if not name_slug_map:
        return warnings

    # Find which support names are used via placeholder
    used_via_placeholder: set[str] = set()
    for match in re.finditer(
        r"\{support\.([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_.]*)\}", raw_sql
    ):
        used_via_placeholder.add(match.group(1))

    for name, slug in name_slug_map.items():
        if name in used_via_placeholder:
            continue

        escaped = re.escape(slug)
        for str_match in re.finditer(
            r"""['"](?:[^'"]*""" + escaped + r"""[^'"]*)['"]""",
            raw_sql,
        ):
            text = str_match.group(0)
            # Filtra falsi positivi: deve sembrare un path (slash, backslash, extension)
            if not any(c in text for c in ("/", "\\", ".")):
                continue
            warnings.append(
                f"Support '{slug}' referenced via hardcoded path "
                f"instead of a {{support.{name}.*}} placeholder: "
                f"{'' if not sql_label else sql_label + ' > '}{text[:120]}"
            )

    return warnings


def flatten_support_template_ctx(payloads: list[dict[str, Any]]) -> dict[str, Any]:
    ctx: dict[str, Any] = {}
    for payload in payloads:
        name = payload["name"]
        ctx[f"support.{name}.outputs"] = payload["outputs"]
        ctx[f"support.{name}.mart"] = payload["mart"]
        if payload.get("clean"):
            ctx[f"support.{name}.clean"] = payload["clean"]
        for table, table_path in (payload.get("mart_by_table") or {}).items():
            ctx[f"support.{name}.mart.{table}"] = table_path
        if payload.get("path"):
            ctx[f"support.{name}.path"] = payload["path"]
    return ctx
