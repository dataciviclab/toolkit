"""Orchestrazione support dataset — materializzazione e run.

Separa la parte di orchestration (subprocess, SDMX fetch, run callback)
dalla payload resolution pura in ``toolkit.core.support``. Questo modulo
è chiamato solo dall'orchestrazione CLI (cmd_run), non dai layer runtime
(clean/mart).
"""

from __future__ import annotations

import os
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

from toolkit.core.config import ensure_dict
from toolkit.core.exceptions import DownloadError
from toolkit.core.support import (
    _codelist_output_path,
    _entry_type,
    resolve_support_payloads,
)


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
    if stype == "external":
        raise ValueError(f"support external '{name}' esiste già su GCS, nessuna materializzazione")
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

    codes = cast(dict[str, str], result.get("codes") or {})
    annotations = cast(dict[str, dict[str, str]], result.get("annotations") or {})

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


def resolve_and_run_support(
    cfg: Any,
    *,
    logger: Any,
    dry_run: bool,
    smoke: bool,
    sample_rows: int | None,
    sample_bytes: int | None,
    sample_mode: bool,
    refresh: bool,
    run_year_fn: Callable[..., Any],
    load_cfg_fn: Callable[..., tuple[Any, Any]],
) -> str:
    """Process all support entries: resolve, run, validate.

    Extracted from ``_run_pipeline`` to decouple support orchestration from
    the CLI pipeline. Returns ``"passed"`` or ``"failed"``.

    Args:
        cfg: PipelineConfig for the candidate dataset.
        logger: Logger instance.
        dry_run: If True, only log support entries without executing.
        smoke: If True, support runs use smoke root override.
        sample_rows: Row limit for clean layer.
        sample_bytes: Byte limit for raw layer.
        sample_mode: True if any sampling is active.
        refresh: If True, re-run support even if outputs exist.
        run_year_fn: Callback ``(cfg, year, step, logger, ...) -> RunContext``.
        load_cfg_fn: Callback ``(config_path, root_override=...) -> (cfg, logger)``.
    """
    support_entries = cfg.support or []
    if not support_entries:
        return "passed"

    logger.info(
        "RUN — processing %d support dataset(s) before candidate",
        len(support_entries),
    )
    for entry in support_entries:
        entry_dict = ensure_dict(entry)
        stype = str(entry_dict.get("type") or "dataset")

        if dry_run:
            logger.info(
                "  [dry-run] support: %s — type=%s years=%s",
                entry.name,
                stype,
                getattr(entry, "years", []),
            )
            continue

        try:
            payloads = resolve_support_payloads(
                [entry_dict],
                require_exists=False,
                smoke=smoke,
                root=cfg.root,
            )
            payload = payloads[0]
        except Exception as exc:
            logger.error("Support: cannot resolve %s: %s", entry.name, exc)
            return "failed"

        if payload["all_outputs_exist"] and not refresh:
            logger.info("  reuse support %s (output presenti)", entry.name)
            continue

        if stype == "dataset":
            try:
                if sample_mode:
                    _sup0, _ = load_cfg_fn(str(entry.config))
                    support_cfg, support_logger = load_cfg_fn(
                        str(entry.config),
                        root_override=str(_sup0.root / "smoke"),
                    )
                else:
                    support_cfg, support_logger = load_cfg_fn(str(entry.config))
            except Exception as exc:
                logger.error("Support: cannot load config %s: %s", entry.config, exc)
                return "failed"

            for sy in entry.years:
                logger.info("Support: running %s year=%s", entry.name, sy)
                try:
                    ctx = run_year_fn(
                        support_cfg,
                        sy,
                        step="all",
                        logger=support_logger,
                        sample_rows=sample_rows,
                        sample_bytes=sample_bytes,
                        smoke=smoke,
                    )
                except Exception as exc:
                    logger.error("Support run failed: %s year=%s — %s", entry.name, sy, exc)
                    return "failed"

                all_support_passed = all(
                    ctx.validations.get(layer, {}).get("passed", False)
                    for layer in ("raw", "clean", "mart")
                )
                if not all_support_passed:
                    logger.error("Support validation failed: %s year=%s", entry.name, sy)
                    return "failed"
        elif stype == "external":
            # external: file already exists on GCS, no materialization needed
            logger.info("Support: external %s (GCS, no-op)", entry.name)
        else:
            # codelist / file: materializza (fetch / command) — output verificato mancante
            logger.info("Support: materializing %s (%s)", entry.name, stype)
            try:
                materialize_support(entry_dict, root=cfg.root, smoke=smoke)
            except Exception as exc:
                logger.error("Support materialization failed: %s — %s", entry.name, exc)
                return "failed"
            try:
                payloads = resolve_support_payloads(
                    [entry_dict], require_exists=False, smoke=smoke, root=cfg.root
                )
                if not payloads[0]["all_outputs_exist"]:
                    logger.error("Support materialization produced no output: %s", entry.name)
                    return "failed"
            except Exception as exc:
                logger.error("Support: cannot re-resolve %s: %s", entry.name, exc)
                return "failed"

    return "passed"
