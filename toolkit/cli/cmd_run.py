from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import typer

from toolkit.cli.common import dump_cfg_section, iter_selected_years, load_cfg_and_logger
from toolkit.cli.sql_dry_run import validate_sql_dry_run
from toolkit.clean.run import run_clean
from toolkit.clean.validate import run_clean_validation
from toolkit.core.logging import bind_logger, get_logger
from toolkit.core.paths import layer_dataset_dir, layer_year_dir
from toolkit.core.run_context import RunContext
from toolkit.mart.run import run_mart, run_mart_multi_year
from toolkit.mart.validate import run_mart_validation
from toolkit.mcp.schema_ops import review_readiness as _review_readiness
from toolkit.raw.run import run_raw
from toolkit.raw.validate import run_raw_validation





class ValidationGateError(RuntimeError):
    pass


def _validation_runner(layer_name: str):
    if layer_name == "raw":
        return lambda cfg, year, logger: run_raw_validation(cfg.root, cfg.dataset, year, logger)
    if layer_name == "clean":
        return run_clean_validation
    if layer_name == "mart":
        return run_mart_validation
    raise ValueError(f"Unsupported validation layer: {layer_name}")


def _planned_layers(step: str) -> list[str]:
    if step == "all":
        return ["probe", "raw", "clean", "mart"]
    if step == "raw":
        return ["probe", "raw"]
    return [step]


def _resolve_sql_path(cfg, rel_path: str | None) -> Path:
    if not rel_path:
        raise ValueError("Missing SQL path in dataset.yml")
    path = Path(rel_path)
    if path.is_absolute():
        return path
    return Path(cfg.base_dir) / path


def _is_mart_only_cfg(cfg) -> bool:
    return not bool(cfg.clean.get("sql"))


def _validate_execution_plan(cfg, step: str) -> list[str]:
    layers = _planned_layers(step)

    if step == "all" and _is_mart_only_cfg(cfg):
        raise ValueError(
            "run all is not supported for mart-only / compose-only configs; "
            "use: toolkit run mart --config ...",
        )

    if "clean" in layers:
        if _is_mart_only_cfg(cfg):
            raise ValueError(
                "run clean is not supported for mart-only / compose-only configs; "
                "use: toolkit run mart --config ...",
            )
        clean_sql = _resolve_sql_path(cfg, cfg.clean.get("sql"))
        if not clean_sql.exists():
            raise ValueError(
                f"CLEAN SQL file not found: {clean_sql}\n"
                f"This config is not bootstrapped yet.\n"
                f"Run: toolkit init --config <config> --years <year>\n"
                f"Then review sql/clean.sql and run: toolkit run all ..."
            )

    if "mart" in layers:
        tables = cfg.mart.get("tables") or []
        if not isinstance(tables, list) or not tables:
            raise ValueError("mart.tables missing or empty in dataset.yml")
        for table in tables:
            sql_path = _resolve_sql_path(cfg, table.get("sql") if hasattr(table, "get") else getattr(table, "sql", None))
            if not sql_path.exists():
                raise FileNotFoundError(f"MART SQL file not found: {sql_path}")

    return layers


def _layers_from_start(layers: list[str], start_from_layer: str | None) -> list[str]:
    if start_from_layer is None:
        return layers
    if start_from_layer not in layers:
        raise ValueError(f"Cannot start from layer '{start_from_layer}' for planned steps {layers}")
    start_index = layers.index(start_from_layer)
    return layers[start_index:]


def _print_execution_plan(cfg, year: int, layers: list[str], context: RunContext, fail_on_error: bool) -> None:
    typer.echo("Execution Plan")
    typer.echo(f"dataset: {cfg.dataset}")
    typer.echo(f"year: {year}")
    typer.echo("status: DRY_RUN")
    typer.echo(f"run_id: {context.run_id}")
    if context.resumed_from:
        typer.echo(f"resumed_from: {context.resumed_from}")
    typer.echo(f"steps: {', '.join(layers)}")
    typer.echo(f"validation.fail_on_error: {fail_on_error}")
    typer.echo(f"run_record: {context.path}")
    typer.echo("output_dirs:")
    for layer in layers:
        typer.echo(f"  - {layer}: {layer_year_dir(cfg.root, layer, cfg.dataset, year)}")
    typer.echo("")


_PROBE_FORMATS = {
    "text/csv": "CSV",
    "text/tab-separated-values": "TSV",
    "application/json": "JSON",
    "application/xml": "XML",
    "application/zip": "ZIP",
    "application/gzip": "GZ",
    "application/pdf": "PDF",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "XLSX",
    "application/vnd.ms-excel": "XLS",
    "application/vnd.oasis.opendocument.spreadsheet": "ODS",
    "text/html": "HTML",
}


def _probe_fmt(content_type: str | None) -> str:
    """Riduce un content-type a un formato leggibile (es. XLSX, CSV)."""
    if not content_type:
        return "?"
    base = content_type.split(";")[0].strip().lower()
    return _PROBE_FORMATS.get(base, base)


def _run_probe(cfg, year: int, logger) -> None:
    """Passo probe della pipeline: verifica raggiungibilita' fonti remote.

    Riutilizza probe_url_routed dello scout (routing automatico,
    format detection) per output ricco come lo scout CLI.
    Non blocca mai — il vero errore arrivera' da raw.
    Salta local_file, sdmx, sparql (non timeoutano).
    """
    sources = (cfg.raw or {}).get("sources") or []
    if not sources:
        logger.info("PROBE | nessuna fonte remota da verificare")
        return

    from toolkit.scout.http import probe_url_headers

    for src in sources:
        stype = src.get("type", "http_file")
        args = src.get("args", {})
        name = src.get("name") or stype
        url = (args.get("url") or "").replace("{year}", str(year))

        try:
            if stype in ("http_file", "http_post_file"):
                if not url:
                    continue
                probe = probe_url_headers(url, timeout=5)
                sc = probe.get("status_code", 0)
                if 200 <= sc < 400:
                    logger.info("PROBE | %s -> HTTP %s (%s)", name, sc, _probe_fmt(probe.get("content_type")))
                else:
                    logger.warning("PROBE | %s -> HTTP %s %s", name, sc or "ERR", url)

            elif stype == "ckan":
                portal = (args.get("portal_url") or "").replace("{year}", str(year))
                if portal:
                    probe = probe_url_headers(portal, timeout=5)
                    sc = probe.get("status_code", 0)
                    if 200 <= sc < 400:
                        logger.info("PROBE | %s CKAN -> HTTP %s (%s)", name, sc, probe.get("final_url", portal))
                    else:
                        logger.warning("PROBE | %s CKAN -> HTTP %s at %s", name, sc or "ERR", portal)

        except RuntimeError as exc:
            logger.warning("PROBE | %s -> unreachable: %s", name, exc)


def run_year(
    cfg,
    year: int,
    *,
    step: str,
    start_from_layer: str | None = None,
    dry_run: bool = False,
    logger=None,
    resumed_from: str | None = None,
    sample_rows: int | None = None,
    sample_bytes: int | None = None,
) -> RunContext:
    if logger is None:
        logger = get_logger()

    fail_on_error = bool((cfg.validation or {}).get("fail_on_error", True))
    planned_layers = _validate_execution_plan(cfg, step)
    layers_to_run = _layers_from_start(planned_layers, start_from_layer)

    context = RunContext(cfg.dataset, year, root=cfg.root, resumed_from=resumed_from)
    base_logger = bind_logger(
        logger,
        dataset=cfg.dataset,
        year=year,
        run_id=context.run_id,
    )
    log_ctx = (
        f"RUN context | dataset={cfg.dataset}"
        f"{f' source_id={cfg.source_id}' if cfg.source_id else ''}"
        f" year={year} base_dir={cfg.base_dir}"
        f" effective_root={cfg.root}"
        f" root_source={cfg.root_source}"
    )
    base_logger.info(log_ctx)

    if dry_run:
        context.mark_dry_run()
        _print_execution_plan(cfg, year, layers_to_run, context, fail_on_error)
        try:
            validate_sql_dry_run(cfg, year=year, layers=layers_to_run, dry_run=dry_run)
        except Exception as exc:
            context.fail_run(str(exc))
            raise
        if any(layer in {"clean", "mart"} for layer in layers_to_run):
            typer.echo("sql_validation: OK")
            typer.echo("")
        return context

    base_logger.info(f"RUN -> step={step} dataset={cfg.dataset} year={year}")
    run_has_validation_warnings = False
    sample_mode = sample_rows is not None or sample_bytes is not None

    def _execute_layer(layer_name: str, target, *args, **kwargs) -> bool:
        """Esegue un layer e restituisce True se ok, False se fallito.

        Con fail_on_error: false, il fallimento viene loggato ma non
        ri-lanciato. I layer downstream vengono skippati.
        """
        nonlocal run_has_validation_warnings

        layer_logger = bind_logger(base_logger, layer=layer_name)
        context.start_layer(layer_name)
        try:
            metrics = target(*args, logger=layer_logger, **kwargs)
            context.complete_layer(layer_name)
            if isinstance(metrics, dict):
                context.set_layer_metrics(layer_name, **metrics)

            validation_kwargs = {}
            if layer_name in ("clean", "mart"):
                validation_kwargs["sample_mode"] = sample_mode
            summary = _validation_runner(layer_name)(cfg, year, layer_logger, **validation_kwargs)
            context.set_validation(layer_name, summary)
            if not summary.get("passed", False):
                message = f"{layer_name.upper()} validation failed"
                if fail_on_error:
                    raise ValidationGateError(message)
                run_has_validation_warnings = True
            return True
        except Exception as exc:
            context.fail_layer(layer_name, str(exc))
            if fail_on_error:
                context.fail_run(str(exc))
                raise
            run_has_validation_warnings = True
            base_logger.warning(
                "SKIP %s layer for %s (%s) — source unreachable? %s",
                layer_name, cfg.dataset, year, exc,
            )
            return False

    source_id = cfg.source_id

    if "probe" in layers_to_run and not dry_run:
        _run_probe(cfg, year, base_logger)

    if "raw" in layers_to_run:
        if not _execute_layer(
            "raw",
            run_raw,
            cfg.dataset,
            year,
            cfg.root,
            dump_cfg_section(cfg.raw),
            base_dir=cfg.base_dir,
            run_id=context.run_id,
            output_cfg=dump_cfg_section(cfg.output),
            clean_cfg=dump_cfg_section(cfg.clean),
            sample_bytes=sample_bytes,
            source_id=source_id,
        ):
            # RAW fallito: skip layer downstream (clean, mart)
            # per evitare output stale con dati di run precedenti
            layers_to_run = []
    
    if "clean" in layers_to_run and not _is_mart_only_cfg(cfg):
        if not _execute_layer(
            "clean",
            run_clean,
            cfg.dataset,
            year,
            cfg.root,
            dump_cfg_section(cfg.clean),
            base_dir=cfg.base_dir,
            output_cfg=dump_cfg_section(cfg.output),
            sample_rows=sample_rows,
            source_id=source_id,
        ):
            # CLEAN fallito: skip mart per evitare output stale
            layers_to_run = [l for l in layers_to_run if l != "mart"]

    if "mart" in layers_to_run and _has_single_year_mart(cfg):
        _execute_layer(
            "mart",
            run_mart,
            cfg.dataset,
            year,
            cfg.root,
            dump_cfg_section(cfg.mart),
            base_dir=cfg.base_dir,
            clean_cfg=dump_cfg_section(cfg.clean),
            output_cfg=dump_cfg_section(cfg.output),
            support_cfg=dump_cfg_section(cfg.support),
            source_id=source_id,
        )

    context.complete_run(success_with_warnings=run_has_validation_warnings)
    return context


def _has_multi_year_mart(cfg) -> bool:
    """Check if any mart table has an explicit ``years`` field (multi-year)."""
    tables = (cfg.mart or {}).get("tables") or []
    return any(
        isinstance(t, dict) and t.get("years")
        for t in tables
    )


def _has_single_year_mart(cfg) -> bool:
    """Check if any mart table does NOT have an explicit ``years`` field,
    OR if a hierarchy section is defined (runtime-generated aggregation).

    Quando tutte le tabelle sono multi-year (hanno ``years``) e non c'è
    hierarchy, il per-year ``run mart`` non ha nulla da elaborare.
    """
    tables = (cfg.mart or {}).get("tables") or []
    has_single_year = any(
        isinstance(t, dict) and not t.get("years")
        for t in tables
    )
    has_hierarchy = bool((cfg.mart or {}).get("hierarchy"))
    return has_single_year or has_hierarchy


def _maybe_run_multi_year_mart(
    cfg,
    selected_years: list[int],
    *,
    dry_run: bool = False,
    logger=None,
) -> None:
    """Run multi-year MART tables if any table has explicit ``years``.

    Da chiamare una volta per dataset (non per anno), dopo il processing
    dei singoli anni. Sostituisce l'ex comando ``run cross_year``.
    """
    if not _has_multi_year_mart(cfg):
        return
    if logger is None:
        logger = get_logger()

    fail_on_error = bool((cfg.validation or {}).get("fail_on_error", True))

    if dry_run:
        typer.echo("  multi-year mart tables detected")
        multi_year_dir = layer_dataset_dir(cfg.root, "mart", cfg.dataset)
        typer.echo(f"  output_dir: {multi_year_dir}")
        return

    logger.info(
        "MART multi-year | dataset=%s years=%s",
        cfg.dataset,
        ",".join(str(y) for y in selected_years),
    )
    try:
        run_mart_multi_year(
            cfg.dataset,
            selected_years,
            cfg.root,
            dump_cfg_section(cfg.mart),
            logger,
            base_dir=cfg.base_dir,
            output_cfg=dump_cfg_section(cfg.output),
            support_cfg=dump_cfg_section(cfg.support),
            source_id=cfg.source_id,
        )
    except Exception as exc:
        if fail_on_error:
            raise ValidationGateError(f"Multi-year MART failed: {exc}")
        logger.warning("Multi-year MART failed (non-fatal): %s", exc)


# ---- run step wrappers for subcommand registration ----


def run(
    step: str,
    config: str,
    years: str | None = None,
    dry_run: bool = False,
    strict_config: bool = False,
    sample_rows: int | None = None,
    sample_bytes: int | None = None,
):
    """Backward-compatible Python entrypoint used by tests and internal callers."""
    strict_flag = strict_config if isinstance(strict_config, bool) else False
    cfg, logger = load_cfg_and_logger(config, strict_config=strict_flag)
    dry_flag = dry_run if isinstance(dry_run, bool) else False
    years_arg = years if isinstance(years, str) else None
    selected_years = iter_selected_years(cfg, years_arg=years_arg)

    for year in selected_years:
        run_year(cfg, year, step=step, dry_run=dry_flag, logger=logger,
                 sample_rows=sample_rows, sample_bytes=sample_bytes)

    # Multi-year mart: run once per dataset after per-year processing
    if step in ("all", "mart"):
        _maybe_run_multi_year_mart(cfg, selected_years, dry_run=dry_flag, logger=logger)


def _make_step_cmd(step: str):
    """Factory: returns a Typer command wrapping run_year for the given step."""
    _step = step

    def cmd(
        config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
        year: int | None = typer.Option(None, "--year", "-y", help="Single dataset year"),
        years: str | None = typer.Option(None, "--years", help="Comma-separated dataset years"),
        smoke: bool = typer.Option(False, "--smoke", help="Alias per --sample-rows 1000 --sample-bytes 1048576"),
        sample_rows: int | None = typer.Option(None, "--sample-rows", help="Leggi solo N righe in CLEAN (LIMIT N sul output SQL)"),
        sample_bytes: int | None = typer.Option(None, "--sample-bytes", help="Scarica solo N bytes in RAW (HTTP Range header + troncamento locale)"),
        root: str | None = typer.Option(None, "--root", help="Override root output directory (es. DCL_ROOT)"),
        dry_run: bool = typer.Option(False, "--dry-run", help="Print execution plan without executing"),
        strict_config: bool = typer.Option(False, "--strict-config", help="Treat deprecated config forms as errors"),
    ):
        strict_flag = strict_config if isinstance(strict_config, bool) else False
        cfg, logger = load_cfg_and_logger(config, strict_config=strict_flag, root_override=root)

        dry_flag = dry_run if isinstance(dry_run, bool) else False
        years_arg = years if isinstance(years, str) else None
        year_arg = year if isinstance(year, int) else None
        selected_years = iter_selected_years(cfg, year_arg=year_arg, years_arg=years_arg)
        sample_rows_final = 1000 if smoke else sample_rows
        sample_bytes_final = 1048576 if smoke else sample_bytes

        for year in selected_years:
            run_year(cfg, year, step=_step, dry_run=dry_flag, logger=logger,
                     sample_rows=sample_rows_final, sample_bytes=sample_bytes_final)

        # Multi-year mart: run once per dataset after per-year processing
        if _step in ("all", "mart"):
            _maybe_run_multi_year_mart(cfg, selected_years, dry_run=dry_flag, logger=logger)

    cmd.__name__ = f"run_{_step}_cmd"
    cmd.__doc__ = f"Esegue lo step {_step} della pipeline."
    return cmd


run_probe_cmd = _make_step_cmd("probe")
run_raw_cmd = _make_step_cmd("raw")
run_clean_cmd = _make_step_cmd("clean")
run_mart_cmd = _make_step_cmd("mart")
run_all_cmd = _make_step_cmd("all")


def run_init(
    config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
    year: int | None = typer.Option(None, "--year", "-y", help="Single dataset year"),
    years: str | None = typer.Option(None, "--years", help="Comma-separated dataset years"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print plan without executing"),
    strict_config: bool = typer.Option(False, "--strict-config", help="Treat deprecated config forms as errors"),
    # Parametri interni (non CLI) — passati da cmd_init.py
    sample_bytes: int | None = None,
    root_override: str | None = None,
):
    """
    Bootstrap candidate: esegue run raw e scaffold clean.sql se assente.

    Non esegue clean ne mart. Output: raw scaricato, profilo disponibile,
    sql/clean.sql scaffoldato oppure skip esplicito se gia esistente.
    """
    strict_config_flag = strict_config if isinstance(strict_config, bool) else False
    cfg, logger = load_cfg_and_logger(config, strict_config=strict_config_flag, root_override=root_override)
    dry_run_flag = dry_run if isinstance(dry_run, bool) else False
    years_arg = years if isinstance(years, str) else None
    year_arg = year if isinstance(year, int) else None
    selected_years = iter_selected_years(cfg, year_arg=year_arg, years_arg=years_arg)

    if dry_run_flag:
        # Validate the execution plan before showing the dry-run plan.
        # This catches missing sources, invalid paths, etc. early.
        try:
            _validate_execution_plan(cfg, "raw")
        except (ValueError, FileNotFoundError) as exc:
            raise typer.BadParameter(str(exc))

        # Also validate raw.sources specifically (not covered by _validate_execution_plan)
        raw_sources = cfg.raw.get("sources") if cfg.raw else None
        if not raw_sources:
            raise typer.BadParameter("raw.sources missing or empty in dataset.yml")

        typer.echo("Init bootstrap plan")
        typer.echo(f"dataset: {cfg.dataset}")
        typer.echo(f"years: {', '.join(str(y) for y in selected_years)}")
        typer.echo("steps: raw (+ scaffold clean.sql if missing)")
        typer.echo("status: DRY_RUN")
        typer.echo("")
        typer.echo("Nota: clean.sql sara scaffoldato solo se non esiste gia.")
        return

    for year in selected_years:
        logger.info("INIT | dataset=%s year=%s", cfg.dataset, year)

        # Track scaffold state BEFORE run_raw, so we can tell if it was
        # scaffolded by this run vs. pre-existing.
        clean_cfg = cfg.clean or {}
        clean_sql_rel = clean_cfg.get("sql", "sql/clean.sql")
        clean_sql_path = Path(cfg.base_dir) / clean_sql_rel
        scaffold_existed_before = clean_sql_path.exists()

        run_year(cfg, year, step="raw", dry_run=False, logger=logger,
                 sample_bytes=sample_bytes)

        typer.echo(f"[init] Bootstrap completato per {cfg.dataset}/{year}")
        typer.echo("  - raw scaricato")
        profile_dir = layer_year_dir(cfg.root, "raw", cfg.dataset, year) / "_profile"
        profile_exists = (profile_dir / "raw_profile.json").exists()

        scaffolded_now = not scaffold_existed_before and clean_sql_path.exists()
        if scaffold_existed_before:
            if profile_exists:
                typer.echo("  - profiling disponibile")
            typer.echo(f"  - clean.sql gia esistente ({clean_sql_rel}), skip scaffold")
        elif scaffolded_now:
            if profile_exists:
                typer.echo("  - profiling disponibile")
            typer.echo(f"  - clean.sql scaffoldato ({clean_sql_rel})")
        else:
            if not profile_exists:
                raise typer.BadParameter(
                    f"Profilo raw non disponibile per {cfg.dataset}/{year}. "
                    f"Esegui prima: toolkit run raw -c <config> oppure crea clean.sql manualmente."
                )
            typer.echo("  - profiling disponibile")
            typer.echo("  - clean.sql non scaffoldato nonostante il profilo raw sia disponibile")
            typer.echo("    Esegui: toolkit scaffold clean -c <config> oppure verifica i permessi di scrittura")

    typer.echo("")
    typer.echo("Prossimo passo: toolkit run clean -c <config>")


def run_full(
    config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
    years: str | None = typer.Option(None, "--years", help="Comma-separated dataset years"),
    smoke: bool = typer.Option(False, "--smoke", help="Alias per --sample-rows 1000 --sample-bytes 1048576"),
    sample_rows: int | None = typer.Option(None, "--sample-rows", help="Leggi solo N righe in CLEAN (LIMIT N sul output SQL)"),
    sample_bytes: int | None = typer.Option(None, "--sample-bytes", help="Scarica solo N bytes in RAW (HTTP Range header + troncamento locale)"),
    root: str | None = typer.Option(None, "--root", help="Override root output directory (es. DCL_ROOT)"),
    json_output: bool = typer.Option(False, "--json", help="Output JSON report"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print execution plan without executing"),
    strict_config: bool = typer.Option(False, "--strict-config", help="Treat deprecated config forms as errors"),
):
    """Esegue run all + validate all + review-readiness in un unico comando.

    Se il dataset.yml dichiara support: [], i support vengono eseguiti
    automaticamente prima del candidate (run all + validate per ogni anno).
    """
    strict_flag = strict_config if isinstance(strict_config, bool) else False
    cfg, logger = load_cfg_and_logger(config, strict_config=strict_flag, root_override=root)
    years_arg = years if isinstance(years, str) else None
    selected_years = iter_selected_years(cfg, year_arg=None, years_arg=years_arg)
    dry_flag = dry_run if isinstance(dry_run, bool) else False
    sample_rows_final = 1000 if smoke else sample_rows
    sample_bytes_final = 1048576 if smoke else sample_bytes
    sample_mode = sample_rows_final is not None or sample_bytes_final is not None

    results: dict[str, Any] = {
        "config": config,
        "years": selected_years,
        "steps": {},
        "status": "passed",
    }

    # Process support datasets (dichiarati in dataset.yml con support:)
    # Vengono eseguiti prima del candidate cosi' i loro output sono disponibili
    # per le query MART del candidate (placeholder {support.NAME.mart} ecc.).
    # In dry-run i support vengono solo annunciati (non eseguiti): la validazione
    # SQL del candidate usa require_exists=False e non richiede file reali.
    support_entries = cfg.support or []
    if support_entries:
        logger.info(
            "RUN FULL — processing %d support dataset(s) before candidate",
            len(support_entries),
        )
        for entry in support_entries:
            logger.info("Support: %s — %s", entry.name, entry.config)

            if dry_flag:
                typer.echo(f"  [dry-run] support: {entry.name} — years={entry.years}")
                continue

            try:
                support_cfg, support_logger = load_cfg_and_logger(
                    str(entry.config), strict_config=strict_flag
                )
            except Exception as exc:
                logger.error("Support: cannot load config %s: %s", entry.config, exc)
                results["status"] = "failed"
                break  # dipendenza non disponibile, abort

            for sy in entry.years:
                logger.info("Support: running %s year=%s", entry.name, sy)
                try:
                    run_year(support_cfg, sy, step="all", logger=support_logger)
                except Exception as exc:
                    logger.error("Support run failed: %s year=%s — %s", entry.name, sy, exc)
                    results["status"] = "failed"
                    break  # dipendenza fallita, abort

                # Validate all layers
                try:
                    sv_raw = run_raw_validation(support_cfg.root, support_cfg.dataset, sy, support_logger)
                    sv_clean = run_clean_validation(support_cfg, sy, support_logger, sample_mode=sample_mode)
                    sv_mart = run_mart_validation(support_cfg, sy, support_logger, sample_mode=sample_mode)
                    all_support_passed = all(
                        r.get("passed") for r in [sv_raw, sv_clean, sv_mart]
                    )
                    if not all_support_passed:
                        logger.error("Support validation failed: %s year=%s", entry.name, sy)
                        results["status"] = "failed"
                        break  # dipendenza fallita, abort
                except Exception as exc:
                    logger.error("Support validation error: %s year=%s — %s", entry.name, sy, exc)
                    results["status"] = "failed"
                    break  # dipendenza fallita, abort

            if results["status"] == "failed":
                break  # esci dal loop support, vai direttamente al report

    # Se un support e' fallito, non eseguire il candidate (dipendenza assente)
    candidate_blocked = results["status"] == "failed" and not dry_flag

    if not candidate_blocked:
        # Mart-only config (compose): non ha raw/clean, solo mart.
        # run full usa step="mart" e valida solo mart.
        is_mart_only = _is_mart_only_cfg(cfg)
        run_step = "mart" if is_mart_only else "all"

        fail_on_error_flag = bool((cfg.validation or {}).get("fail_on_error", True))

        for year in selected_years:
            logger.info("Run %s — year=%s", run_step, year)
            run_year(cfg, year, step=run_step, dry_run=dry_flag, logger=logger,
                     sample_rows=sample_rows_final, sample_bytes=sample_bytes_final)

            if not dry_flag:
                if is_mart_only:
                    # Validate solo mart (compose non ha raw/clean)
                    val_mart = run_mart_validation(cfg, year, logger, sample_mode=sample_mode)
                    all_passed = bool(val_mart.get("passed"))
                else:
                    # Validate all layers
                    logger.info("Validate all — year=%s", year)
                    val_raw = run_raw_validation(cfg.root, cfg.dataset, year, logger)
                    val_clean = run_clean_validation(cfg, year, logger, sample_mode=sample_mode)
                    val_mart = run_mart_validation(cfg, year, logger, sample_mode=sample_mode)
                    all_passed = all(
                        r.get("passed") for r in [val_raw, val_clean, val_mart]
                    )
                results["steps"][str(year)] = {
                    "run": "ok",
                    "validate": "passed" if all_passed else "failed",
                }
                if not all_passed and fail_on_error_flag:
                    results["status"] = "failed"

                # Review readiness (capture, not print)
                readiness = _review_readiness(config, year or None)
                results["steps"][str(year)]["readiness"] = readiness.get("readiness")
                results["steps"][str(year)]["checks"] = readiness.get("check_count", 0)
                results["steps"][str(year)]["checks_ok"] = readiness.get("ok_count", 0)
                results["steps"][str(year)]["checks_fail"] = readiness.get("fail_count", 0)

        # Multi-year mart: run once per dataset after per-year processing
        if not dry_flag and _has_multi_year_mart(cfg):
            try:
                _maybe_run_multi_year_mart(cfg, selected_years, dry_run=False, logger=logger)
                results["multi_year_mart"] = "ok"
            except Exception as exc:
                results["multi_year_mart"] = f"failed: {exc}"
                if fail_on_error_flag:
                    results["status"] = "failed"

    if json_output:
        typer.echo(json.dumps(results, indent=2, default=str))
    else:
        status = results["status"]
        typer.echo(f"config: {config}")
        typer.echo(f"years: {selected_years}")
        typer.echo(f"status: {status}")
        for y, s in results["steps"].items():
            typer.echo(f"  {y}: run={s['run']} validate={s['validate']} readiness={s.get('readiness','?')} checks={s.get('checks_ok',0)}/{s.get('checks',0)}")

    if results["status"] != "passed":
        raise typer.Exit(code=1)


def _deprecated_cross_year_cmd(
    ctx: typer.Context,
    config: str = typer.Option(..., "--config", "-c"),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    """⚠️ RIMOSSO: sostituito da mart.tables[].years.

    Il layer cross_year non esiste piu'. Definisci la tabella multi-anno
    direttamente in mart.tables[] con il campo 'years', poi esegui:
        toolkit run mart --config <yml>
    """
    typer.echo(
        "ERRORE: 'toolkit run cross_year' non esiste piu'.\n"
        "Il layer cross_year e' stato assorbito in MART.\n\n"
        "Per tabelle multi-anno, aggiungi in dataset.yml:\n"
        "  mart:\n"
        "    tables:\n"
        "      - name: mia_tabella\n"
        "        sql: sql/multi_anno.sql\n"
        "        years: [2022, 2023]\n\n"
        "Poi esegui: toolkit run mart --config <yml>",
        err=True,
    )
    raise typer.Exit(code=1)


def register(app: typer.Typer) -> None:
    run_sub = typer.Typer(no_args_is_help=True, add_completion=False)
    run_sub.command("probe")(run_probe_cmd)
    run_sub.command("raw")(run_raw_cmd)
    run_sub.command("clean")(run_clean_cmd)
    run_sub.command("mart")(run_mart_cmd)
    run_sub.command("all")(run_all_cmd)
    run_sub.command("full")(run_full)
    run_sub.command("cross_year")(_deprecated_cross_year_cmd)
    run_sub.command("cross-year")(_deprecated_cross_year_cmd)  # alias hyphen
    app.add_typer(run_sub, name="run", help="Esegue la pipeline RAW → CLEAN → MART per un dataset.")
