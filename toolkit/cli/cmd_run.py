from __future__ import annotations

import contextlib
import io
import json
from pathlib import Path
from time import perf_counter
from typing import Any

import typer

from toolkit.cli.common import dump_cfg_section, load_cfg_and_logger
from toolkit.core.sql_validation import validate_sql_dry_run
from toolkit.clean.run import run_clean
from toolkit.clean.validate import run_clean_validation
from toolkit.core.logging import bind_logger, get_logger
from toolkit.core.paths import RAW_PROFILE, layer_dataset_dir, layer_year_dir
from toolkit.core.probe import ProbePool, probe_fmt
from toolkit.core.run_context import RunContext
from toolkit.domain.common import iter_selected_years
from toolkit.domain.source_utils import resolve_source as _resolve_source
from toolkit.mart.run import run_mart, run_mart_multi_year
from toolkit.mart.validate import run_mart_validation
from toolkit.raw.run import run_raw
from toolkit.raw.validate import run_raw_validation


class ValidationGateError(RuntimeError):
    """Layer validation failed in strict mode."""

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
        return ["raw", "clean", "mart"]
    if step == "raw":
        return ["raw"]
    return [step]


def _layers_from_start(layers: list[str], start_from_layer: str | None) -> list[str]:
    if start_from_layer is None:
        return layers
    if start_from_layer not in layers:
        raise ValueError(f"Cannot start from layer '{start_from_layer}' for planned steps {layers}")
    start_index = layers.index(start_from_layer)
    return layers[start_index:]


def _validate_execution_plan(cfg, step: str) -> list[str]:
    layers = _planned_layers(step)

    if step == "all" and cfg.is_mart_only:
        raise ValueError(
            "run all is not supported for mart-only / compose-only configs; "
            "use: toolkit run mart --config ...",
        )
    if "clean" in layers:
        if cfg.is_mart_only:
            raise ValueError(
                "run clean is not supported for mart-only / compose-only configs; "
                "use: toolkit run mart --config ...",
            )
        clean_sql = cfg.resolve(cfg.clean.sql)
        if not clean_sql.exists():
            raise ValueError(
                f"CLEAN SQL file not found: {clean_sql}\n"
                f"This config is not bootstrapped yet.\n"
                f"Run: toolkit run raw -c <config> -y <year>\n"
                f"Then review sql/clean.sql and run: toolkit run all ..."
            )
    if "mart" in layers:
        tables = cfg.mart.tables or []
        if not isinstance(tables, list) or not tables:
            raise ValueError("mart.tables missing or empty in dataset.yml")
        for table in tables:
            sql_path = cfg.resolve(table.sql if hasattr(table, "sql") else None)
            if not sql_path.exists():
                raise FileNotFoundError(f"MART SQL file not found: {sql_path}")

    return layers


def _run_probe(cfg, year: int, logger, pool=None) -> None:
    """Passo probe della pipeline: verifica raggiungibilità fonti remote."""
    sources = cfg.raw.sources
    if not sources:
        logger.info("PROBE | nessuna fonte remota da verificare")
        return

    _own_pool = pool is None
    pool = pool or ProbePool(workers=8, circuit_threshold=3)

    try:
        futures = []
        for src in sources:
            resolved = _resolve_source(src, year)
            stype, name, args = resolved["stype"], resolved["name"], resolved["args"]
            if stype in ("http_file", "http_post_file"):
                url = resolved["url"]
                if url:
                    futures.append(pool.submit(url, dataset=name))
            elif stype == "ckan":
                portal_url = args.get("portal_url", "")
                portal = portal_url.replace("{year}", str(year)) if portal_url else ""
                if portal:
                    futures.append(pool.submit(portal, dataset=name))

        for result in pool.as_completed(futures):
            if result.reachable:
                logger.info(
                    "PROBE | %s -> HTTP %s (%s)",
                    result.dataset,
                    result.status_code,
                    probe_fmt(result.content_type),
                )
            elif result.circuit_open:
                logger.warning("PROBE | %s -> CIRCUIT OPEN (%s)", result.dataset, result.error)
            elif result.error:
                logger.warning("PROBE | %s -> unreachable: %s", result.dataset, result.error)
            elif not result.reachable and result.status_code:
                logger.warning(
                    "PROBE | %s -> HTTP %s %s", result.dataset, result.status_code, result.url
                )
    finally:
        if _own_pool:
            pool.close()


def _print_execution_plan(
    cfg, year: int, layers: list[str], context: RunContext, fail_on_error: bool
) -> None:
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
    smoke: bool = False,
) -> RunContext:
    if logger is None:
        logger = get_logger()

    fail_on_error = bool(cfg.validation.fail_on_error)
    validation_mode = cfg.validation.mode
    planned_layers = _validate_execution_plan(cfg, step)
    layers_to_run = _layers_from_start(planned_layers, start_from_layer)

    context = RunContext(cfg.dataset, year, root=cfg.root, resumed_from=resumed_from, smoke=smoke)
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

    # Backward compat: batch --step probe instrada verso _run_probe
    if step == "probe":
        if dry_run:
            context.mark_dry_run()
        else:
            _run_probe(cfg, year, base_logger)
            context.complete_run()
        return context

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

    validations: dict[str, dict[str, Any]] = {}

    def _execute_layer(layer_name: str, target, *args, **kwargs) -> bool:
        """Esegue un layer e restituisce True se ok, False se fallito.

        Con fail_on_error: false, il fallimento viene loggato ma non
        ri-lanciato. I layer downstream vengono skippati.

        validation_mode:
          strict (default): gli errori di validazione bloccano se fail_on_error=True
          warn_only: gli errori di validazione diventano warning, la pipeline continua
        """
        nonlocal run_has_validation_warnings
        nonlocal validations

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
            validations[layer_name] = summary
            if not summary.get("passed", False):
                message = f"{layer_name.upper()} validation failed"
                if validation_mode == "strict" and fail_on_error:
                    raise ValidationGateError(message)
                run_has_validation_warnings = True
                if validation_mode == "warn_only":
                    layer_logger.warning(
                        "VALIDATION %s (%s) — warn_only mode, continuing",
                        layer_name.upper(),
                        message,
                    )
            return True
        except Exception as exc:
            context.fail_layer(layer_name, str(exc))
            if fail_on_error:
                context.fail_run(str(exc))
                raise
            run_has_validation_warnings = True
            base_logger.warning(
                "SKIP %s layer for %s (%s) — source unreachable? %s",
                layer_name,
                cfg.dataset,
                year,
                exc,
            )
            return False

    source_id = cfg.source_id

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

    # Il resolver dei support deve sapere se il campionamento e' attivo
    # (root override in {root}/smoke), non solo se --smoke e' stato usato
    sampling_active = smoke or sample_rows is not None or sample_bytes is not None

    if "clean" in layers_to_run and not cfg.is_mart_only:
        raw_sources = dump_cfg_section(cfg.raw).get("sources", [])
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
            support_cfg=dump_cfg_section(cfg.support),
            smoke=sampling_active,
            raw_sources=raw_sources,
        ):
            # CLEAN fallito: skip mart per evitare output stale
            layers_to_run = [layer for layer in layers_to_run if layer != "mart"]

    if "mart" in layers_to_run and cfg.has_single_year_mart:
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
            smoke=sampling_active,
        )

    context.complete_run(success_with_warnings=run_has_validation_warnings)
    return context


def _maybe_run_multi_year_mart(
    cfg,
    selected_years: list[int],
    *,
    dry_run: bool = False,
    logger=None,
    sampling_active: bool = False,
) -> None:
    """Run multi-year MART tables if any table has explicit ``years``.

    Da chiamare una volta per dataset (non per anno), dopo il processing
    dei singoli anni. Sostituisce l'ex comando ``run cross_year``.

    ``sampling_active`` indica che il root e' stato spostato in ``{root}/smoke``
    per via di ``--smoke``, ``--sample-rows`` o ``--sample-bytes``.
    """
    if not cfg.has_multi_year_mart:
        return
    if logger is None:
        logger = get_logger()

    fail_on_error = bool(cfg.validation.fail_on_error)

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
            smoke=sampling_active,
        )
    except Exception as exc:
        if fail_on_error:
            raise ValidationGateError(f"Multi-year MART failed: {exc}")
        logger.warning("Multi-year MART failed (non-fatal): %s", exc)


# ---- run step wrappers for subcommand registration ----


def run(
    step: str,
    config: str | None = None,
    years: str | None = None,
    dry_run: bool = False,
    sample_rows: int | None = None,
    sample_bytes: int | None = None,
):
    """Backward-compatible Python entrypoint used by tests and internal callers."""
    cfg, logger = load_cfg_and_logger(config)
    dry_flag = dry_run if isinstance(dry_run, bool) else False
    years_arg = years if isinstance(years, str) else None
    selected_years = iter_selected_years(cfg, years_arg=years_arg)

    for year in selected_years:
        run_year(
            cfg,
            year,
            step=step,
            dry_run=dry_flag,
            logger=logger,
            sample_rows=sample_rows,
            sample_bytes=sample_bytes,
        )

    # Multi-year mart: run once per dataset after per-year processing
    if step in ("all", "mart"):
        _maybe_run_multi_year_mart(cfg, selected_years, dry_run=dry_flag, logger=logger)


_STEP_DOCSTRINGS: dict[str, str] = {
    "probe": (
        "Verifica la raggiungibilità delle fonti remote definite in raw.sources.\n\n"
        "Supporta HTTP, CKAN. Salta fonti locali (local_file), SDMX e SPARQL "
        "(non timeoutano). Non blocca mai la pipeline: l'errore reale sarà "
        "rilevato da raw."
    ),
    "raw": (
        "Scarica i file raw dalle fonti e produce il profiling.\n\n"
        "Esegue probe + download dei file raw secondo la configurazione "
        "raw.sources. Produce il profilo raw (encoding, delimiter, colonne, "
        "missingness, mapping_suggestions) per guidare la scrittura di clean.sql."
    ),
    "clean": (
        "Applica le trasformazioni SQL (clean.sql) ai dati raw.\n\n"
        "Legge il parquet raw, esegue clean.sql e produce il layer CLEAN. "
        "Il risultato è un dataset strutturato in formato Parquet, "
        "pronto per le trasformazioni MART."
    ),
    "mart": (
        "Genera le tabelle MART (dataset pubblico) dai dati clean.\n\n"
        "Applica le query SQL definite in mart.tables[] ai dati clean "
        "e produce tabelle denormalizzate in formato Parquet. "
        "Per tabelle multi-anno (con years esplicito), esegue anche "
        "l'aggregazione cross-year automaticamente."
    ),
    "all": (
        "Esegue l'intera pipeline: raw → clean → mart.\n\n"
        "Equivalente a eseguire i tre step in sequenza. "
        "Se il dataset è mart-only (compose), usa solo lo step mart.\n\n"
        "Per dataset semplici (senza support: [] dichiarati in dataset.yml). "
        "Se hai dipendenze tra dataset (support), usa 'toolkit run full'."
    ),
}


def _make_step_cmd(step: str):
    """Factory: returns a Typer command wrapping run_year for the given step."""
    _step = step

    def cmd(
        config: str | None = typer.Option(
            None, "--config", "-c", help="Path o slug del dataset.yml"
        ),
        year: int | None = typer.Option(None, "--year", "-y", help="Single dataset year"),
        years: str | None = typer.Option(None, "--years", help="Comma-separated dataset years"),
        smoke: bool = typer.Option(
            False, "--smoke", help="Alias per --sample-rows 1000 --sample-bytes 1048576"
        ),
        sample_rows: int | None = typer.Option(
            None, "--sample-rows", help="Leggi solo N righe in CLEAN (LIMIT N sul output SQL)"
        ),
        sample_bytes: int | None = typer.Option(
            None,
            "--sample-bytes",
            help="Scarica solo N bytes in RAW (HTTP Range header + troncamento locale)",
        ),
        root: str | None = typer.Option(
            None, "--root", help="Override root output directory (es. DCL_ROOT)"
        ),
        dry_run: bool = typer.Option(
            False, "--dry-run", help="Print execution plan without executing"
        ),
    ):
        dry_flag = dry_run if isinstance(dry_run, bool) else False

        sample_rows_final = 1000 if smoke else sample_rows
        sample_bytes_final = 1048576 if smoke else sample_bytes

        # Qualsiasi forma di campionamento (--smoke, --sample-rows, --sample-bytes)
        # isola l'output in {root}/smoke per evitare contaminazione dei dati reali
        sampling_active = sample_rows_final is not None or sample_bytes_final is not None
        root_override_final = root
        if sampling_active and not root and config is not None:
            _cfg0, _ = load_cfg_and_logger(config)
            root_override_final = str(_cfg0.root / "smoke")

        cfg, logger = load_cfg_and_logger(config, root_override=root_override_final)

        years_arg = years if isinstance(years, str) else None
        year_arg = year if isinstance(year, int) else None
        selected_years = iter_selected_years(cfg, year_arg=year_arg, years_arg=years_arg)

        for year in selected_years:
            run_year(
                cfg,
                year,
                step=_step,
                dry_run=dry_flag,
                logger=logger,
                sample_rows=sample_rows_final,
                sample_bytes=sample_bytes_final,
                smoke=smoke,
            )

        # Multi-year mart: run once per dataset after per-year processing
        if _step in ("all", "mart"):
            _sampling = sample_rows_final is not None or sample_bytes_final is not None
            _maybe_run_multi_year_mart(
                cfg, selected_years, dry_run=dry_flag, logger=logger, sampling_active=_sampling
            )

    cmd.__name__ = f"run_{_step}_cmd"
    cmd.__doc__ = _STEP_DOCSTRINGS.get(_step, f"Esegue lo step {_step} della pipeline.")
    return cmd


def _run_probe_cmd(
    config: str | None = typer.Option(None, "--config", "-c", help="Path or slug to dataset.yml"),
    years: str | None = typer.Option(None, "--years", help="Comma-separated dataset years"),
    json_output: bool = typer.Option(False, "--json", help="Output JSON report"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print plan without executing"),
):
    """Verifica la raggiungibilita' delle fonti remote per un dataset.

    Esegue probe HTTP per fonti http_file e CKAN.
    Salta fonti locali, SDMX e SPARQL (non timeoutano).
    Non blocca mai la pipeline: l'errore reale sara' rilevato da raw.

    Per diagnostica piu' approfondita (quality score CSV, schema, encoding)
    usa: toolkit run preflight
    """
    dry_flag = dry_run if isinstance(dry_run, bool) else False

    cfg, logger = load_cfg_and_logger(config)
    years_arg = years if isinstance(years, str) else None
    selected_years = iter_selected_years(cfg, year_arg=None, years_arg=years_arg)

    if dry_flag:
        typer.echo(f"Probe plan — dataset: {cfg.dataset}")
        typer.echo(f"  years: {', '.join(str(y) for y in selected_years)}")
        sources = cfg.raw.sources or []
        probe_count = sum(
            1
            for s in sources
            for stype in (
                [getattr(s, "type", None)] if hasattr(s, "type") else [s.get("type", "http_file")]
            )
            if stype in ("http_file", "http_post_file", "ckan")
        )
        skip_count = len(sources) - probe_count
        typer.echo(f"  sources: {probe_count} da probe, {skip_count} skip")
        if json_output:
            typer.echo(json.dumps({"status": "DRY_RUN", "dataset": cfg.dataset}))
        return

    for year in selected_years:
        _run_probe(cfg, year, logger)


run_raw_cmd = _make_step_cmd("raw")
run_clean_cmd = _make_step_cmd("clean")
run_mart_cmd = _make_step_cmd("mart")
run_all_cmd = _make_step_cmd("all")


def run_init(
    config: str | None = typer.Option(None, "--config", "-c", help="Path or slug to dataset.yml"),
    year: int | None = typer.Option(None, "--year", "-y", help="Single dataset year"),
    years: str | None = typer.Option(None, "--years", help="Comma-separated dataset years"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print plan without executing"),
    # Parametri interni (non CLI) — passati da cmd_init.py
    sample_bytes: int | None = None,
    root_override: str | None = None,
):
    """
    Bootstrap candidate: esegue run raw e scaffold clean.sql se assente.

    Non esegue clean ne mart. Output: raw scaricato, profilo disponibile,
    sql/clean.sql scaffoldato oppure skip esplicito se gia esistente.
    """
    cfg, logger = load_cfg_and_logger(config, root_override=root_override)
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
        raw_sources = cfg.raw.sources
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
        clean_sql_rel = str(cfg.clean.sql) if cfg.clean.sql else "sql/clean.sql"
        clean_sql_path = Path(cfg.base_dir) / clean_sql_rel
        scaffold_existed_before = clean_sql_path.exists()

        run_year(cfg, year, step="raw", dry_run=False, logger=logger, sample_bytes=sample_bytes)

        typer.echo(f"[init] Bootstrap completato per {cfg.dataset}/{year}")
        typer.echo("  - raw scaricato")
        profile_dir = layer_year_dir(cfg.root, "raw", cfg.dataset, year) / "_profile"
        profile_exists = (profile_dir / RAW_PROFILE).exists()

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
            typer.echo(
                "    Esegui: toolkit run init -c <config> per generare clean.sql,"
                " oppure verifica i permessi di scrittura"
            )

    typer.echo("")
    typer.echo("Prossimo passo: toolkit run clean -c <config>")


# ---------------------------------------------------------------------------
# Batch execution (toolkit run --batch)
# ---------------------------------------------------------------------------


_ALLOWED_BATCH_STEPS = {"probe", "raw", "clean", "mart", "all"}


def _read_config_list(configs_file: Path) -> list[Path]:
    """Read a text file with one dataset.yml path per line."""
    if not configs_file.exists():
        raise FileNotFoundError(f"Config list not found: {configs_file}")

    config_paths: list[Path] = []
    for raw_line in configs_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        config_path = Path(line)
        if not config_path.is_absolute():
            cwd_resolved = (Path.cwd() / config_path).resolve()
            if cwd_resolved.exists():
                config_path = cwd_resolved
            else:
                config_path = (configs_file.parent / config_path).resolve()
        config_paths.append(config_path)

    if not config_paths:
        raise ValueError(f"No config paths found in {configs_file}")

    return config_paths


def _run_batch(
    batch_file: str,
    step: str = "all",
    years: str | None = None,
    smoke: bool = False,
    sample_rows: int | None = None,
    sample_bytes: int | None = None,
    root: str | None = None,
    json_output: bool = False,
    dry_run: bool = False,
) -> None:
    """Esegue piu' config in sequenza e stampa un report aggregato.

    Ogni config viene eseguito via ``_run_pipeline()`` — la stessa funzione
    usata da ``toolkit run``. Il loop batch aggiunge solo la tabella riassuntiva.

    Args:
        batch_file: Path al file con lista di dataset.yml.
        step: Step da eseguire (raw, clean, mart, all). Default ``all``.
        years: Non usato (ogni config usa i propri anni).
        smoke: Alias per --sample-rows 1000 --sample-bytes 1048576.
        sample_rows: Limite righe in CLEAN.
        sample_bytes: Limite bytes in RAW.
        root: Override root output.
        json_output: Output JSON.
        dry_run: Solo plan senza esecuzione.
    """
    config_paths = _read_config_list(Path(batch_file))

    rows: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    for config_path in config_paths:
        started_at = perf_counter()
        dataset_label = config_path.stem

        # In modalita' --json, reindirizza stdout per evitare che i log
        # del toolkit contaminino l'output JSON.
        _stdout_ctx = (
            contextlib.redirect_stdout(io.StringIO()) if json_output else contextlib.nullcontext()
        )

        results: dict[str, Any] = {}
        with _stdout_ctx:
            try:
                results = _run_pipeline(
                    str(config_path),
                    years=years,
                    step=step,
                    smoke=smoke,
                    sample_rows=sample_rows,
                    sample_bytes=sample_bytes,
                    root=root,
                    dry_run=dry_run,
                )
                status = "SUCCESS" if results["status"] == "passed" else "FAILED"
                # In dry-run, un esito ok e' un piano valido, non una run eseguita
                if status == "SUCCESS" and dry_run:
                    status = "DRY_RUN"
                dataset_label = results.get("dataset_name") or dataset_label
            except Exception as exc:
                status = "FAILED"
                failures.append(
                    {"config": str(config_path), "dataset": dataset_label, "error": str(exc)}
                )

            rows.append(
                {
                    "dataset": dataset_label,
                    "config": str(config_path),
                    "years": str(results.get("years")) if status in ("SUCCESS", "DRY_RUN") else "-",
                    "status": status,
                    "duration": f"{perf_counter() - started_at:.3f}s",
                }
            )

    if json_output:
        typer.echo(
            json.dumps(
                {
                    "summary": {
                        "total": len(rows),
                        "passed": sum(1 for r in rows if r["status"] in ("SUCCESS", "DRY_RUN")),
                        "failed": sum(1 for r in rows if r["status"] not in ("SUCCESS", "DRY_RUN")),
                    },
                    "rows": rows,
                    "failures": failures,
                },
                indent=2,
                default=str,
            )
        )
    else:
        headers = ["dataset", "years", "status", "duration", "config"]
        widths = {h: len(h) for h in headers}
        for row in rows:
            for h in headers:
                widths[h] = max(widths[h], len(str(row.get(h, ""))))
        typer.echo("Batch Report")
        typer.echo("  ".join(h.ljust(widths[h]) for h in headers))
        typer.echo("  ".join("-" * widths[h] for h in headers))
        for row in rows:
            typer.echo("  ".join(str(row.get(h, "")).ljust(widths[h]) for h in headers))
        if failures:
            typer.echo("")
            typer.echo("Failures")
            for f in failures:
                typer.echo(f"- config={f['config']} dataset={f['dataset']} error={f['error']}")

    if failures or any(r["status"] not in ("SUCCESS", "DRY_RUN") for r in rows):
        raise typer.Exit(code=1)


# ---------------------------------------------------------------------------
# Pipeline completa — condivisa tra `toolkit run` (default) e `run full`
# ---------------------------------------------------------------------------


def _run_pipeline(
    config: str | None,
    years: str | None = None,
    step: str = "all",
    smoke: bool = False,
    sample_rows: int | None = None,
    sample_bytes: int | None = None,
    root: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Esegue pre-flight + support + raw → clean → mart.

    Versione pura senza output: non stampa nulla, non alza eccezioni CLI.
    Restituisce un dict con esito, step e readiness.

    Args:
        config: Path/slug per dataset.yml, o None per auto-detect CWD.
        years: Anni separati da virgola, o None per tutti quelli configurati.
        step: Step da eseguire (raw, clean, mart, all). Default ``all``.
        smoke: Se True, attiva --sample-rows 1000 --sample-bytes 1048576.
        sample_rows: Limite righe in CLEAN (LIMIT su output SQL).
        sample_bytes: Limite bytes in RAW (HTTP Range + troncamento).
        root: Override root output directory.
        dry_run: Se True, solo plan senza esecuzione.

    Returns:
        Dict con ``status`` (``passed``/``failed``), ``steps`` (per anno),
        ``config``, ``years``, e readiness per ogni layer.
    """
    dry_flag = dry_run if isinstance(dry_run, bool) else False

    sample_rows_final = 1000 if smoke else sample_rows
    sample_bytes_final = 1048576 if smoke else sample_bytes
    sample_mode = sample_rows_final is not None or sample_bytes_final is not None

    sampling_active = sample_rows_final is not None or sample_bytes_final is not None
    root_override_final = root
    if sampling_active and not root and config is not None:
        _cfg0, _ = load_cfg_and_logger(config)
        root_override_final = str(_cfg0.root / "smoke")

    cfg, logger = load_cfg_and_logger(config, root_override=root_override_final)
    years_arg = years if isinstance(years, str) else None
    selected_years = iter_selected_years(cfg, year_arg=None, years_arg=years_arg)

    results: dict[str, Any] = {
        "config": config,
        "dataset_name": cfg.dataset,
        "years": selected_years,
        "steps": {},
        "status": "passed",
    }

    # ── Config check (sempre, zero network I/O) ─────────────────────────
    from toolkit.domain.preflight import run_config_check

    config_check = run_config_check(cfg, config)
    results["config_check"] = config_check
    if not config_check.get("ok", False):
        if dry_flag:
            logger.warning(
                "Config: %s (dry-run, continua)", "; ".join(config_check.get("errors", []))
            )
        else:
            logger.error("Config validation failed — aborting")
            results["status"] = "failed"
            return results
    for warn in config_check.get("warnings", []):
        logger.warning("Config: %s", warn)

    # ── Source probe (solo in esecuzione reale, fa rete) ─────────────────
    if not dry_flag:
        from toolkit.domain.preflight import run_preflight as _run_preflight

        preflight = _run_preflight(config, years_arg=years_arg)
        results["preflight"] = {
            "config_check": preflight["config_check"],
            "sources": preflight["sources"],
        }
        if preflight["status"] != "passed":
            logger.warning(
                "Pre-flight: %d source(s) unreachable (pipeline continua)",
                sum(1 for s in preflight["sources"] if not s["reachable"]),
            )

    # Process support datasets
    support_entries = cfg.support or []
    if support_entries:
        logger.info(
            "RUN — processing %d support dataset(s) before candidate",
            len(support_entries),
        )
        for entry in support_entries:
            logger.info("Support: %s — %s", entry.name, entry.config)

            if dry_flag:
                logger.info("  [dry-run] support: %s — years=%s", entry.name, entry.years)
                continue

            try:
                if sample_mode:
                    _sup0, _ = load_cfg_and_logger(str(entry.config))
                    support_cfg, support_logger = load_cfg_and_logger(
                        str(entry.config),
                        root_override=str(_sup0.root / "smoke"),
                    )
                else:
                    support_cfg, support_logger = load_cfg_and_logger(str(entry.config))
            except Exception as exc:
                logger.error("Support: cannot load config %s: %s", entry.config, exc)
                results["status"] = "failed"
                break

            for sy in entry.years:
                logger.info("Support: running %s year=%s", entry.name, sy)
                try:
                    ctx = run_year(
                        support_cfg,
                        sy,
                        step="all",
                        logger=support_logger,
                        sample_rows=sample_rows_final,
                        sample_bytes=sample_bytes_final,
                        smoke=smoke,
                    )
                except Exception as exc:
                    logger.error("Support run failed: %s year=%s — %s", entry.name, sy, exc)
                    results["status"] = "failed"
                    break

                all_support_passed = all(
                    ctx.validations.get(layer, {}).get("passed", False)
                    for layer in ("raw", "clean", "mart")
                )
                if not all_support_passed:
                    logger.error("Support validation failed: %s year=%s", entry.name, sy)
                    results["status"] = "failed"
                    break

            if results["status"] == "failed":
                break

    # ── Candidate ────────────────────────────────────────────────────────
    candidate_blocked = results["status"] == "failed" and not dry_flag
    if not candidate_blocked:
        is_mart_only = cfg.is_mart_only
        run_step = "mart" if (is_mart_only and step == "all") else step
        fail_on_error_flag = bool(cfg.validation.fail_on_error)

        for year in selected_years:
            results["steps"][str(year)] = {"run": "running", "validate": "running"}
            try:
                logger.info("Run %s — year=%s", run_step, year)
                ctx = run_year(
                    cfg,
                    year,
                    step=run_step,
                    dry_run=dry_flag,
                    logger=logger,
                    sample_rows=sample_rows_final,
                    sample_bytes=sample_bytes_final,
                    smoke=smoke,
                )
            except Exception as exc:
                logger.error("Run %s year=%s fallito: %s", run_step, year, exc)
                results["steps"][str(year)] = {"run": "failed", "validate": "failed"}
                results["status"] = "failed"
                break

            if not dry_flag:
                if is_mart_only:
                    all_passed = bool(ctx.validations.get("mart", {}).get("passed", False))
                else:
                    all_passed = all(
                        ctx.validations.get(layer, {}).get("passed", False)
                        for layer in ("raw", "clean", "mart")
                    )
                results["steps"][str(year)] = {
                    "run": "ok",
                    "validate": "passed" if all_passed else "failed",
                    "validations": ctx.validations,
                }
                if not all_passed and fail_on_error_flag:
                    results["status"] = "failed"

                from toolkit.domain.readiness import review_readiness as _review_readiness

                readiness = _review_readiness(config, year or None)
                results["steps"][str(year)]["readiness"] = readiness.get("readiness")
                results["steps"][str(year)]["checks"] = readiness.get("check_count", 0)
                results["steps"][str(year)]["checks_ok"] = readiness.get("ok_count", 0)
                results["steps"][str(year)]["checks_fail"] = readiness.get("fail_count", 0)
                results["steps"][str(year)]["layers"] = readiness.get("layers", {})

        if results["status"] == "passed" and not dry_flag and cfg.has_multi_year_mart:
            try:
                _maybe_run_multi_year_mart(
                    cfg,
                    selected_years,
                    dry_run=False,
                    logger=logger,
                    sampling_active=sample_mode,
                )
                results["multi_year_mart"] = "ok"
            except Exception as exc:
                results["multi_year_mart"] = f"failed: {exc}"
                if fail_on_error_flag:
                    results["status"] = "failed"

    return results


def _execute_pipeline(
    config: str | None,
    years: str | None,
    smoke: bool,
    sample_rows: int | None,
    sample_bytes: int | None,
    root: str | None,
    json_output: bool,
    dry_run: bool,
) -> None:
    """Wrapper CLI: esegue _run_pipeline e stampa output su stdout."""
    results = _run_pipeline(
        config,
        years,
        step="all",
        smoke=smoke,
        sample_rows=sample_rows,
        sample_bytes=sample_bytes,
        root=root,
        dry_run=dry_run,
    )

    if json_output:
        typer.echo(json.dumps(results, indent=2, default=str))
        if results["status"] != "passed":
            raise typer.Exit(code=1)
        return

    status = results["status"]
    typer.echo(f"config: {config}")
    typer.echo(f"years: {results['years']}")
    typer.echo(f"status: {status}")
    for y, s in results.get("steps", {}).items():
        typer.echo(f"  {y}: run={s['run']} validate={s['validate']}")
        lyrs = s.get("layers", {})
        for lname in ("raw", "clean", "mart"):
            ln = lyrs.get(lname) or {}
            lv = ln.get("validation") or {}
            ok = lv.get("ok")
            qs = lv.get("quality_score")
            icon = "✅" if ok else ("🔴" if ok is False else "·")
            parts = []
            if qs is not None:
                parts.append(f"qs={qs}")
            if lname == "raw":
                pf = ln.get("profile") or {}
                if pf.get("encoding"):
                    parts.append(f"encoding={pf['encoding']}")
                if pf.get("delim"):
                    parts.append(f"delim={pf['delim']}")
                pw = ln.get("profile_warnings") or []
                if pw:
                    parts.append(f"{len(pw)} warning")
            elif lname == "clean":
                rc = lv.get("row_count") or ln.get("row_count")
                cc = lv.get("col_count")
                if rc is not None:
                    parts.append(f"{rc} righe")
                if cc is not None:
                    parts.append(f"{cc} colonne")
                tr = ln.get("transition") or {}
                if tr.get("row_drop_pct") is not None:
                    parts.append(f"raw->clean: {tr['row_drop_pct']}% righe")
            elif lname == "mart":
                tbl = ln.get("tables") or []
                ready = sum(1 for t in tbl if t.get("readable"))
                parts.append(f"{ready}/{len(tbl)} tabelle")
            typer.echo(
                f"       {lname}: {icon}  {'  '.join(parts)}"
                if parts
                else f"       {lname}: {icon}"
            )
        typer.echo(
            f"       readiness: {s.get('readiness', '?')}  ({s.get('checks_ok', 0)}/{s.get('checks', 0)})"
        )

    if results["status"] != "passed":
        raise typer.Exit(code=1)


def run_full(
    config: str | None = None,
    years: str | None = None,
    smoke: bool = False,
    sample_rows: int | None = None,
    sample_bytes: int | None = None,
    root: str | None = None,
    json_output: bool = False,
    dry_run: bool = False,
) -> None:
    """Backward compat: esegue pipeline completa.

    Chiamata programmatica (non CLI) — il comando CLI ``toolkit run full``
    non è più registrato. Usa ``toolkit run``.
    """
    import warnings

    warnings.warn(
        "run_full() è deprecato, usa toolkit.run o _execute_pipeline()",
        DeprecationWarning,
        stacklevel=2,
    )
    _execute_pipeline(config, years, smoke, sample_rows, sample_bytes, root, json_output, dry_run)


def run_preflight_cmd(
    config: str | None = typer.Option(None, "--config", "-c", help="Path or slug to dataset.yml"),
    years: str | None = typer.Option(None, "--years", help="Comma-separated dataset years"),
    json_output: bool = typer.Option(False, "--json", help="Output JSON report"),
):
    """Pre-flight check: valida config, verifica raggiungibilita' fonti,
    e per CSV scarica un preview con quality score PA.

    Non esegue la pipeline — solo diagnostica preventiva.
    """
    from toolkit.domain.preflight import run_preflight

    result = run_preflight(config, years_arg=years)

    if json_output:
        import json as _json

        typer.echo(_json.dumps(result, indent=2, ensure_ascii=False, default=str))
    else:
        status_icon = "✅" if result["status"] == "passed" else "🔴"
        typer.echo(f"{status_icon} Pre-flight: {result['dataset']} ({result['config']})")
        ck = result["config_check"]
        if ck.get("ok"):
            typer.echo("   Config: OK")
        else:
            for e in ck.get("errors", []):
                typer.echo(f"   Config: 🔴 {e}")

        for src in result["sources"]:
            if src["status"] == "skipped":
                continue
            icon = "✅" if src["reachable"] else "🔴"
            parts = [f"{src['name']} ({src['type']})"]
            if src.get("url"):
                parts.append(src["url"])
            parts.append(f"{icon} {src['status']}")
            if src.get("quality_score") is not None:
                parts.append(f"quality={src['quality_score']}/100")
            if src.get("columns"):
                parts.append(f"{len(src['columns'])} colonne")
            typer.echo(f"   {'  '.join(parts)}")

    if result["status"] != "passed":
        raise typer.Exit(code=1)


def register(app: typer.Typer) -> None:
    """Register ``toolkit run`` command group.

    ``toolkit run`` (default) → pipeline completa con preflight + support.
    Subcommands: preflight, raw, clean, mart.
    """
    run_sub = typer.Typer(no_args_is_help=False, add_completion=False)

    @run_sub.callback(invoke_without_command=True)
    def run_default(
        ctx: typer.Context,
        config: str | None = typer.Option(
            None, "--config", "-c", help="Path or slug to dataset.yml"
        ),
        years: str | None = typer.Option(None, "--years", help="Comma-separated dataset years"),
        year: int | None = typer.Option(None, "--year", "-y", help="Single dataset year"),
        batch: str | None = typer.Option(
            None,
            "--batch",
            help="File con lista di dataset.yml (uno per riga) — esegue in sequenza",
        ),
        smoke: bool = typer.Option(
            False, "--smoke", help="Alias per --sample-rows 1000 --sample-bytes 1048576"
        ),
        sample_rows: int | None = typer.Option(
            None, "--sample-rows", help="Leggi solo N righe in CLEAN"
        ),
        sample_bytes: int | None = typer.Option(
            None, "--sample-bytes", help="Scarica solo N bytes in RAW"
        ),
        root: str | None = typer.Option(
            None, "--root", help="Override root output directory (es. DCL_ROOT)"
        ),
        json_output: bool = typer.Option(False, "--json", help="Output JSON report"),
        dry_run: bool = typer.Option(False, "--dry-run", help="Print plan without executing"),
    ):
        """Esegue la pipeline: singolo dataset o batch (--batch)."""
        if ctx.invoked_subcommand is not None:
            return
        if batch:
            _run_batch(
                batch,
                years=years,
                smoke=smoke,
                sample_rows=sample_rows,
                sample_bytes=sample_bytes,
                root=root,
                json_output=json_output,
                dry_run=dry_run,
            )
            return
        # Unifica --year e --years
        years_str = str(year) if year is not None else years
        if year is not None and years is not None:
            typer.echo("Use either --year or --years, not both", err=True)
            raise typer.Exit(code=2)
        try:
            _execute_pipeline(
                config, years_str, smoke, sample_rows, sample_bytes, root, json_output, dry_run
            )
        except FileNotFoundError as exc:
            typer.echo(str(exc), err=True)
            raise typer.Exit(code=1)

    # Subcomandi layer
    run_sub.command("preflight")(run_preflight_cmd)
    run_sub.command("raw")(run_raw_cmd)
    run_sub.command("clean")(run_clean_cmd)
    run_sub.command("mart")(run_mart_cmd)

    app.add_typer(run_sub, name="run", help="Esegue la pipeline per un dataset.")
