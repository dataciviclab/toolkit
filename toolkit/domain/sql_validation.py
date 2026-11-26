from __future__ import annotations

import json
import logging
import re
from typing import Any

import duckdb
from lab_connectors.duckdb import safe_connect

from toolkit.core.constants import RAW_INPUT_VIEW, CLEAN_INPUT_VIEW
from toolkit.clean.run import load_clean_sql
from toolkit.clean.sql_execute import _load_standard_macros
from toolkit.core.config import ensure_dict
from toolkit.core.paths import resolve_sql_path as _resolve_mart_sql_path
from toolkit.core.sql_utils import q_ident
from toolkit.core.support import (
    check_support_path_drift,
    flatten_support_template_ctx,
    resolve_support_payloads,
)
from toolkit.core.template import (
    _strip_sql_comments,
    build_runtime_template_ctx,
    render_template,
)

_logger = logging.getLogger("toolkit.domain.sql_validation")

_QUOTED_IDENTIFIER_RE = re.compile(r'"([^"]+)"')

# DuckDB non usa un messaggio unico per "colonna assente": il testo cambia con
# la forma della query (tabella con alias, alias riusato in SELECT, VALUES).
# Il dry-run deve riconoscerli tutti, altrimenti boccia SQL validi solo perche'
# il placeholder non puo' conoscere le colonne reali della fonte.
_BINDER_MISSING_COLUMN_PATTERNS = (
    re.compile(r'Referenced column "([^"]+)" not found in FROM clause'),
    re.compile(r"Referenced column (\S+) not found in FROM clause and can't find in alias map"),
    re.compile(r'Column "([^"]+)" referenced that exists in the SELECT clause'),
    re.compile(r'Values list "[^"]*" does not have a column named "([^"]+)"'),
)

# Ogni iterazione aggiunge una colonna al placeholder. 25 era troppo basso:
# su clean.sql reali (decine di colonne raw) il loop si esauriva prima di
# arrivare a un binding valido.
_MAX_PLACEHOLDER_COLUMNS = 200

# Il dry-run non conosce i tipi reali della fonte, e un tipo globale scelto a
# priori produce falsi negativi in una delle due direzioni: VARCHAR fa fallire
# sum/confronti numerici, NULL (INTEGER per DuckDB) fa fallire length/trim/LIKE,
# TIMESTAMP fa fallire le stringhe. Si prova in cascata: la prima ipotesi che
# produce un binding valido vince. Costa solo sui SQL che falliscono davvero.
_PLACEHOLDER_TYPE_PASSES: tuple[str | None, ...] = ("VARCHAR", None, "TIMESTAMP")


def _dedupe_preserve_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if not item or item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _declared_map(clean_cfg: dict[str, Any]) -> dict[str, tuple[str, str | None]]:
    """``clean.read.columns`` → {nome_fonte: (nome_output, tipo|None)}."""
    read_columns = (clean_cfg.get("read") or {}).get("columns") or {}
    result: dict[str, tuple[str, str | None]] = {}
    if isinstance(read_columns, dict):
        for source_name, spec in read_columns.items():
            name, _, type_name = str(spec).partition(":")
            name = name.strip()
            if name:
                result[str(source_name)] = (name, type_name.strip() or None)
    return result


def _declared_columns(clean_cfg: dict[str, Any]) -> tuple[list[str], dict[str, str]]:
    """Colonne esposte da ``raw_input`` secondo ``clean.read.columns``.

    Il runtime rinomina sui nomi di output: ``{"col0": "data_ora:VARCHAR"}``
    espone ``data_ora`` (verificato con ``read_raw_to_relation``). Il tipo dopo
    ``:`` e' opzionale.
    """
    names: list[str] = []
    types: dict[str, str] = {}
    for name, type_name in _declared_map(clean_cfg).values():
        names.append(name)
        if type_name:
            types[name] = type_name
    return names, types


def _placeholder_columns(clean_cfg: dict[str, Any], sql: str) -> list[str]:
    columns: list[str] = []
    columns.extend(_declared_columns(clean_cfg)[0])

    # Fallback minimale: raccoglie identifier quoted dal SQL per costruire un
    # raw_input placeholder abbastanza utile nel dry-run. E' deliberatamente
    # approssimativo: puo' includere nomi non-colonna e non copre colonne non
    # quotate se non sono gia' dichiarate in clean.read.columns.
    columns.extend(match.group(1) for match in _QUOTED_IDENTIFIER_RE.finditer(sql))
    return _dedupe_preserve_order(columns)


def _normalize_sql(sql: str) -> str:
    return sql.strip().rstrip(";").strip()


_SUPPORT_PLACEHOLDER_DDL = (
    "CREATE OR REPLACE TABLE __dry_run_clean_preview AS "
    "SELECT NULL::VARCHAR AS __support_placeholder LIMIT 0"
)


def _clean_preview_ddl(clean_sql: str) -> str:
    """DDL del preview; il newline evita che un commento finale mangi la ``)``."""
    return (
        "CREATE OR REPLACE TABLE __dry_run_clean_preview AS\n"
        f"SELECT * FROM (\n{clean_sql}\n) AS q LIMIT 0"
    )


def _is_missing_support_file(exc: Exception, *, dry_run: bool, support_paths: list[str]) -> bool:
    """Il fallimento riguarda un support non ancora materializzato?"""
    if not dry_run:
        return False
    message = str(exc)
    if "No files found that match the pattern" not in message:
        return False
    return bool(support_paths) and any(path in message for path in support_paths)


def _type_passes(
    declared_types: dict[str, str],
) -> tuple[dict[str, str] | str | None, ...]:
    """Ipotesi di tipo in ordine: prima quelle dichiarate, poi globali."""
    passes: list[dict[str, str] | str | None] = []
    if declared_types:
        passes.append(declared_types)
    passes.extend(_PLACEHOLDER_TYPE_PASSES)
    return tuple(passes)


def _create_placeholder_raw_input_with_columns(
    con: duckdb.DuckDBPyConnection,
    columns: list[str],
    *,
    type_name: str | None = None,
    types: dict[str, str] | None = None,
) -> None:
    """Crea ``raw_input`` con le colonne note.

    Il tipo e' un'ipotesi del dry-run, non un fatto. ``types`` mappa colonna →
    tipo quando lo schema e' noto (preview/profilo o ``clean.read.columns``);
    ``type_name`` e' l'ipotesi globale dei pass di fallback; nessuno dei due
    lascia a DuckDB il tipo di default.
    """
    types = types or {}

    def _expr(name: str) -> str:
        resolved = types.get(name) or type_name
        cast = f"::{resolved}" if resolved else ""
        return f"NULL{cast} AS {q_ident(name)}"

    if columns:
        projection = ", ".join(_expr(name) for name in columns)
    else:
        projection = "NULL AS __dry_run_placeholder"
    con.execute(f"CREATE OR REPLACE VIEW {RAW_INPUT_VIEW} AS SELECT {projection} LIMIT 0")


def _extract_missing_binder_column(exc: Exception) -> str | None:
    """Nome della colonna che DuckDB non riesce a risolvere, se riconoscibile.

    I riferimenti possono arrivare qualificati (``r."data"``): per il
    placeholder serve l'ultimo segmento (``data``), non l'alias.
    """
    message = str(exc)
    for pattern in _BINDER_MISSING_COLUMN_PATTERNS:
        match = pattern.search(message)
        if match:
            return match.group(1).rsplit(".", 1)[-1].strip('"')
    return None


def _build_clean_preview(
    cfg,
    *,
    year: int,
    con: duckdb.DuckDBPyConnection,
    support_cfg: list[dict[str, Any]] | None = None,
    dry_run: bool = False,
    raw_schema: dict[str, str] | None = None,
) -> bool:
    """Costruisce ``__dry_run_clean_preview`` dal clean.sql.

    Args:
        raw_schema: schema noto di ``raw_input`` (colonna → tipo DuckDB), per
            esempio dal preview del preflight o dal profilo RAW. Quando c'è,
            il dry-run NON inventa colonne ne' tipi: valida contro lo schema
            reale e riporta gli errori veri.

    Returns:
        ``True`` se il preview e' un segnaposto non rappresentativo (support
        non ancora materializzato): in quel caso le colonne reali del clean
        non sono note e il MART va saltato, non bocciato.
    """
    clean_cfg_ = ensure_dict(cfg.clean)
    clean_sql_path, clean_sql, _ = load_clean_sql(
        clean_cfg_,
        dataset=cfg.dataset,
        year=year,
        root=cfg.root,
        base_dir=cfg.base_dir,
        support_cfg=support_cfg,
    )

    clean_sql = _normalize_sql(clean_sql)
    declared_types = _declared_columns(clean_cfg_)[1]
    columns = _placeholder_columns(clean_cfg_, clean_sql)

    # Pre-calcola i path attesi del support per gestire IOError in dry-run
    support_paths: list[str] = []
    support_payloads_drift: list[dict[str, Any]] = []
    if dry_run and support_cfg:
        try:
            sp_payloads = resolve_support_payloads(
                support_cfg, require_exists=False, smoke=False, root=cfg.root
            )
            support_paths = _all_support_expected_paths(sp_payloads)
            support_payloads_drift = sp_payloads
        except Exception:
            pass

    # Anti-path-drift: verifica che il SQL non referenzi support dataset
    # via path hardcoded invece di {support.NAME.mart}
    if support_payloads_drift:
        raw_sql_text = clean_sql_path.read_text(encoding="utf-8")
        drift_warnings = check_support_path_drift(
            raw_sql_text, support_payloads_drift, sql_label=str(clean_sql_path)
        )
        for w in drift_warnings:
            _logger.warning("PATH DRIFT: %s", w)

    # Schema noto (preview/profilo): nessuna invenzione. Un errore qui e' un
    # errore vero contro lo schema reale della fonte.
    if raw_schema:
        _create_placeholder_raw_input_with_columns(con, list(raw_schema), types=raw_schema)
        try:
            con.execute(_clean_preview_ddl(clean_sql))
            return False
        except Exception as exc:
            if _is_missing_support_file(exc, dry_run=dry_run, support_paths=support_paths):
                con.execute(_SUPPORT_PLACEHOLDER_DDL)
                return True
            raise ValueError(f"CLEAN SQL dry-run failed ({clean_sql_path}): {exc}") from exc

    # Fallback incrementale: se il clean.sql usa colonne raw non quotate e non
    # dichiarate in clean.read.columns, il binder di DuckDB ci dice il nome
    # mancante. Lo aggiungiamo al placeholder e riproviamo, cosi' il dry-run
    # evita falsi positivi banali senza provare a parsare SQL completo.
    # Per ogni ipotesi di tipo si riparte dalle colonne note: se nessun tipo
    # produce un binding valido, l'errore riportato e' quello del primo giro.
    base_columns = list(columns)
    first_error: Exception | None = None
    for pass_spec in _type_passes(declared_types):
        columns = list(base_columns)
        for _ in range(_MAX_PLACEHOLDER_COLUMNS):
            if isinstance(pass_spec, dict):
                _create_placeholder_raw_input_with_columns(con, columns, types=pass_spec)
            else:
                _create_placeholder_raw_input_with_columns(con, columns, type_name=pass_spec)
            try:
                con.execute(_clean_preview_ddl(clean_sql))
                return False
            except Exception as exc:
                if _is_missing_support_file(exc, dry_run=dry_run, support_paths=support_paths):
                    # Preview non rappresentativo: le colonne reali del clean
                    # non sono note, quindi il MART non e' validabile.
                    con.execute(_SUPPORT_PLACEHOLDER_DDL)
                    return True

                missing = _extract_missing_binder_column(exc)
                if missing and missing not in columns:
                    columns.append(missing)
                    continue
                if first_error is None:
                    first_error = exc
                break  # prova il prossimo typing
        else:
            if first_error is None:
                first_error = RuntimeError("exceeded placeholder inference attempts")

    raise ValueError(f"CLEAN SQL dry-run failed ({clean_sql_path}): {first_error}") from first_error


def _all_support_expected_paths(support_payloads: list[dict[str, Any]]) -> list[str]:
    """All support mart output paths attesi (anche se non esistono ancora)."""
    paths: list[str] = []
    for entry in support_payloads:
        paths.extend(entry.get("outputs", []))
    return paths


# ---------------------------------------------------------------------------
# Syntax check (parse-only): deterministico, nessun dato e nessuno schema
# ---------------------------------------------------------------------------

_TEMPLATE_PLACEHOLDER_RE = re.compile(r"\{[A-Za-z0-9_.]+\}")
_TEMPLATE_TOKEN_RE = re.compile(r"\{([A-Za-z0-9_.]+)\}")


def _unknown_support_refs(sql: str, support_names: set[str]) -> list[str]:
    """Riferimenti ``{support.NOME...}`` a support non dichiarati.

    Solo questi: altri placeholder (``{year}``, o ``{2}`` dentro una regex)
    non sono verificabili qui senza un vero parser di stringhe/commenti.
    A runtime un nome sbagliato fa fallire il rendering: anticiparlo in PR e'
    gratis e deterministico.
    """
    unknown: list[str] = []
    for token in _TEMPLATE_TOKEN_RE.findall(sql):
        head, _, rest = token.partition(".")
        if head != "support":
            continue
        name = rest.partition(".")[0]
        if name and name not in support_names:
            unknown.append(f"{{{token}}}")
    return sorted(set(unknown))


def _neutralize_templates(sql: str) -> str:
    """Sostituisce i placeholder ``{...}`` con un identificatore, senza renderli.

    Il parse-only non ha bisogno dei valori reali (year, support path): serve
    solo sapere se la struttura SQL e' valida. Cosi' il check funziona anche in
    PR, dove il support non e' materializzato e il rendering fallirebbe.
    """
    return _TEMPLATE_PLACEHOLDER_RE.sub("__dry_run_template__", sql)


def validate_sql_syntax(cfg, *, layers: list[str]) -> dict[str, Any]:
    """Parse-only di clean.sql/mart.sql: sintassi, senza binding ne' dati.

    Usa ``json_serialize_sql`` di DuckDB, che parsa senza risolvere tabelle o
    colonne. E' deterministico e non produce falsi positivi: e' il check
    adatto alla CI/PR, dove il RAW non esiste ancora.

    Returns:
        ``{"ok": bool, "checked": [...], "errors": [...], "skipped": [...]}``.
        ``skipped`` elenca gli SQL che DuckDB non sa serializzare (es. PIVOT):
        non sono verificabili qui e non bloccano.
    """
    entries: list[tuple[str, str]] = []
    clean_cfg = ensure_dict(cfg.clean)
    if "clean" in layers and clean_cfg.get("sql"):
        entries.append(("clean", str(clean_cfg["sql"])))
    mart_cfg = ensure_dict(cfg.mart)
    if "mart" in layers:
        for table in mart_cfg.get("tables") or []:
            if isinstance(table, dict) and table.get("sql"):
                entries.append((f"mart:{table.get('name')}", str(table["sql"])))

    checked: list[dict[str, Any]] = []
    errors: list[str] = []
    skipped: list[str] = []
    support_names = {
        str(entry.get("name"))
        for entry in ensure_dict(cfg.support) or []
        if isinstance(entry, dict) and entry.get("name")
    }

    with safe_connect() as con:
        for label, sql_ref in entries:
            item: dict[str, Any] = {"label": label, "path": None, "ok": True, "error": None}
            try:
                path = _resolve_mart_sql_path(sql_ref, base_dir=cfg.base_dir)
                item["path"] = str(path)
                sql_text = path.read_text(encoding="utf-8")
                # I commenti possono citare esempi tipo {support.NOME...}:
                # non sono riferimenti reali.
                unknown = _unknown_support_refs(_strip_sql_comments(sql_text), support_names)
                if unknown:
                    item["ok"] = False
                    item["error"] = f"placeholder sconosciuti: {', '.join(unknown)}"
                    errors.append(f"{label}: {item['error']}")
                sql = _neutralize_templates(sql_text)
                raw = con.execute("SELECT json_serialize_sql(?)", [sql]).fetchone()
                payload = json.loads(raw[0]) if raw else {}
                if payload.get("error"):
                    message = str(payload.get("error_message") or payload.get("error_type"))
                    if "Only SELECT statements can be serialized" in message:
                        # PIVOT/statement non serializzabili: non verificabile qui.
                        item["skipped"] = True
                        skipped.append(f"{label}: {message}")
                    else:
                        item["ok"] = False
                        item["error"] = message
                        errors.append(f"{label}: {item['error']}")
            except FileNotFoundError as exc:
                item["ok"] = False
                item["error"] = f"file non trovato: {exc}"
                errors.append(f"{label}: {item['error']}")
            except Exception as exc:
                item["ok"] = False
                item["error"] = f"{type(exc).__name__}: {exc}"
                errors.append(f"{label}: {item['error']}")
            checked.append(item)

    return {"ok": not errors, "checked": checked, "errors": errors, "skipped": skipped}


def schema_from_preview(
    clean_cfg: dict[str, Any],
    columns: list[str] | None,
    col_types: dict[str, str] | None,
) -> dict[str, str] | None:
    """Schema di ``raw_input`` ricavato dal preview di una fonte CSV.

    Applica la stessa rinomina del runtime (``clean.read.columns``) e usa i
    tipi dichiarati in config quando presenti, altrimenti quelli del preview.
    Restituisce ``None`` se il preview non e' utilizzabile per lo schema
    (nessuna colonna o lettura con ``normalize_rows_to_columns``).
    """
    if not columns:
        return None
    if (clean_cfg.get("read") or {}).get("normalize_rows_to_columns"):
        return None

    preview_types = col_types or {}
    declared = _declared_map(clean_cfg)
    if declared:
        schema: dict[str, str] = {}
        for source_name, (name, declared_type) in declared.items():
            schema[name] = declared_type or preview_types.get(source_name) or "VARCHAR"
        return schema
    return {col: preview_types.get(col) or "VARCHAR" for col in columns}


def _validate_mart_sql(
    cfg, *, year: int, con: duckdb.DuckDBPyConnection, dry_run: bool = False
) -> None:
    clean_cfg_ = ensure_dict(cfg.clean)
    mart_cfg_ = ensure_dict(cfg.mart)
    if clean_cfg_.get("sql"):
        con.execute(
            f"CREATE OR REPLACE VIEW {CLEAN_INPUT_VIEW} AS SELECT * FROM __dry_run_clean_preview"
        )

    tables = mart_cfg_.get("tables") or []
    # In dry-run: require_exists=False per non bloccare candidate senza support
    # ancora generati. Se DuckDB fallisce su read_parquet (file non esiste),
    # il dry-run lo registra come avviso ma non blocca.
    support_payloads = resolve_support_payloads(
        ensure_dict(cfg.support), require_exists=not dry_run, root=cfg.root
    )
    template_ctx = build_runtime_template_ctx(
        dataset=cfg.dataset,
        year=year,
        root=cfg.root,
        base_dir=cfg.base_dir,
        support=flatten_support_template_ctx(support_payloads),
    )

    for table in tables:
        name = table.get("name")
        sql_ref = table.get("sql")
        sql_path = _resolve_mart_sql_path(sql_ref, base_dir=cfg.base_dir)
        raw_sql_text = sql_path.read_text(encoding="utf-8")
        # Anti-path-drift: verifica che il SQL non referenzi support dataset
        # via path hardcoded invece di {support.NAME.mart}
        if support_payloads:
            drift_warnings = check_support_path_drift(
                raw_sql_text, support_payloads, sql_label=f"{sql_path} ({name})"
            )
            for w in drift_warnings:
                _logger.warning("PATH DRIFT: %s", w)
        sql = _normalize_sql(render_template(raw_sql_text, template_ctx))
        try:
            # Stessa forma del runtime (mart/run.py: CREATE OR REPLACE TABLE):
            # creare la tabella vuota permette ai mart successivi di leggere
            # quelli precedenti, come in un run reale.
            # Newline prima di ``)``: un SQL che finisce con un commento ``--``
            # commenterebbe la parentesi di chiusura.
            con.execute(
                f"CREATE OR REPLACE TABLE {q_ident(name)} AS\n"
                f"SELECT * FROM (\n{sql}\n) AS q LIMIT 0"
            )
        except Exception as exc:
            err_msg = str(exc)
            # In dry-run, DuckDB puo' fallire su read_parquet se il file support
            # non esiste ancora (placeholder {support.*.mart}). Verifichiamo che
            # l'errore riguardi un path support atteso, non un qualsiasi IO Error.
            if dry_run and "No files found that match the pattern" in err_msg:
                support_paths = _all_support_expected_paths(support_payloads)
                if support_paths and any(sp in err_msg for sp in support_paths):
                    continue
            raise ValueError(f"MART SQL dry-run failed ({name}, {sql_path}): {exc}") from exc


def validate_sql_dry_run(
    cfg,
    *,
    year: int,
    layers: list[str],
    dry_run: bool = False,
    raw_schema: dict[str, str] | None = None,
) -> None:
    """Valida clean/mart SQL senza eseguire la pipeline.

    ``raw_schema`` (colonna → tipo DuckDB) rende la validazione deterministica:
    quando manca, il dry-run costruisce un ``raw_input`` sintetico e inferisce
    le colonne dagli errori del binder.
    """
    if not any(layer in {"clean", "mart"} for layer in layers):
        return

    # Il config check e' responsabilita' del chiamante (``toolkit run`` lo fa
    # prima del dry-run): qui il path del config non e' disponibile.

    with safe_connect() as con:
        _load_standard_macros(con, logger=None)
        preview_is_placeholder = False
        if cfg.clean.sql:
            preview_is_placeholder = _build_clean_preview(
                cfg,
                year=year,
                con=con,
                support_cfg=ensure_dict(cfg.support),
                dry_run=dry_run,
                raw_schema=raw_schema,
            )
        if "mart" in layers:
            if preview_is_placeholder:
                _logger.warning(
                    "MART SQL dry-run saltato: clean preview non rappresentativo "
                    "(support non materializzato)"
                )
            else:
                _validate_mart_sql(cfg, year=year, con=con, dry_run=dry_run)
