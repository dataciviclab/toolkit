"""RAW profiling entry point.

Orchestrates encoding/delimiter sniffing, DuckDB-based column profiling,
and mapping suggestion generation. Internal sniffing logic lives in
``_sniff_encoding``, ``_sniff_delimiter``, and ``_column_profile``.
"""

from __future__ import annotations

import csv
import re
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, cast

import duckdb
from lab_connectors.duckdb import safe_connect

from toolkit.core.constants import RAW_INPUT_VIEW
from toolkit.core.csv_read import (
    normalize_read_cfg,
)
from toolkit.core.duckdb_read import ReadInfo, read_raw_to_relation
from toolkit.core.input_file import RawInputFile
from toolkit.core.logging import get_logger
from toolkit.core.paths import RAW_PROFILE, RAW_PROFILE_DIR
from toolkit.core.io import read_json_or_none, write_json_atomic
from toolkit.profile._sniff_encoding import is_binary_file as _is_binary_file, sniff_encoding
from toolkit.profile._sniff_delimiter import sniff_decimal, sniff_delim, suggest_skip
from toolkit.profile._column_profile import _build_mapping_suggestions, _normalize_colname


# ---------------------------------------------------------------------------
# Date format detection (moved from scaffold/clean.py to fix dependency inversion)
# ---------------------------------------------------------------------------

_DATE_FORMAT_GROUPS: list[tuple[str, ...]] = [
    ("%d/%m/%Y", "%m/%d/%Y"),  # slash 4-digit
    ("%d-%m-%Y", "%m-%d-%Y"),  # dash 4-digit
    ("%d/%m/%y", "%m/%d/%y"),  # slash 2-digit
    ("%d-%m-%y", "%m-%d-%y"),  # dash 2-digit
    ("%Y/%m/%d",),  # ISO con slash
]


def _try_strptime(value: str, fmt: str) -> bool:
    """Try to parse a date string with the given strptime format."""
    import datetime

    try:
        datetime.datetime.strptime(value, fmt)
        return True
    except ValueError:
        return False


def suggest_dateformat(profile: dict[str, Any]) -> str | None:
    """Detect non-ISO date format from raw date values in the profile.

    Uses ``date_raw_values`` (extracted from raw CSV *before* DuckDB
    converts dates) and ``datetime.strptime`` for validation.

    Each column picks its best format (>=60% of its non-empty values).
    Only suggests a ``dateformat`` if EVERY column picks the SAME format.
    """
    date_raw = profile.get("date_raw_values", {})
    if not date_raw:
        return None

    col_formats: dict[str, str] = {}
    for col, values in date_raw.items():
        non_empty = [v for v in values if v]
        total = len(non_empty)
        if total == 0:
            continue

        best_fmt: str | None = None
        best_score = 0

        for fmt_group in _DATE_FORMAT_GROUPS:
            if len(fmt_group) == 1:
                fmt = fmt_group[0]
                count = sum(1 for v in non_empty if _try_strptime(v, fmt))
                if count > best_score:
                    best_score = count
                    best_fmt = fmt
            else:
                dmy_fmt, mdy_fmt = fmt_group
                dmy_count = sum(1 for v in non_empty if _try_strptime(v, dmy_fmt))
                mdy_count = sum(1 for v in non_empty if _try_strptime(v, mdy_fmt))
                if dmy_count > mdy_count and dmy_count > best_score:
                    best_score = dmy_count
                    best_fmt = dmy_fmt
                elif mdy_count > dmy_count and mdy_count > best_score:
                    best_score = mdy_count
                    best_fmt = mdy_fmt

        if best_fmt is not None and best_score >= total * 0.6:
            col_formats[col] = best_fmt

    if not col_formats:
        return None

    unique = set(col_formats.values())
    if len(unique) == 1:
        return unique.pop()

    return None


def _safe_mkdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _raw_files(raw_dir: Path) -> list[Path]:
    return sorted([p for p in raw_dir.glob("*") if p.is_file()])


def _preview_columns(header_line: str | None, delim: str | None) -> list[str]:
    if not header_line or not delim:
        return []
    parts = [segment.strip() for segment in header_line.split(delim)]
    return [_normalize_colname(part) for part in parts if part.strip()]


def sniff_source_file(filepath: Path) -> dict[str, Any]:
    """Pure source sniffing: encoding, delimiter, decimal, skip, header.

    Does not read the file with DuckDB — only inspects raw bytes/text to
    produce suggested parse parameters and the header line at the skip offset.

    Returns
    -------
    dict with keys:
        - file_used (str): filename
        - encoding_suggested (str): detected encoding
        - delim_suggested (str): detected delimiter
        - decimal_suggested (str): detected decimal separator
        - skip_suggested (int): suggested skip rows
        - header_line (str | None): header text at skip offset
        - true_header_line (str | None): header text at line 0 (for mismatch detection)
        - columns_preview (list[str]): normalised column names from header_line
        - warnings (list[str]): issues detected during sniffing
        - is_binary_file (str | None): 'xlsx' / 'xls' if binary format detected
    """
    # Binary files (XLSX/XLS) cannot be decoded as text — return early.
    binary_fmt = _is_binary_file(filepath)
    if binary_fmt:
        return {
            "file_used": filepath.name,
            "encoding_suggested": None,
            "delim_suggested": None,
            "decimal_suggested": None,
            "skip_suggested": 0,
            "header_line": None,
            "true_header_line": None,
            "columns_preview": [],
            "warnings": [f"binary_file_detected: {binary_fmt}"],
            "is_binary_file": binary_fmt,
        }

    enc, txt = sniff_encoding(filepath)
    delim = sniff_delim(txt)
    # Il delim è noto: sniff_decimal field-aware evita falsi positivi tipo
    # "2008,88" (anno + valore separati dal delim CSV) come decimale con virgola.
    dec = sniff_decimal(txt, delim)
    skip = suggest_skip(txt, delim)
    warnings: list[str] = []

    if skip:
        warnings.append(
            "header_preamble_detected: first non-empty line looks like a title row, consider skip: 1"
        )

    header_line: str | None = None
    true_header_line: str | None = None

    # Read header at skip offset (where DuckDB would start reading data).
    # This preserves backward-compatible header_line for suggested_read.
    try:
        with filepath.open("r", encoding=enc, errors="replace") as f:
            for _ in range(skip):
                f.readline()
            header_line = f.readline().rstrip("\n\r")
    except Exception as exc:
        warnings.append(f"header_read_failed: {type(exc).__name__}: {exc}")

    # Also read the true file header from line 0 (independent of skip).
    # This is the ground-truth header row used for mismatch detection:
    # if header at line 0 has fewer tokens than data columns returned by
    # DESCRIBE, the file has extra cols in data rows (IRPEF comunale pattern).
    try:
        with filepath.open("r", encoding=enc, errors="replace") as f:
            true_header_line = f.readline().rstrip("\n\r")
    except Exception:
        pass  # already logged above; degrade gracefully

    return {
        "file_used": filepath.name,
        "encoding_suggested": enc,
        "delim_suggested": delim,
        "decimal_suggested": dec,
        "skip_suggested": skip,
        "header_line": header_line,
        "true_header_line": true_header_line,
        "columns_preview": _preview_columns(header_line, delim),
        "warnings": warnings,
        "is_binary_file": None,
    }


def build_suggested_read_cfg(
    profile: "RawProfile | dict[str, Any]",
    read_cfg: dict[str, Any] | None = None,
) -> dict[str, Any]:
    data = profile if isinstance(profile, dict) else asdict(profile)
    cfg: dict[str, Any] = {}

    source_cfg = dict(read_cfg or {})
    for key in (
        "delim",
        "header",
        "encoding",
        "decimal",
        "skip",
        "auto_detect",
        "quote",
        "escape",
        "comment",
        "ignore_errors",
        "strict_mode",
        "null_padding",
        "nullstr",
        "columns",
        "trim_whitespace",
        "sample_size",
        "dateformat",
    ):
        if source_cfg.get(key) is not None:
            cfg[key] = source_cfg[key]

    if cfg.get("delim") is None and data.get("delim_suggested") is not None:
        cfg["delim"] = data["delim_suggested"]
    if cfg.get("decimal") is None and data.get("decimal_suggested") is not None:
        cfg["decimal"] = data["decimal_suggested"]
    if cfg.get("encoding") is None and data.get("encoding_suggested") is not None:
        cfg["encoding"] = data["encoding_suggested"]
    if cfg.get("skip") is None and int(data.get("skip_suggested") or 0) > 0:
        cfg["skip"] = int(data["skip_suggested"])

    # Propaga dateformat se non gia' in source_cfg
    if cfg.get("dateformat") is None and data.get("date_raw_values"):
        fmt = suggest_dateformat(data)
        if fmt:
            cfg["dateformat"] = fmt

    cfg.setdefault("header", True)

    # NOTA: `robust_read_suggested` NON viene tradotto in ignore_errors /
    # null_padding / auto_detect. Il layer CLEAN filtra comunque quelle chiavi
    # (``filter_suggested_format_keys``), perché attivare ``ignore_errors`` da
    # solo maskererebbe righe scartate e corruzione di encoding. Il flag resta
    # nel profilo come segnale per l'operatore (e per la preview).

    return normalize_read_cfg(cfg)


def _pick_data_file(files: list[Path]) -> Path:
    preferred = [p for p in files if p.suffix.lower() in {".csv", ".tsv", ".txt", ".php", ".gz"}]
    if preferred:
        return preferred[0]
    for p in files:
        if p.suffix.lower() not in {".json", ".md", ".yml", ".yaml"}:
            return p
    return files[0]


# Mode in cui il CLEAN legge un insieme di file, non il solo primary del manifest.
_MULTI_FILE_MODES = {"all", "explicit", "largest"}


def _raw_root(raw_dir: Path) -> Path | None:
    """Root del dataset, se ``raw_dir`` ha il layout ``<root>/data/raw/<ds>/<year>``.

    Fallback per i chiamanti che non passano ``root`` esplicito (test, preview).
    Con un layout diverso restituisce ``None`` e la selezione ricade sulla
    directory.
    """
    if raw_dir.parent.parent.name != "raw" or raw_dir.parent.parent.parent.name != "data":
        return None
    return raw_dir.parents[3]


def _select_profile_files(
    raw_dir: Path,
    primary_file: Path,
    read_cfg: dict[str, Any] | None,
    logger,
    *,
    source_files: list[Path] | None = None,
    dataset: str | None = None,
    year: int | None = None,
    root: str | Path | None = None,
) -> list[Path]:
    """I file che il CLEAN leggerà davvero.

    ``mode: latest``/``fallback`` → il ``primary_output_file`` del manifest
    (comportamento storico, una sola relazione). ``all``/``explicit``/
    ``largest`` → la selezione del clean, riusando
    ``core.input_selection.select_raw_input``: stessi file, stesso ``include``.

    Precedenza dei candidati:

    1. ``source_files`` — i file scritti da *questo* run RAW. È la verità
       quando il profilo gira dentro ``run_raw``: il ``metadata.json`` sul
       disco è ancora quello del run precedente, quindi leggerlo darebbe la
       relazione vecchia.
    2. le entry del manifest (``_metadata_candidates``), come fa il clean, per
       i chiamanti diretti che profilano una directory esistente.
    3. la lista della directory, se non c'è manifest utilizzabile.

    Se la selezione non trova nulla si ricade sul primary: meglio un profilo
    parziale e dichiarato che nessun profilo.
    """
    cfg = read_cfg or {}
    mode = str(cfg.get("mode") or "fallback")
    if mode not in _MULTI_FILE_MODES:
        return [primary_file]

    from toolkit.core.input_selection import is_usable_input_file, select_raw_input

    prof_logger = logger if logger is not None else get_logger("toolkit.profile")
    candidates = None
    explicit_root: str | Path | None = root
    if source_files:
        candidates = [p for p in source_files if is_usable_input_file(p)]
    elif explicit_root is None:
        explicit_root = _raw_root(raw_dir)
    try:
        use_manifest = explicit_root is not None and not candidates
        selected = select_raw_input(
            raw_dir,
            prof_logger,
            mode=mode,
            root=str(explicit_root) if use_manifest else None,
            dataset=dataset if use_manifest else None,
            year=year if use_manifest else None,
            glob=str(cfg.get("glob") or "*"),
            prefer_from_raw_run=bool(cfg.get("prefer_from_raw_run", True)),
            candidates=candidates,
            include=cfg.get("include"),
            allow_ambiguous=bool(cfg.get("allow_ambiguous", False)),
        )
    except Exception as exc:
        prof_logger.warning(
            "profile: selezione RAW (mode=%s) fallita, profilo solo su %s: %s: %s",
            mode,
            primary_file.name,
            type(exc).__name__,
            exc,
        )
        return [primary_file]
    return selected or [primary_file]


def _effective_profile_read_cfg(
    read_cfg: dict[str, Any] | None,
    *,
    encoding: str,
    delim: str | None,
    decimal: str | None,
    skip: int,
) -> dict[str, Any]:
    effective_read_cfg = dict(read_cfg) if isinstance(read_cfg, dict) else {}
    effective_read_cfg.pop("source", None)
    # `None` significa "non impostato": la config tipizzata emette tutte le
    # chiavi, quindi il controllo deve essere sul valore, non sulla presenza.
    if effective_read_cfg.get("delim") is None and effective_read_cfg.get("sep") is None and delim:
        effective_read_cfg["delim"] = delim
    elif effective_read_cfg.get("delim") is None and effective_read_cfg.get("sep"):
        effective_read_cfg["delim"] = effective_read_cfg["sep"]
    if effective_read_cfg.get("encoding") is None and encoding:
        effective_read_cfg["encoding"] = encoding
    if effective_read_cfg.get("decimal") is None and decimal:
        effective_read_cfg["decimal"] = decimal
    if effective_read_cfg.get("skip") is None and skip:
        effective_read_cfg["skip"] = skip
    effective_read_cfg.setdefault("header", True)
    return effective_read_cfg


def create_profile_relation(
    con: duckdb.DuckDBPyConnection,
    files: Path | Sequence[Path],
    read_cfg: dict[str, Any],
    *,
    read_mode: str = "fallback",
    logger=None,
) -> ReadInfo:
    """Crea ``RAW_INPUT_VIEW`` con la STESSA pipeline che usa il layer clean.

    Il profilo non deve avere un proprio parser: se lo avesse, "come leggerà il
    clean" sarebbe una promessa non verificata. Delega a
    ``core.duckdb_read.read_raw_to_relation``, che gestisce formato compatto
    ``clean_name:DUCKDB_TYPE``, ``normalize_rows_to_columns``,
    ``align_by_header``, multi-file, Excel e parquet.

    Returns:
        ``ReadInfo`` — ``info.source`` vale ``strict`` se la config dichiarata
        è bastata, ``robust`` se è servito il preset robusto.
    """
    cfg = dict(read_cfg or {})
    # `source` è una chiave di selezione del layer clean, non un'opzione CSV.
    cfg.pop("source", None)
    # `None` = "non impostato" (la config tipizzata emette tutte le chiavi).
    # Il clean riceve un dict già filtrato da `merge_read_cfg`; il profilo no,
    # e con le chiavi a None il preset robusto non avrebbe effetto.
    cfg = {key: value for key, value in cfg.items() if value is not None}
    selected = [files] if isinstance(files, Path) else list(files)
    return read_raw_to_relation(
        con,
        [RawInputFile(path=path.resolve()) for path in selected],
        cfg,
        read_mode,
        logger if logger is not None else get_logger("toolkit.profile"),
    )


def _describe_columns(con: duckdb.DuckDBPyConnection) -> tuple[list[str], list[str], list[str]]:
    cols = con.execute(f"DESCRIBE {RAW_INPUT_VIEW}").fetchall()
    columns_raw = [str(r[0]) for r in cols]
    columns_norm = [_normalize_colname(c) for c in columns_raw]
    duckdb_types = [str(r[1]) for r in cols]  # DuckDB-inferred types
    return columns_raw, columns_norm, duckdb_types


def _sample_profile_rows(
    con: duckdb.DuckDBPyConnection,
    columns_raw: list[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, float]]:
    """Sample rows, missingness_top (top 25), and full null_counts for ALL columns.

    Returns:
        (sample_rows, missingness_top, null_counts)

        - ``null_count`` e' una mappa completa ``{col_name: missing_pct}`` per TUTTE
          le colonne (non troncata). Usata dall'inferenza NOT NULL della validazione.
    """
    df = con.execute(f"SELECT * FROM {RAW_INPUT_VIEW} LIMIT 50").fetchdf()
    # pandas-stubs: to_dict(orient='records') restituisce dict[Hashable, Any];
    # le chiavi sono sempre str (nomi colonna) → cast esplicito.
    sample_rows = cast(list[dict[str, Any]], df.to_dict(orient="records"))

    # Single-pass missingness: one query with all columns in CASE expressions.
    # Avoids O(N) separate queries (N = columns), which was slow for 50+ column files.
    missingness_top: list[dict[str, Any]] = []
    null_counts: dict[str, float] = {}
    cols_to_profile = columns_raw[:200]
    if cols_to_profile:
        case_exprs: list[str] = []
        for c in cols_to_profile:
            col_ref = f'"{c}"'
            case_exprs.append(
                f"SUM(CASE WHEN {col_ref} IS NULL OR TRIM(CAST({col_ref} AS VARCHAR)) = '' THEN 1 ELSE 0 END) AS \"{c}__missing\""
            )

        row = con.execute(
            f"SELECT COUNT(*) AS n_total, {', '.join(case_exprs)} FROM {RAW_INPUT_VIEW}"
        ).fetchone()
        if row is not None:
            n_total = int(row[0])
            if n_total > 0:
                for i, c in enumerate(cols_to_profile):
                    nmiss = int(row[i + 1])
                    pct = float(nmiss) / float(n_total) * 100.0
                    null_counts[c] = pct
                    if nmiss > 0:
                        missingness_top.append({"column": c, "missing_pct": pct})

                missingness_top = sorted(missingness_top, key=lambda x: -x["missing_pct"])[:25]

    return sample_rows, missingness_top, null_counts


def profile_excel(
    files: Path | Sequence[Path],
    read_cfg: dict[str, Any] | None = None,
    *,
    read_mode: str = "fallback",
    logger=None,
) -> dict[str, Any]:
    """Profile an Excel file with the shared CLEAN read pipeline.

    Thin wrapper over ``profile_with_read_cfg``: Excel non ha un lettore
    dedicato al profilo. Il runtime clean usa ``_load_excel_frame`` via
    ``read_raw_to_relation``, e il profilo profila la stessa view — un
    secondo parser avrebbe la stessa deriva già pagata sui CSV.
    """
    first = files if isinstance(files, Path) else list(files)[0]
    return profile_with_read_cfg(
        files,
        sniff_source_file(first),
        dict(read_cfg or {}),
        read_mode=read_mode,
        logger=logger,
    )


def profile_with_read_cfg(
    files: Path | Sequence[Path],
    sniff_hints: dict[str, Any],
    effective_read_cfg: dict[str, Any],
    *,
    read_mode: str = "fallback",
    logger=None,
) -> dict[str, Any]:
    """Profile the RAW relation the CLEAN layer will read.

    This is the "runtime" half of profiling: it builds ``RAW_INPUT_VIEW`` with
    ``core.duckdb_read.read_raw_to_relation`` — the exact function the clean
    layer uses — and profiles that view. Non esiste un parser "del profilo":
    se esistesse, "legge come il clean" sarebbe una promessa non verificata
    (era la causa dei profili vuoti su ``columns`` compatto e su
    ``normalize_rows_to_columns``).

    Parameters
    ----------
    files:
        Data file, oppure lista di file quando il CLEAN ne legge più di uno
        (``mode: all|explicit|largest``). Con più file la view è quella di
        ``read_csv`` con ``union_by_name`` — la stessa del clean.
    sniff_hints:
        Output of ``sniff_source_file`` — provides ``true_header_line`` for
        mismatch diagnostics and carries the sniff-level warnings.
    effective_read_cfg:
        Read configuration (encoding, delim, skip, columns, ...) to use.
    read_mode:
        Clean read mode: ``strict`` | ``fallback`` | ``robust``. Deve essere
        lo stesso del clean, altrimenti il profilo può riuscire dove il clean
        fallisce (o viceversa).

    Returns
    -------
    dict with keys:
        - columns_raw (list[str])
        - columns_norm (list[str])
        - duckdb_types (list[str])
        - sample_rows (list[dict])
        - missingness_top (list[dict])
        - row_count (int | None)
        - mapping_suggestions (dict)
        - warnings (list[str]): extended with runtime-specific warnings
        - robust_read_suggested (bool): True if robust preset was needed
    """
    true_header_line: str | None = sniff_hints.get("true_header_line")
    warnings = list(sniff_hints.get("warnings") or [])
    robust_read_suggested = False
    row_count: int | None = None

    try:
        with safe_connect() as con:
            info = create_profile_relation(
                con, files, effective_read_cfg, read_mode=read_mode, logger=logger
            )
            if info.source == "robust":
                # Il fallback strict→robust è dentro read_raw_to_relation: qui
                # resta solo il fatto (e resta nel warning, con lo stesso tag
                # usato dal resto della diagnostica).
                robust_read_suggested = True
                warnings.append(
                    "profile_read_retry: strict read failed, profiled with robust preset"
                )

            # Forza eager evaluation e riusa il conteggio: DuckDB crea la
            # vista in modo lazy, l'errore di parsing scoppia al primo SELECT
            # che tocca le colonne.
            row_count = int(con.execute(f"SELECT COUNT(*) FROM {RAW_INPUT_VIEW}").fetchone()[0])

            columns_raw, columns_norm, duckdb_types = _describe_columns(con)

            # Diagnostica (non più meccanismo): l'header reale del file ha meno
            # token delle colonne lette? Con `read_raw_to_relation` la lettura
            # è già avvenuta e, se il file è irregolare, è il fallback robusto
            # ad averla risolta. Qui resta il segnale per l'operatore.
            # Escluso quando le colonne sono dichiarate in config (il conteggio
            # riflette la config, non il file) e quando i file sono più di uno
            # (la union per nome allarga legittimamente le colonne).
            explicit_columns = (effective_read_cfg or {}).get("columns")
            multi_file = not isinstance(files, Path) and len(list(files)) > 1
            if true_header_line is not None and not explicit_columns and not multi_file:
                sep_effective = (
                    effective_read_cfg.get("sep") or effective_read_cfg.get("delim") or ";"
                )
                true_header_tokens = true_header_line.count(sep_effective) + 1
                if len(columns_raw) > true_header_tokens:
                    warnings.append(
                        f"header_data_cols_mismatch: header has {true_header_tokens} tokens, "
                        f"data has {len(columns_raw)} columns; the file rows are wider than "
                        "its header — consider null_padding/align_by_header"
                    )

            duckdb_type_map: dict[str, str] = {
                raw: dtype for raw, dtype in zip(columns_raw, duckdb_types)
            }
            sample_rows, missingness_top, null_counts = _sample_profile_rows(con, columns_raw)
            mapping_suggestions = _build_mapping_suggestions(
                columns_raw, sample_rows, duckdb_types=duckdb_type_map
            )
    except Exception as e:
        warnings.append(f"profile_failed: {type(e).__name__}: {e}")
        warnings.append(
            "python_fallback_used: suggested_read generated from lightweight sniffing only"
        )
        columns_raw, columns_norm, duckdb_types = [], [], []
        sample_rows, missingness_top, null_counts = [], [], {}
        mapping_suggestions = {}
        robust_read_suggested = True
        row_count = None

    return {
        "columns_raw": columns_raw,
        "columns_norm": columns_norm,
        "duckdb_types": duckdb_types,
        "sample_rows": sample_rows,
        "missingness_top": missingness_top,
        "null_counts": null_counts,
        "row_count": row_count,
        "mapping_suggestions": mapping_suggestions,
        "warnings": warnings,
        "robust_read_suggested": robust_read_suggested,
    }


# Pattern per riconoscere valori che sembrano date (NN/NN/NNNN, NNNN/NN/NN, ecc.)
# Usato in _extract_date_raw_values per trovare colonne VARCHAR che DuckDB
# non ha classificato come DATE per mancanza di dateformat.
_DATE_LIKE_RE = re.compile(r"^\d{1,4}[/\-]\d{1,2}[/\-]\d{1,4}$")


def _extract_date_raw_values(
    file0: Path,
    mapping_suggestions: dict[str, Any],
    columns_raw: list[str],
    effective_read_cfg: dict[str, Any],
) -> dict[str, list[str]]:
    """Extract raw string values for date-like columns before DuckDB conversion.

    Reads the raw CSV file directly (using the SAME read configuration as DuckDB)
    and determines which columns look like dates by scanning raw string values
    — NOT using DuckDB's ``sample_rows`` which already converts dates to Timestamp.

    Returns ``{col_name: [val1, val2, ...]}`` for each column that looks like
    a date, or empty dict if none found.
    """
    if not columns_raw:
        return {}

    enc = effective_read_cfg.get("encoding") or "utf-8"
    sep = effective_read_cfg.get("delim") or effective_read_cfg.get("sep") or ","
    skip_rows = int(effective_read_cfg.get("skip") or 0)
    has_header = bool(effective_read_cfg.get("header", True))

    dialect_kwargs: dict[str, Any] = {"delimiter": sep}
    if effective_read_cfg.get("quote"):
        dialect_kwargs["quotechar"] = effective_read_cfg["quote"]
    if effective_read_cfg.get("escape"):
        dialect_kwargs["escapechar"] = effective_read_cfg["escape"]

    # Single pass: read first 30 data rows, sample values per column.
    # Both detection and extraction use the SAME 30-row sample — keeps
    # profile size bounded (date_raw_values has at most 30 items/column).
    MAX_SAMPLE = 30
    raw_samples: dict[int, list[str]] = {}
    rows_read = 0

    try:
        with open(file0, encoding=enc, newline="") as f:
            reader = csv.reader(f, **dialect_kwargs)

            for _ in range(skip_rows):
                next(reader, None)
            if has_header:
                next(reader, None)

            for row in reader:
                if not row:
                    continue
                if rows_read >= MAX_SAMPLE:
                    break
                rows_read += 1
                for i, val in enumerate(row):
                    trimmed = val.strip()
                    if trimmed:
                        raw_samples.setdefault(i, []).append(trimmed)
    except Exception:
        pass

    if not raw_samples:
        return {}

    # Find columns where >= 60% of sample values look like dates
    date_col_indices: set[int] = set()
    for col_idx, values in raw_samples.items():
        if not values:
            continue
        date_like = sum(1 for v in values if _DATE_LIKE_RE.match(v))
        if date_like >= len(values) * 0.6 and col_idx < len(columns_raw):
            date_col_indices.add(col_idx)

    if not date_col_indices:
        return {}

    # Build result from the same 30-row sample
    result: dict[str, list[str]] = {}
    for col_idx in sorted(date_col_indices):
        col_name = columns_raw[col_idx]
        vals = raw_samples.get(col_idx, [])
        if vals:
            result[col_name] = vals

    return result


@dataclass
class RawProfile:
    dataset: str
    year: int
    file_used: str

    encoding_suggested: str | None
    delim_suggested: str | None
    decimal_suggested: str | None
    skip_suggested: int
    robust_read_suggested: bool

    header_line: str | None
    columns_raw: list[str]
    columns_norm: list[str]
    row_count: int | None

    missingness_top: list[dict[str, Any]]
    sample_rows: list[dict[str, Any]]
    mapping_suggestions: dict[str, Any]
    date_raw_values: dict[str, list[str]]

    warnings: list[str]


def profile_raw(
    raw_dir: Path,
    dataset: str,
    year: int,
    read_cfg: dict[str, Any] | None = None,
    *,
    primary_file: Path | None = None,
    read_mode: str = "fallback",
    source_files: list[Path] | None = None,
    root: str | Path | None = None,
    logger=None,
) -> RawProfile:
    """Profile a RAW directory.

    Facade: seleziona i file come il CLEAN (``_select_profile_files``), sniffa
    il primo file selezionato (encoding, delimiter, skip, header) e profila la
    relazione con la stessa pipeline di lettura del CLEAN
    (``core.duckdb_read.read_raw_to_relation``).

    Parameters
    ----------
    raw_dir:
        Directory containing RAW files.
    dataset:
        Dataset slug.
    year:
        Year of the dataset.
    read_cfg:
        Optional explicit read configuration (``clean.read``). Values here
        override the sniffed suggestions. Contiene anche ``mode``/``include``/
        ``glob``, che determinano **quali** file vengono letti.
    primary_file:
        Optional explicit path to the primary source file. Vincola la
        selezione con ``mode: latest``/``fallback`` (la maggioranza dei
        dataset); con ``mode: all|explicit|largest`` vince la selezione del
        clean e questo valore fa solo da fallback.
    read_mode:
        Clean read mode (``strict`` | ``fallback`` | ``robust``). Pass the
        dataset's ``clean.read_mode`` so the profile cannot succeed with a
        config the clean layer would reject.
    source_files:
        I file scritti dal run RAW corrente (``run_raw``). Quando presenti
        hanno precedenza sul manifest su disco, che a questo punto è ancora
        quello del run precedente.
    root:
        Root del dataset (la directory che contiene ``data/raw/...``). Esplicito
        è meglio: senza, si inferisce dal layout di ``raw_dir``. Serve a
        ``select_raw_input`` per leggere le entry del manifest come fa il clean.
    logger:
        Logger for the shared read pipeline (retry/fallback diagnostics).

    Returns
    -------
    RawProfile
    """
    files = _raw_files(raw_dir)
    if not files:
        raise FileNotFoundError(f"No RAW files found in {raw_dir}")

    file0 = primary_file if primary_file is not None else _pick_data_file(files)

    # Stessa selezione del CLEAN: con mode=all/explicit/largest il clean legge
    # più file (o un sottoinsieme per `include`), e profile_relation
    # descriverebbe un'altra relazione se guardasse solo il primary.
    selected = _select_profile_files(
        raw_dir,
        file0,
        read_cfg,
        logger,
        source_files=source_files,
        dataset=dataset,
        year=year,
        root=root,
    )
    # Lo sniff descrive la relazione che viene letta: con `include` il primary
    # del manifest può non essere tra i file selezionati.
    file0 = selected[0]

    # Phase 1: pure source sniffing
    sniff_hints = sniff_source_file(file0)

    enc = sniff_hints["encoding_suggested"]
    delim = sniff_hints["delim_suggested"]
    dec = sniff_hints["decimal_suggested"]
    skip = sniff_hints["skip_suggested"]
    header_line = sniff_hints["header_line"]

    # Phase 1b: Excel-like files — profilo via la stessa view del runtime clean
    # (.ods incluso: è supportato dal clean, non è un "zip non supportato")
    binary_fmt = sniff_hints.get("is_binary_file")
    if binary_fmt in ("xlsx", "xls", "ods"):
        runtime_result = profile_excel(
            selected[0] if len(selected) == 1 else selected,
            read_cfg,
            read_mode=read_mode,
            logger=logger,
        )

        return RawProfile(
            dataset=dataset,
            year=year,
            file_used=str(file0.name),
            encoding_suggested=None,
            delim_suggested=None,
            decimal_suggested=None,
            skip_suggested=skip,
            robust_read_suggested=runtime_result["robust_read_suggested"],
            header_line=None,
            columns_raw=runtime_result["columns_raw"],
            columns_norm=runtime_result["columns_norm"],
            row_count=runtime_result.get("row_count"),
            missingness_top=runtime_result["missingness_top"],
            sample_rows=runtime_result["sample_rows"],
            mapping_suggestions=runtime_result["mapping_suggestions"],
            date_raw_values={},
            warnings=runtime_result["warnings"],
        )
    # ZIP or other unsupported binary — return empty profile with warning
    if binary_fmt == "zip":
        return RawProfile(
            dataset=dataset,
            year=year,
            file_used=str(file0.name),
            encoding_suggested=None,
            delim_suggested=None,
            decimal_suggested=None,
            skip_suggested=skip,
            robust_read_suggested=True,
            header_line=None,
            columns_raw=[],
            columns_norm=[],
            row_count=None,
            missingness_top=[],
            sample_rows=[],
            mapping_suggestions={},
            date_raw_values={},
            warnings=["binary_file_not_supported: zip — use a different source format"],
        )

    # Phase 2: resolve effective read cfg (sniff + user override)
    effective_read_cfg = _effective_profile_read_cfg(
        read_cfg,
        encoding=enc,
        delim=delim,
        decimal=dec,
        skip=skip,
    )

    # Phase 3: runtime profiling — stessa pipeline di lettura del layer CLEAN
    runtime_result = profile_with_read_cfg(
        selected,
        sniff_hints,
        effective_read_cfg,
        read_mode=read_mode,
        logger=logger,
    )
    # row_count arriva dal profilo: la view è già stata scansionata una volta,
    # non serve una seconda connessione che rilegge il file da capo.
    row_count = runtime_result.get("row_count")

    # Phase 4: extract raw string values for date-typed columns
    # (DuckDB converts dates to Timestamp, losing the original format).
    # Solo con un file: la mappa è posizionale, e su una union multi-file
    # l'ordine delle colonne può differire da quello del singolo file.
    date_raw_values: dict[str, list[str]] = {}
    if len(selected) == 1:
        date_raw_values = _extract_date_raw_values(
            selected[0],
            runtime_result["mapping_suggestions"],
            runtime_result["columns_raw"],
            effective_read_cfg,
        )

    return RawProfile(
        dataset=dataset,
        year=year,
        file_used=str(file0.name),
        encoding_suggested=enc,
        delim_suggested=delim,
        decimal_suggested=dec,
        skip_suggested=skip,
        robust_read_suggested=runtime_result["robust_read_suggested"],
        header_line=header_line,
        columns_raw=runtime_result["columns_raw"],
        columns_norm=runtime_result["columns_norm"],
        row_count=row_count,
        missingness_top=runtime_result["missingness_top"],
        sample_rows=runtime_result["sample_rows"],
        mapping_suggestions=runtime_result["mapping_suggestions"],
        date_raw_values=date_raw_values,
        warnings=runtime_result["warnings"],
    )


def write_raw_profile(
    out_dir: Path,
    profile: RawProfile,
    *,
    write_canonical: bool = True,
) -> dict[str, Path]:
    _safe_mkdir(out_dir)

    p_raw_json = out_dir / RAW_PROFILE
    payload = asdict(profile)
    written: dict[str, Path] = {}

    if write_canonical:
        write_json_atomic(p_raw_json, payload)
        written["raw_json"] = p_raw_json

    return written


def load_raw_profile_status(raw_dir: Path) -> dict[str, Any]:
    """Stato compatto del profilo RAW di un anno, per validation/readiness.

    Il profilo vive in ``_profile/raw_profile.json`` e può esistere anche
    quando è *vuoto*: ``profile_raw`` cattura l'errore, scrive i warning
    nell'artefatto e restituisce zero colonne. Senza questo stato il run
    resterebbe verde con un profilo inutilizzabile.

    Returns:
        ``{"status": "ok"|"failed"|"missing", "row_count", "columns",
        "file", "warnings"}``. ``status`` è ``failed`` quando il profilo
        esiste ma non ha colonne (fallback allo sniff leggero).
    """
    path = raw_dir / RAW_PROFILE_DIR / RAW_PROFILE
    if not path.exists():
        return {"status": "missing", "row_count": None, "columns": 0, "file": None, "warnings": []}

    payload = read_json_or_none(path) or {}
    columns = payload.get("columns_raw") or []
    warnings = [str(w) for w in (payload.get("warnings") or [])]
    return {
        "status": "ok" if columns else "failed",
        "row_count": payload.get("row_count"),
        "columns": len(columns),
        "file": payload.get("file_used"),
        "warnings": warnings,
    }
