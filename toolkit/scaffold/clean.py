from __future__ import annotations

import datetime
import re
from pathlib import Path
from typing import Any


def _snake_case(name: str) -> str:
    """Convert a column name to snake_case (best-effort)."""

    # Replace spaces and special chars with underscores
    s = name.strip()
    # Insert underscore before uppercase letters that follow lowercase
    s = re.sub(r"([a-z])([A-Z])", r"\1_\2", s)
    # Replace non-alphanumeric with underscores
    s = re.sub(r"[^a-zA-Z0-9]+", "_", s)
    # Lowercase and collapse multiple underscores
    s = re.sub(r"_+", "_", s).lower().strip("_")
    return s or name


def _map_duckdb_type(raw_type: str) -> str:
    """Map raw profiling type to DuckDB SQL type for TRY_CAST."""
    raw_type_lower = raw_type.lower().strip()
    if raw_type_lower in (
        "int",
        "integer",
        "bigint",
        "hugeint",
        "smallint",
        "tinyint",
        "ubigint",
        "uinteger",
    ):
        return "BIGINT"
    if raw_type_lower in ("double", "float", "real", "hugeint", "uhugeint"):
        return "DOUBLE"
    if raw_type_lower in (
        "date",
        "datetime",
        "timestamp",
        "timestamp_s",
        "timestamp_ms",
        "timestamp_ns",
    ):
        return "DATE"
    if raw_type_lower in ("bool", "boolean"):
        return "BOOLEAN"
    return "VARCHAR"


def _map_datastore_type(ds_type: str) -> str:
    """Map CKAN DataStore type to DuckDB SQL type.

    DataStore types (https://docs.ckan.org/en/latest/maintaining/datastore.html):
      ``numeric``, ``int``, ``integer``, ``bigint`` → ``BIGINT``
      ``float``, ``double``, ``real`` → ``DOUBLE``
      ``text`` → ``VARCHAR``
      ``timestamp``, ``date`` → ``DATE``
      ``bool``, ``boolean`` → ``BOOLEAN``
    """
    dt = ds_type.strip().lower()
    if dt in ("numeric", "int", "integer", "bigint", "smallint", "tinyint"):
        return "BIGINT"
    if dt in ("float", "double", "real", "number"):
        return "DOUBLE"
    if dt in ("timestamp", "date", "datetime"):
        return "DATE"
    if dt in ("bool", "boolean"):
        return "BOOLEAN"
    return "VARCHAR"  # text, json, array, e unknown


def profile_from_datastore(fields: list[dict[str, Any]]) -> dict[str, Any]:
    """Costruisce un profile dict compatibile con ``generate_clean_sql()``
    a partire dai fields restituiti da ``datastore_search?limit=0``.

    Args:
        fields: Lista di dict CKAN DataStore con ``id``, ``type``,
                opzionalmente ``info.label`` e ``info.notes``.

    Returns:
        Profile dict con ``mapping_suggestions`` utilizzabile da
        ``generate_clean_sql()`` e ``generate_full_scaffold()``.
    """
    mapping: dict[str, dict[str, str]] = {}
    for f in fields:
        col = f.get("id", "")
        if not col:
            continue
        ds_type = f.get("type", "text")
        sql_type = _map_datastore_type(ds_type)
        mapping[col] = {"type": sql_type}
    return {
        "mapping_suggestions": mapping,
        "columns_raw": [f["id"] for f in fields if f.get("id")],
    }


# Formati data organizzati per gruppo con stesso separatore.
# I gruppi con due formati (DMY/MDY) richiedono disambiguazione:
# valori che parsano in UN SOLO formato votano quello; valori che
# parsano in entrambi (es. "03/04/2024") sono ambigui e non contano.
# I gruppi con un solo formato (YYYY/mm/dd) sono non-ambigu.
_DATE_FORMAT_GROUPS: list[tuple[str, ...]] = [
    ("%d/%m/%Y", "%m/%d/%Y"),  # slash 4-digit
    ("%d-%m-%Y", "%m-%d-%Y"),  # dash 4-digit
    ("%d/%m/%y", "%m/%d/%y"),  # slash 2-digit
    ("%d-%m-%y", "%m-%d-%y"),  # dash 2-digit
    ("%Y/%m/%d",),  # ISO con slash
]


def _try_strptime(value: str, fmt: str) -> bool:
    """Try to parse a date string with the given strptime format."""
    try:
        datetime.datetime.strptime(value, fmt)
        return True
    except ValueError:
        return False


def _suggest_dateformat(profile: dict[str, Any]) -> str | None:
    """Detect non-ISO date format from raw date values in the profile.

    Uses ``date_raw_values`` (extracted from raw CSV *before* DuckDB
    converts dates) and ``datetime.strptime`` for validation.

    For ambiguous separators (``/``, ``-``), counts how many values
    successfully parse in each format (DMY and MDY). Values that parse
    in both formats are counted for both — the ambiguity means neither
    format is *excluded*, but the format with more total parses wins.

    Each column picks its best format (>=60% of its non-empty values).
    Only suggests a ``dateformat`` if EVERY column picks the SAME format.

    Returns the ``dateformat`` string or ``None``.
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

                # Only consider when one format clearly wins over the other
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


def _find_anno_raw_column(profile: dict[str, Any]) -> str | None:
    """Find the raw column name that looks like a year column.

    Returns the raw (un-normalized) column name, or None if no year column
    is found. Checks both mapping_suggestions and columns_raw.
    """
    mapping = profile.get("mapping_suggestions", {})
    if mapping:
        col_names: list[str] = list(mapping.keys())
    else:
        col_names = profile.get("columns_raw", [])

    for col in col_names:
        normalized = _snake_case(col).lower()
        if normalized in ("anno", "anno_di_imposta", "anno_imposta", "year", "tax_year"):
            return col
    return None


def _has_anno_column(profile: dict[str, Any]) -> bool:
    """Check if any raw column name looks like a year column after normalization."""
    return _find_anno_raw_column(profile) is not None


def _select_expr(
    raw_col: str,
    sql_type: str,
    out_name: str,
) -> str:
    """Build a SELECT expression for one column with smart transformations.

    - VARCHAR columns: TRIM with CAST AS VARCHAR (safe for columns sniffed as BIGINT)
      Comma-decimal numbers are handled by clean.read.decimal in dataset.yml
      (see propose_clean_read()), not by REPLACE in SQL — DuckDB's read_csv
      native decimal support is more reliable and avoids DOUBLE→STRING round-trips.
    """
    if sql_type == "VARCHAR":
        return f'trim(CAST("{raw_col}" AS VARCHAR)) AS {out_name}'

    return f'TRY_CAST("{raw_col}" AS {sql_type}) AS {out_name}'


def _columns_spec(profile: dict[str, Any], year: int) -> tuple[list[str], dict[str, str]]:
    """Build SELECT expressions and read_csv columns spec from mapping_suggestions."""
    mapping = profile.get("mapping_suggestions", {})
    if not mapping:
        # Fallback to columns_raw if mapping is empty (e.g. Excel profiling,
        # or profiling that failed to infer types). Use trim(CAST(... AS VARCHAR))
        # to safely handle both text and numeric columns.
        columns_raw = profile.get("columns_raw", [])
        if columns_raw:
            select_exprs = [
                f'trim(CAST("{c}" AS VARCHAR)) AS {_snake_case(c)}' for c in columns_raw
            ]
            columns_spec = {c: "VARCHAR" for c in columns_raw}
            return select_exprs, columns_spec
        return ["*"], {}

    # Reaching here means mapping was non-empty; define fresh.
    select_exprs: list[str] = []  # type: ignore[no-redef]
    columns_spec: dict[str, str] = {}  # type: ignore[no-redef]

    for raw_col, spec in mapping.items():
        raw_type = spec.get("type", "str")
        sql_type = _map_duckdb_type(raw_type)
        out_name = _snake_case(raw_col)

        select_exprs.append(_select_expr(raw_col, sql_type, out_name))
        columns_spec[raw_col] = sql_type

    return select_exprs, columns_spec


def scaffold_clean_if_missing(
    profile: dict[str, Any],
    dataset: str,
    year: int,
    base_dir: Path | str,
    clean_cfg: dict[str, Any] | None,
    logger,
) -> str | None:
    """
    Scaffold clean.sql from a raw profile dict, if the file doesn't exist.

    Returns the path written, or None if skipped because file already exists.
    ``base_dir`` is the directory containing dataset.yml.
    ``clean_cfg`` is the ``clean:`` section of dataset.yml (may be None or empty).
    """
    clean_cfg = clean_cfg or {}
    clean_sql_rel = clean_cfg.get("sql", "sql/clean.sql")
    clean_sql_path = Path(base_dir) / clean_sql_rel

    if clean_sql_path.exists():
        logger.info("clean.sql gia esistente, scaffold saltato (%s)", clean_sql_path)
        return None

    scaffold_sql = generate_clean_sql(profile, dataset, year)
    clean_sql_path.parent.mkdir(parents=True, exist_ok=True)
    clean_sql_path.write_text(scaffold_sql, encoding="utf-8")
    logger.info("scaffold clean.sql -> %s", clean_sql_path)

    # Log suggested clean.read config if not already present
    if not clean_cfg.get("read"):
        proposal = format_clean_read_proposal(profile)
        logger.info(
            "Proposta clean.read (da aggiungere a dataset.yml):\n%s",
            proposal,
        )
    return str(clean_sql_path)


def generate_clean_sql(
    profile: dict[str, Any],
    dataset: str,
    year: int,
) -> str:
    """Generate a portable first-draft clean.sql from a RAW profile.

    The generated SQL is a pure transformation query against the ``raw_input``
    view (created by clean.read at runtime). All CSV parsing options belong
    in ``clean.read`` in dataset.yml — run ``propose_clean_read()`` to get
    the suggested config.

    Improvements over basic TRY_CAST scaffold:
    - TRIM for VARCHAR columns (mirrors real usage)
    - WHERE clause filtering null years (if anno column exists in source)
    - Comma-decimal handling via clean.read.decimal config (see propose_clean_read())
    """
    file_used = profile.get("file_used", "")

    # Build SELECT expressions
    select_exprs, _ = _columns_spec(profile, year)

    # If the CSV doesn't have a column named "anno" (after normalization),
    # inject {year}::INTEGER AS anno so the clean layer has it without requiring
    # a special mapping.
    has_real_anno = _has_anno_column(profile)
    if not has_real_anno:
        select_exprs.insert(0, "{year}::INTEGER AS anno")

    # Build header comment
    header_parts = ["-- Generated by toolkit scaffold clean"]
    if file_used:
        header_parts.append(f"-- Source: data/raw/{dataset}/{year}/{file_used}")

    encoding = profile.get("encoding_suggested")
    delim = profile.get("delim_suggested")

    meta_parts = []
    if encoding:
        meta_parts.append(f"Encoding: {encoding}")
    if delim:
        meta_parts.append(f"Delimiter: {delim}")
    if profile.get("decimal_suggested"):
        meta_parts.append(f"Decimal: {profile['decimal_suggested']}")

    if meta_parts:
        header_parts.append(f"-- {' | '.join(meta_parts)}")

    warnings = profile.get("warnings", [])
    if warnings:
        header_parts.append("--")
        header_parts.append("-- Warnings from profiling:")
        for w in warnings[:10]:
            # I warning possono essere multi-linea (es. DuckDB error message).
            # I template placeholder ${...} vengono rimossi.
            w_clean = re.sub(r"\$\{[^}]+\}", "${?}", w)
            for line in w_clean.splitlines():
                header_parts.append(f"--   {line}")

    header_parts.append("--")
    header_parts.append("-- This is a FIRST DRAFT. Review and adjust before running.")
    header_parts.append(
        "-- CSV parsing options (delim, columns, encoding, header, skip) "
        "belong in clean.read in dataset.yml."
    )
    header_parts.append("-- Pass --run to execute raw and clean in one step, or run:")
    header_parts.append("--   toolkit run clean -c dataset.yml")
    header_parts.append("")

    # Build SELECT clause
    select_block = "    " + ",\n    ".join(select_exprs)

    sql_lines = header_parts
    sql_lines.append("SELECT")
    sql_lines.append(select_block)
    sql_lines.append("FROM raw_input")

    # WHERE clause: filter out null years when the CSV has a real anno column.
    # This mirrors real clean.sql patterns (e.g. terna, civile-flussi).
    if has_real_anno:
        anno_raw = _find_anno_raw_column(profile)
        if anno_raw is not None:
            # Use TRY_CAST for safety — some years may be non-integer (e.g. "2024a")
            sql_lines.append(f'WHERE try_cast("{anno_raw}" AS INTEGER) IS NOT NULL')

    sql_lines.append("")

    return "\n".join(sql_lines)


def _header_names_from_line(header_line: str | None, delim: str | None) -> list[str]:
    """Extract stripped column names from a header line string."""
    if not header_line or not delim:
        return []
    return [c.strip() for c in header_line.split(delim)]


def _names_match(keys: list[str], header_names: list[str]) -> bool:
    """Check if mapping keys and header names refer to the same columns after normalization."""
    if len(keys) != len(header_names):
        return False
    norm_keys = [_snake_case(k) for k in keys]
    norm_hdr = [_snake_case(h) for h in header_names]
    return norm_keys == norm_hdr


def _looks_like_real_header(names: list[str]) -> bool:
    """Check if header names look like real column identifiers, not data.

    Guards against the false-positive case: a positional CSV with no header
    line but first row containing values like '1;2;3' or 'foo;bar;baz'.
    Real column identifiers typically contain at least one letter.
    """
    if not names:
        return False
    # All names must contain at least one letter to be considered real column names.
    # This filters out purely numeric values (e.g. '1', '2', '3') and
    # generic strings that could be data values.
    return all(any(c.isalpha() for c in name) for name in names)


def propose_clean_read(profile: dict[str, Any]) -> dict[str, Any]:
    """Build a suggested ``clean.read`` config section from a raw profile.

    Returns a dict suitable for serialization into ``dataset.yml`` under
    ``clean.read``.  The caller should merge with any existing config.
    """
    read: dict[str, Any] = {}

    # --- CSV parsing parameters ---
    delim = profile.get("delim_suggested")
    if delim:
        read["delim"] = delim

    encoding = profile.get("encoding_suggested")
    if encoding:
        read["encoding"] = encoding

    decimal = profile.get("decimal_suggested")
    if decimal:
        read["decimal"] = decimal

    # --- dateformat: auto-detect non-ISO date formats (es. dd/mm/YYYY) ---
    date_fmt = _suggest_dateformat(profile)
    if date_fmt:
        read["dateformat"] = date_fmt

    # --- Columns: raw_name -> DuckDB type ---
    mapping = profile.get("mapping_suggestions", {})
    has_columns = bool(mapping)
    raw_header = profile.get("header_line") is not None

    if has_columns:
        # When mapping exists, check whether it reflects a real header or was built
        # with header=false (profiler read the first data row as column names).
        # Scenario: header_line="CodiceEnte;Importo" but mapping has {column01, column02}
        # → real header exists, propose header:true and drop the generic columns.
        header_line = profile.get("header_line")
        header_names = _header_names_from_line(header_line, delim)
        keys = list(mapping.keys())

        if (
            header_names
            and _looks_like_real_header(header_names)
            and not _names_match(keys, header_names)
        ):
            # Header names look real (contain letters) and differ from mapping keys
            # → file has a real header that was misread.
            effective_header = True
            columns: dict[str, str] | None = None
            # skip stays as-is (no bump needed, header line is now real)
            effective_skip = profile.get("skip_suggested", 0)
        elif (
            header_names
            and _looks_like_real_header(header_names)
            and _names_match(keys, header_names)
        ):
            # Header names match mapping keys → header: true, skip auto-rilevato
            effective_header = True
            columns = None
            effective_skip = profile.get("skip_suggested", 0)
        else:
            # No real header (es. prima riga numerica) → columns esplicite
            effective_header = False
            columns = {}
            for raw_col, spec in mapping.items():
                raw_type = spec.get("type", "str")
                columns[raw_col] = _map_duckdb_type(raw_type)
            raw_skip = profile.get("skip_suggested", 0)
            effective_skip = raw_skip + 1 if raw_header else raw_skip
    else:
        # No mapping → auto-detect header
        effective_header = raw_header
        columns = None
        effective_skip = profile.get("skip_suggested", 0)

    if columns is not None:
        read["columns"] = columns

    read["header"] = effective_header
    if effective_skip > 0:
        read["skip"] = effective_skip

    return read


def format_clean_read_proposal(profile: dict[str, Any]) -> str:
    """Format a raw profile into a YAML-formatted clean.read proposal.

    The output is ready to paste under ``clean:`` in dataset.yml.
    Validates the proposed dict against CleanReadConfig before formatting.
    Includes ``read_mode: robust`` as a top-level ``clean`` field when
    ``robust_read_suggested`` is set (not inside ``clean.read``).
    """
    from toolkit.core.config_models.clean import CleanReadConfig
    from toolkit.core.io import yaml_dumps

    proposed = propose_clean_read(profile)
    if not proposed:
        return "# Nessuna config read suggerita (nessun mapping o columns_raw disponibile)"

    # Validate: raises ValidationError if keys are invalid or types wrong
    CleanReadConfig(**proposed)

    result: dict[str, Any] = {"clean": {"read": proposed}}

    # read_mode: robust goes outside clean.read (it's a clean-level field)
    if profile.get("robust_read_suggested"):
        result["clean"]["read_mode"] = "robust"

    return yaml_dumps(result)
