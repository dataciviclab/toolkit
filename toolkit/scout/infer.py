"""Inferenze pure per URL scout — anni, granularità, topic, validation, SQL.

Tutta logica pura (no HTTP, no I/O). Serve a produrre scaffold
intelligenti per candidate dataset. Usabile da CLI, MCP e SO.
"""

from __future__ import annotations

import re
from typing import Any

# ---------------------------------------------------------------------------
# Year inference
# ---------------------------------------------------------------------------

_YEAR_RE = re.compile(r"(?<!\d)(19\d{2}|20[012]\d)(?!\d)")


def infer_years(text: str) -> tuple[int | None, int | None]:
    """Estrae anni minimo e massimo da un testo.

    Cerca pattern anno (1900-2029) in:
    - URL / nome file / descrizione
    - Valori separati da trattino (es. "2015-2023")
    - Anni compatti (es. "202122" per 2021-2022)
    """
    years: set[int] = set()

    # Pattern anni compatti: 202122 → 2021, 2022
    compact = re.findall(r"(20[012]\d)(\d{2})(?=20|$|\D)", text)
    for first_str, second_str in compact:
        y1 = int(first_str)
        y2_2digit = int(second_str)
        if y2_2digit <= 30:
            y2 = 2000 + y2_2digit
            if y1 < y2 <= y1 + 10:
                years.add(y1)
                years.add(y2)

    # Pattern anno standard
    for y in _YEAR_RE.findall(text):
        years.add(int(y))

    if not years:
        return None, None

    return min(years), max(years)


def infer_years_from_url(url: str) -> tuple[int | None, int | None]:
    """Inferisce anni da un URL."""
    return infer_years(url)


def infer_years_from_columns(column_names: list[str]) -> tuple[int | None, int | None]:
    """Inferisce anni da nomi di colonna."""
    combined = " ".join(column_names)
    return infer_years(combined)


def suggest_years(
    *,
    url: str = "",
    column_names: list[str] | None = None,
    profile: dict[str, Any] | None = None,
) -> list[int]:
    """Suggerisce una lista di anni per il dataset.yml.

    Combina inferenze da URL, colonne e profilo.
    Fallback: [2024] se nessuna inferenza.
    """
    candidates: set[int] = set()

    if url:
        y_min, y_max = infer_years_from_url(url)
        if y_min is not None and y_max is not None:
            candidates.update(range(y_min, y_max + 1))
        elif y_min is not None:
            candidates.add(y_min)

    if column_names:
        y_min, y_max = infer_years_from_columns(column_names)
        if y_min is not None and y_max is not None:
            candidates.update(range(y_min, y_max + 1))

    # Da profilo: colonne che sembrano anni
    if profile:
        norm_cols = profile.get("columns_norm") or profile.get("columns_raw") or []
        y_min, y_max = infer_years_from_columns(norm_cols)
        if y_min is not None and y_max is not None:
            candidates.update(range(y_min, y_max + 1))

    if not candidates:
        return [2024]

    return sorted(candidates)


# ---------------------------------------------------------------------------
# Granularity inference
# ---------------------------------------------------------------------------

_GRANULARITY_PATTERNS: list[tuple[str, str]] = [
    (r"\bcomun[ei]\b|\bmunicip", "comune"),
    (r"\bprovinc", "provincia"),
    (r"\bregion[ei]\b|\bregioni\b", "regione"),
    (r"\bnazional[ei]\b|\bitali[ae]\b|\bnazione\b", "nazionale"),
    (r"\beurope[ao]\b|\bue\b|\beuropa\b", "europeo"),
    (r"\bmondial[ei]\b|\bmondo\b|\bglobal\b|\bworld\b", "mondiale"),
]


def infer_granularity(text: str) -> str:
    """Inferisce granularità territoriale da testo."""
    low = text.lower()
    for pattern, label in _GRANULARITY_PATTERNS:
        if re.search(pattern, low):
            return label
    return "non_determinato"


def infer_granularity_from_name_and_columns(name: str, column_names: list[str]) -> str:
    """Inferisce granularità da nome dataset + colonne."""
    combined = f"{name} {' '.join(column_names)}"
    return infer_granularity(combined)


# ---------------------------------------------------------------------------
# Topic inference
# ---------------------------------------------------------------------------

_TOPIC_KEYWORDS: list[tuple[list[str], str]] = [
    (["lavoro", "occupazione", "disoccupazione", "impiego", "stipendio", "salario", "retribuzione"], "lavoro"),
    (["economia", "pil", "crescita", "inflazione", "debito", "spesa", "bilancio", "finanza"], "economia"),
    (["sanità", "salute", "ospedale", "medico", "salute", "farmaco", "cura", "assistenza"], "sanita"),
    (["scuola", "istruzione", "università", "studente", "docente", "formazione", "educazione"], "istruzione"),
    (["trasporto", "mobilità", "traffico", "treno", "autobus", "strada", "ferrovia"], "trasporti"),
    (["ambiente", "ecologia", "rifiuti", "inquinamento", "clima", "energia rinnovabile", "natura"], "ambiente"),
    (["agricoltura", "coltivazione", "allevamento", "pesca", "forestale", "campagna"], "agricoltura"),
    (["turismo", "visitatore", "arrivo", "presenza", "albergo", "vacanza"], "turismo"),
    (["giustizia", "tribunale", "reato", "crimine", "penale", "carcere", "detenuto"], "giustizia"),
    (["demografia", "popolazione", "residente", "nascita", "morte", "immigrazione", "emigrazione"], "demografia"),
    (["energia", "elettrico", "gas", "petrolio", "rinnovabile", "consumo energetico"], "energia"),
    (["commercio", "vendita", "mercato", "export", "import", "scambio", "fatturato"], "commercio"),
    (["welfare", "assistenza", "sociale", "pensione", "sussidio", "beneficio"], "welfare"),
    (["previdenza", "pensione", "inps", "contributo", "pensionato"], "previdenza"),
    (["casa", "abitazione", "edilizia", "alloggio", "immobile", "proprietà"], "casa"),
    (["cultura", "museo", "biblioteca", "teatro", "mostra", "patrimonio"], "cultura"),
    (["bilancio", "budget", "entrata", "uscita", "tassa", "imposta", "fiscale"], "bilancio"),
    (["innovazione", "ricerca", "sviluppo", "tecnologia", "digitale", "startup"], "innovazione"),
    (["sicurezza", "polizia", "vigile", "protezione civile", "emergenza"], "sicurezza"),
]


def infer_topics(text: str) -> list[dict[str, Any]]:
    """Inferisce topic tematici da un testo.

    Returns lista di dict {topic, score}, ordinati per score decrescente.
    """
    low = text.lower()
    scores: dict[str, int] = {}
    for keywords, topic in _TOPIC_KEYWORDS:
        score = sum(1 for kw in keywords if kw in low)
        if score > 0:
            scores[topic] = score
    if not scores:
        return []
    sorted_topics = sorted(scores.items(), key=lambda x: -x[1])
    return [{"topic": t, "score": s} for t, s in sorted_topics]


# ---------------------------------------------------------------------------
# Validation rules suggestions
# ---------------------------------------------------------------------------


def suggest_validation(profile: dict[str, Any]) -> dict[str, Any]:
    """Suggerisce validation rules da inserire in dataset.yml.

    Basate sul profilo raw (colonne, row_count, tipi).
    """
    norm_cols = profile.get("columns_norm") or profile.get("columns_raw") or []
    row_count = profile.get("row_count", 0)
    validation: dict[str, Any] = {}

    # clean.validate
    clean_val: dict[str, Any] = {}
    if row_count:
        clean_val["min_rows"] = max(1, int(row_count * 0.5))
    if norm_cols:
        clean_val["required_columns"] = norm_cols[:5]  # prime 5 colonne
    if clean_val:
        validation["clean"] = {"validate": clean_val}

    # mart.validate
    mart_val: dict[str, Any] = {}
    if row_count:
        mart_val["min_rows"] = max(1, int(row_count * 0.5))
    if mart_val:
        validation["mart"] = {"validate": mart_val}

    return validation


# ---------------------------------------------------------------------------
# Smart SQL suggestions
# ---------------------------------------------------------------------------


def _has_year_column(columns: list[dict[str, Any]] | list[str]) -> bool:
    """Rileva se c'è una colonna che sembra contenere anni."""
    year_keywords = ["anno", "year", "periodo", "period", "data", "date", "mese", "month"]
    for col in columns:
        name = col if isinstance(col, str) else col.get("name", "")
        if any(kw in name.lower() for kw in year_keywords):
            return True
    return False


def _has_region_column(columns: list[dict[str, Any]] | list[str]) -> bool:
    region_keywords = ["regione", "region", "provincia", "province", "comune", "municip", "area", "territorio"]
    for col in columns:
        name = col if isinstance(col, str) else col.get("name", "")
        if any(kw in name.lower() for kw in region_keywords):
            return True
    return False


def _has_numeric_column(columns: list[dict[str, Any]] | list[str], profile: dict[str, Any]) -> bool:
    """Rileva se ci sono colonne numeriche."""
    mapping = profile.get("mapping_suggestions") or {}
    for col in columns:
        name = col if isinstance(col, str) else col.get("name", "")
        spec = mapping.get(name) or {}
        if spec.get("type") in ("integer", "float", "double", "bigint", "decimal", "int"):
            return True
        if isinstance(col, dict) and col.get("type") in ("integer", "float", "double", "int"):
            return True
    return False


def suggest_clean_sql(
    columns: list[dict[str, Any]] | list[str],
    profile: dict[str, Any],
) -> str:
    """Genera clean.sql con trasformazioni suggerite basate sul profilo."""
    # Normalizza colonne: lista di nomi
    if columns and isinstance(columns[0], dict):
        col_names = [c.get("name", f"col{i}") for i, c in enumerate(columns)]
    else:
        col_names = list(columns) if columns else []

    if not col_names:
        return (
            "-- ATTENZIONE: profiling non ha rilevato colonne.\n"
            "-- Rivedi il file e compila manualmente.\n"
            "SELECT 1 AS placeholder FROM raw_input\n"
        )

    mapping = profile.get("mapping_suggestions") or {}

    lines: list[str] = []
    lines.append("-- Auto-generated by toolkit init --url")
    lines.append("-- Personalizza le trasformazioni qui sotto.")
    lines.append("SELECT")

    select_parts: list[str] = []
    for name in col_names:
        spec = mapping.get(name) or {}
        raw_type = spec.get("type", "text") if isinstance(spec, dict) else "text"

        # Suggerisci cast se numerico
        # Supporta sia tipi DuckDB (integer, bigint, float, double, decimal)
        # che tipi shorthand del profiler (int, float, str)
        if raw_type in ("integer", "bigint", "int"):
            select_parts.append(f'  TRY_CAST("{name}" AS BIGINT) AS "{name}"')
        elif raw_type in ("float", "double", "decimal"):
            select_parts.append(f'  TRY_CAST("{name}" AS DOUBLE) AS "{name}"')
        elif raw_type in ("date",):
            select_parts.append(f'  TRY_CAST("{name}" AS DATE) AS "{name}"')
        else:
            select_parts.append(f'  "{name}"')

    lines.append(",\n".join(select_parts))
    lines.append("FROM raw_input")
    return "\n".join(lines) + "\n"


def suggest_mart_sql(
    columns: list[dict[str, Any]] | list[str],
    profile: dict[str, Any],
) -> str:
    """Genera mart.sql con aggregazione di base.

    Se ci sono colonne anno e regione: GROUP BY anno, regione con conteggio.
    Se solo anno: GROUP BY anno.
    Altrimenti: SELECT * FROM clean.
    """
    if columns and isinstance(columns[0], dict):
        col_names = [c.get("name", f"col{i}") for i, c in enumerate(columns)]
    else:
        col_names = list(columns) if columns else []

    if not col_names:
        return (
            "-- Default mart: SELECT * FROM clean.\n"
            "SELECT * FROM clean\n"
        )

    has_year = _has_year_column(col_names)
    has_region = _has_region_column(col_names)
    has_numeric = _has_numeric_column(col_names, profile)

    if has_year and has_numeric:
        # Trova la prima colonna numerica NON-anno per aggregazione
        year_keywords = ["anno", "year", "periodo", "period"]
        mapping = profile.get("mapping_suggestions") or {}
        numeric_col = None
        for name in col_names:
            is_year_col = any(kw in name.lower() for kw in year_keywords)
            spec = mapping.get(name) or {}
            if is_year_col:
                continue  # skip colonne che sembrano anni
            if spec.get("type") in ("integer", "float", "double", "bigint", "decimal", "int"):
                numeric_col = name
                break
        # Fallback: se nessuna colonna non-anno è numerica, usa la prima numerica
        if numeric_col is None:
            for name in col_names:
                spec = mapping.get(name) or {}
                if spec.get("type") in ("integer", "float", "double", "bigint", "decimal", "int"):
                    numeric_col = name
                    break

        if numeric_col:
            group_cols = [c for c in col_names if c != numeric_col]
            group_expr = ", ".join(f'"{c}"' for c in group_cols) if group_cols else ""
            if group_expr:
                return (
                    f"-- Aggregazione su {', '.join(group_cols)}\n"
                    f"SELECT\n"
                    f"  {group_expr},\n"
                    f'  SUM("{numeric_col}") AS totale_{numeric_col}\n'
                    f"FROM clean\n"
                    f"GROUP BY {group_expr}\n"
                    f"ORDER BY {group_expr}\n"
                )

    if has_year:
        return (
            "-- Conteggio record per anno\n"
            "SELECT\n"
            '  "anno" AS year,\n'
            "  COUNT(*) AS record_count\n"
            "FROM clean\n"
            'GROUP BY "anno"\n'
            'ORDER BY "anno"\n'
        )

    if has_region:
        return (
            "-- Conteggio record per regione\n"
            "SELECT\n"
            '  "regione" AS regione,\n'
            "  COUNT(*) AS record_count\n"
            "FROM clean\n"
            'GROUP BY "regione"\n'
            'ORDER BY "regione"\n'
        )

    return (
        "-- Default mart: SELECT * FROM clean.\n"
        "-- Personalizza per aggregazioni.\n"
        "SELECT * FROM clean\n"
    )
