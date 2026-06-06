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
_YEAR_START_RE = re.compile(r"(?:^|(?<!\d))(20[012]\d)")


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

    # Pattern due anni quadridigit adiacenti: "20142025" → 2014, 2025
    adjacent = re.findall(r"(?<!\d)(20[012]\d)(20[012]\d)(?!\d)", text)
    for y1_str, y2_str in adjacent:
        y1, y2 = int(y1_str), int(y2_str)
        if y1 < y2 <= y1 + 50:
            years.update([y1, y2])

    # Pattern anno all'inizio stringa o dopo boundary non-digit
    for y in _YEAR_START_RE.findall(text):
        years.add(int(y))

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
    (r"\bregion[ei]\b|\bregioni\b|piemonte|lombardia|veneto|emilia|toscana|lazio|campania|puglia|sicilia|sardegna|abruzzo|umbria|marche|molise|calabria|basilicata|friuli|trentin|liguria|valle d['\s]aosta", "regione"),
    (r"(?<![a-zA-Z])(REG|reg)(?![a-zA-Z])", "regione"),
    (r"\bnazional[ei]\b|\bitali[ae]\b|\bnazione\b|\bnational\b|\bregional\b", "nazionale"),
    (r"\beurope[ao]\b|\bue\b|\beuropa\b|\beuropean\b", "europeo"),
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
