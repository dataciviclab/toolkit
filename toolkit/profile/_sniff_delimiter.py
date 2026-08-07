"""Delimiter and decimal sniffing utilities for CSV profiling."""

from __future__ import annotations

import re
from typing import Optional

COMMON_DELIMS = [";", ",", "\t", "|"]


def sniff_delim(sample_text: str) -> Optional[str]:
    lines = [ln for ln in sample_text.splitlines() if ln.strip()][:25]
    if not lines:
        return None
    scores = {}
    for d in COMMON_DELIMS:
        counts = [ln.count(d) for ln in lines]
        non_zero = [c for c in counts if c > 0]
        if not non_zero:
            continue
        variance = max(non_zero) - min(non_zero)
        scores[d] = (len(non_zero), -variance, sum(non_zero))
    if not scores:
        return None
    return sorted(scores.items(), key=lambda kv: (kv[1][0], kv[1][1], kv[1][2]), reverse=True)[0][0]


def sniff_decimal(sample_text: str, delim: Optional[str] = None) -> Optional[str]:
    if delim is None:
        # Legacy (senza delim): conteggio regex su tutto il chunk.
        chunk = sample_text[:200_000]
        comma_dec = len(re.findall(r"\d+,\d{1,3}\b", chunk))
        dot_dec = len(re.findall(r"\d+\.\d{1,3}\b", chunk))
        if comma_dec == 0 and dot_dec == 0:
            return None
        return "," if comma_dec >= dot_dec else "."

    # Field-aware: ragiona sui campi separati dal delim del CSV. La virgola che
    # separa due colonne (es. "2008,88") NON è un separatore decimale — senza
    # delim produce falsi positivi quando i valori numerici hanno <=3 cifre.
    chunk = sample_text[:200_000]
    comma_dec = 0
    dot_dec = 0
    for line in chunk.splitlines():
        fields = [f.strip().strip('"') for f in line.split(delim)]
        for field in fields:
            if re.fullmatch(r"\d+,\d{1,2}", field):
                # "88,5" — virgola decimale semplice (es. delim ';')
                comma_dec += 1
            elif re.fullmatch(r"\d+\.\d{1,3}", field):
                # "88.5" / "21900.0" — punto decimale semplice
                dot_dec += 1
            elif re.fullmatch(r"\d+\.\d{3},\d{1,2}", field):
                # "1.234,56" — numero italiano completo (delim ';': la virgola
                # decimale resta interna al campo)
                comma_dec += 1
        # Coppie adiacenti in formato italiano "1.234" + "56" (mille-separatore
        # col punto + decimali con virgola) quando il delim è la virgola.
        for i in range(len(fields) - 1):
            if re.fullmatch(r"\d+\.\d{3}", fields[i]) and re.fullmatch(r"\d{1,2}", fields[i + 1]):
                comma_dec += 1
    if comma_dec == 0 and dot_dec == 0:
        return None
    return "," if comma_dec >= dot_dec else "."


def suggest_skip(sample_text: str, delim: Optional[str]) -> int:
    if not delim:
        return 0
    lines = [ln for ln in sample_text.splitlines() if ln.strip()][:5]
    if len(lines) < 2:
        return 0
    first_count = lines[0].count(delim)
    second_count = lines[1].count(delim)
    if first_count == 0 and second_count > 0:
        return 1
    if first_count < second_count and first_count <= 1 and second_count >= 3:
        return 1
    return 0
