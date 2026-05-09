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


def sniff_decimal(sample_text: str) -> Optional[str]:
    chunk = sample_text[:200_000]
    comma_dec = len(re.findall(r"\d+,\d{1,3}\b", chunk))
    dot_dec = len(re.findall(r"\d+\.\d{1,3}\b", chunk))
    if comma_dec == 0 and dot_dec == 0:
        return None
    return "," if comma_dec >= dot_dec else "."


def suggest_skip(sample_text: str, delim: Optional[str]) -> int:
    if not delim:
        return 0
    lines = [ln for ln in sample_text.splitlines() if ln.strip()][:10]
    if len(lines) < 2:
        return 0
    counts = [ln.count(delim) for ln in lines]
    max_count = max(counts)
    if max_count == 0:
        return 0
    # Cerca la prima riga che ha almeno meta' del massimo conteggio
    # (salta righe di titolo/descrizione che hanno pochi delimitatori)
    threshold = max(max_count * 0.5, 1.0)
    for idx, c in enumerate(counts):
        if c >= threshold:
            return idx
    return 0
