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
