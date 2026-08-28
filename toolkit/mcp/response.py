"""Response shaping utilities for MCP tools.

Riduce il rumore nelle risposte MCP per AI agenti:
- strip_nulls: rimuove chiavi con valore None/empty
- compact: mantiene solo le chiavi specificate

Queste utility operano sul layer MCP, NON sul backend condiviso.
CLI resta invariata.
"""

from __future__ import annotations

from typing import Any


def strip_nulls(d: dict[str, Any]) -> dict[str, Any]:
    """Rimuove ricorsivamente chiavi con valore None da un dict.

    Non tocca list, int, str, bool — solo None.
    Per nested dict, applica ricorsivamente.

    Esempio::

        strip_nulls({"a": 1, "b": None, "c": {"d": None, "e": "ok"}})
        # → {"a": 1, "c": {"e": "ok"}}
    """
    result: dict[str, Any] = {}
    for k, v in d.items():
        if v is None:
            continue
        if isinstance(v, dict):
            nested = strip_nulls(v)
            if nested:  # non aggiungere dict vuoti
                result[k] = nested
        else:
            result[k] = v
    return result


def compact(d: dict[str, Any], keep: list[str]) -> dict[str, Any]:
    """Mantiene solo le chiavi presenti in *keep*.

    Utile per ridurre response verbose a campi essenziali.

    Esempio::

        compact({"a": 1, "b": 2, "c": 3}, keep=["a", "c"])
        # → {"a": 1, "c": 3}
    """
    return {k: v for k, v in d.items() if k in keep}


def strip_empty_lists(d: dict[str, Any]) -> dict[str, Any]:
    """Rimuove ricorsivamente chiavi con lista vuota [].

    Utile per response con campi come ``matched_columns: []`` o
    ``mart_refs: []`` quando non ci sono elementi.
    """
    result: dict[str, Any] = {}
    for k, v in d.items():
        if v == []:
            continue
        if isinstance(v, dict):
            nested = strip_empty_lists(v)
            if nested:
                result[k] = nested
        elif isinstance(v, list):
            result[k] = v
        else:
            result[k] = v
    return result


def shape(
    d: dict[str, Any], *, strip_none: bool = True, strip_empty: bool = True
) -> dict[str, Any]:
    """Pipeline completa di shaping: strip_nulls + strip_empty_lists.

    Default: applica entrambi. Usare ``shape(result)`` come ultimo
    step prima del return in ogni tool MCP.
    """
    if strip_none:
        d = strip_nulls(d)
    if strip_empty:
        d = strip_empty_lists(d)
    return d
