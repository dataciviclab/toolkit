"""Parser strutturato di macros.sql per il contratto pipeline.

Legge il file SQL delle macro standard e restituisce una lista di dict
con nome, firma, descrizione e annotazioni @contract.

L'annotazione ``@contract`` nei commenti permette di arricchire la macro
con metadati machine-readable senza inquinare il SQL:

    -- @contract returns: DOUBLE
    -- @contract warning: NON usare se decimal=',' e' configurato...
    -- @contract see: altra_macro
    -- @contract example: nome(param) AS alias
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

_MACROS_SQL_PATH = Path(__file__).parent.parent / "sql" / "macros.sql"

# Regex: commento separatore "── nome_macro ── ..."
_RE_MACRO_HEADER = re.compile(r"^--\s+──\s+(\w+)\s+─")

# Regex: dichiarazione CREATE OR REPLACE MACRO name(params)
_RE_MACRO_DECL = re.compile(r"CREATE\s+OR\s+REPLACE\s+MACRO\s+(\w+)\s*\(([^)]*)\)", re.IGNORECASE)

# Regex: annotazione @contract in un commento
_RE_CONTRACT_ANNOT = re.compile(r"^--\s+@contract\s+(\w+)\s*:\s*(.+)$")


def _parse_params(param_str: str) -> list[str]:
    """Estrae i nomi parametri da una stringa 'val, yes_value'."""
    if not param_str or not param_str.strip():
        return []
    return [p.strip() for p in param_str.split(",") if p.strip()]


def _build_example(name: str, params: list[str]) -> str:
    """Genera un example di default se non esplicitamente fornito."""
    args = ", ".join(f'"{p}"' if p in ("val", "col", "value") else p for p in params)
    return f"{name}({args}) AS {name}_result"


def read_macros(sql_path: Path | None = None) -> list[dict[str, Any]]:
    """Parsa macros.sql e restituisce la lista strutturata delle macro.

    Cerca pattern:
      - blocco di commenti con "-- ── nome ──..." come header
      - righe "-- <descrizione>" consecutive
      - righe "-- @contract <chiave>: <valore>" dentro il blocco
      - "CREATE OR REPLACE MACRO nome(params) AS" subito dopo

    Returns:
        List[dict] con chiavi: name, signature, params[], returns,
        description, example, warning (opzionale), see (opzionale).
    """
    path = sql_path or _MACROS_SQL_PATH
    if not path.exists():
        raise FileNotFoundError(f"Macros SQL file not found: {path}")

    lines = path.read_text(encoding="utf-8").splitlines()
    macros: list[dict[str, Any]] = []
    i = 0

    while i < len(lines):
        line = lines[i]

        # Cerca header "── nome_macro ──"
        m = _RE_MACRO_HEADER.match(line)
        if not m:
            i += 1
            continue

        name = m.group(1)

        # Raccogli il blocco commento tra l'header e la dichiarazione SQL
        description_lines: list[str] = []
        contract_annots: dict[str, str] = {}
        last_contract_key: str | None = None
        j = i + 1
        while j < len(lines) and lines[j].startswith("--"):
            # Annotazione @contract
            cm = _RE_CONTRACT_ANNOT.match(lines[j])
            if cm:
                last_contract_key = cm.group(1)
                contract_annots[last_contract_key] = cm.group(2).strip()
            elif last_contract_key and lines[j].startswith("--   "):
                # Continuazione multi-riga di una annotazione @contract
                # (indentata con 3 spazi dopo "--")
                continuation = re.sub(r"^--\s{3}", "", lines[j]).strip()
                if continuation:
                    contract_annots[last_contract_key] += " " + continuation
            else:
                # Descrizione umana: toglie "-- " iniziale
                desc = re.sub(r"^--\s?", "", lines[j])
                if desc.strip():
                    description_lines.append(desc.strip())
                last_contract_key = None
            j += 1

        # j ora punta alla riga con CREATE OR REPLACE MACRO
        if j >= len(lines):
            i = j
            continue

        decl_match = _RE_MACRO_DECL.match(lines[j])
        if not decl_match:
            # Prova a concatenare con la riga successiva (multi-riga)
            if j + 1 < len(lines):
                joined = lines[j] + " " + lines[j + 1]
                decl_match = _RE_MACRO_DECL.match(joined)
        if not decl_match:
            i = j
            continue

        decl_name = decl_match.group(1)
        if decl_name != name:
            # Il nome nel commento non matcha la dichiarazione → skip
            i = j
            continue

        params_str = decl_match.group(2)
        params = _parse_params(params_str)
        signature = f"{name}({', '.join(params)})" if params else f"{name}()"

        returns = contract_annots.pop("returns", None)
        warning = contract_annots.pop("warning", None)
        see = contract_annots.pop("see", None)
        example = contract_annots.pop("example", None)

        # Descrizione: unisci righe, limita a 120 caratteri per la prima riga
        description = " ".join(description_lines) if description_lines else ""
        if len(description) > 120:
            description = description[:117] + "..."

        # Example di default se non specificato
        if not example:
            example = _build_example(name, params)

        macro: dict[str, Any] = {
            "name": name,
            "signature": signature,
            "params": params,
            "returns": returns or "VARCHAR",  # default DuckDB macro
            "description": description,
            "example": example,
        }
        if warning:
            macro["warning"] = warning
        if see:
            macro["see"] = see

        macros.append(macro)

        # Salta alla prossima macro
        if j + 1 < len(lines) and not lines[j + 1].startswith("--"):
            j += 1  # riga implementazione
        i = j + 1

    return macros


def macro_names(sql_path: Path | None = None) -> set[str]:
    """Restituisce i nomi delle macro presenti in macros.sql.

    Utility per test — evita di rieseguire la logica di parsing completa.
    """
    return {m["name"] for m in read_macros(sql_path)}
