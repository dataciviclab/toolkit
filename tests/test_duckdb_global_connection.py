"""Policy: il toolkit non scrive sulla connessione duckdb globale.

policy: `duckdb.sql()` usa la connessione globale condivisa e lascia il
risultato in sospeso, così il chiamante successivo riceve
``ResourceInUseException: connection has an unfinished result``. Il contratto
del repo è `safe_connect()`: connessione dedicata, chiusa a fine blocco.

Il sintomo non è riproducibile in locale — la relazione temporanea viene
rilasciata dal refcount — quindi un test che esegue due query di fila passa
anche sul codice bacato. Qui si protegge l'invariante sul sorgente, via AST:
deterministico, immune ai commenti che citano la funzione, e valido su tutto
il toolkit e non solo sul modulo che si è rotto.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

TOOLKIT = Path(__file__).resolve().parent.parent / "toolkit"

pytestmark = pytest.mark.policy


def _usa_connessione_globale(path: Path) -> bool:
    """True se il file chiama ``duckdb.sql(...)`` come codice, non in un commento."""
    albero = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for nodo in ast.walk(albero):
        if not isinstance(nodo, ast.Call):
            continue
        funzione = nodo.func
        if (
            isinstance(funzione, ast.Attribute)
            and funzione.attr == "sql"
            and isinstance(funzione.value, ast.Name)
            and funzione.value.id == "duckdb"
        ):
            return True
    return False


def test_nessun_uso_della_connessione_duckdb_globale():
    hits = sorted(
        str(path.relative_to(TOOLKIT.parent))
        for path in TOOLKIT.rglob("*.py")
        if _usa_connessione_globale(path)
    )

    assert hits == [], (
        f"connessione duckdb globale usata in {hits}: usare safe_connect() da lab_connectors.duckdb"
    )
