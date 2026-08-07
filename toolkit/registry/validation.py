"""Validazione degli artifact registry contro gli schemi condivisi.

Gli schemi vivono in ``toolkit/registry/schemas/*.schema.json`` (package-data,
vedi pyproject). Validatori standalone (jsonschema) — nessun ``$ref`` esterno:
le definizioni condivise (es. blocco ``run``) sono inline in ogni schema, con
``run.schema.json`` come fonte canonica.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

SCHEMAS_DIR = Path(__file__).resolve().parent / "schemas"


def load_schema(schema_name: str) -> dict[str, Any]:
    """Carica uno schema condiviso per nome file (es. ``clean_catalog.schema.json``)."""
    path = SCHEMAS_DIR / schema_name
    if not path.is_file():
        raise FileNotFoundError(f"Schema non trovato in {SCHEMAS_DIR}: {schema_name}")
    return json.loads(path.read_text(encoding="utf-8"))


def validate_artifact(instance: dict[str, Any], schema_name: str) -> list[str]:
    """Valida un artifact contro lo schema condiviso.

    Returns:
        Lista di errori (stringhe leggibili). Vuota se conforme.
    """
    import jsonschema

    schema = load_schema(schema_name)
    try:
        if schema.get("$schema", "").endswith("/draft-07/schema#"):
            validator_cls = jsonschema.Draft7Validator
        else:
            validator_cls = jsonschema.Draft202012Validator
        validator = validator_cls(schema)
    except Exception as exc:
        return [f"schema invalido {schema_name}: {exc}"]

    errors = []
    for err in sorted(validator.iter_errors(instance), key=lambda e: list(e.path)):
        where = "/".join(str(p) for p in err.path) or "<root>"
        errors.append(f"{where}: {err.message}")
    return errors
