# ADR-003: Config Pydantic con migrazione graduale da dict

**Status:** implemented (2026-04), bridge `_compat_*` rimosso (2026-05)

## Contesto

Il toolkit legge `dataset.yml` per sapere dove trovare i dati, come trasformarli
e dove salvare l'output. La configurazione si è evoluta nel tempo: da pochi campi
a ~20 sezioni annidate con tipi specifici (Path, liste, enum, bool da stringa).

Serviva un modo per validare il YAML all'ingresso con errori espliciti,
mantenendo la compatibilità con le config esistenti in `dataset-incubator`.

## Decisione

**Fase 1 (v1.0, 2026-02):** Pydantic v2 per il parsing + bridge `_compat_*` per
convertire i modelli in dict, mantenendo tutta la pipeline downstream su dict.

```
YAML → Pydantic models → _compat_* → dict[str, Any] → pipeline
```

**Fase 2 (v1.2, 2026-05):** Sostituito bridge con `_CompatModel` wrapper che
supporta sia accesso tipizzato (`cfg.raw.sources`) che dict-style
(`cfg.raw.get("sources")`), consentendo migrazione graduale dei consumatori.

```
YAML → Pydantic models → _CompatModel wrapper → pipeline
                                        ↓
                              dict-style .get() per retrocompat
```

## Conseguenze

**Positive:**
- Errori di configurazione espliciti e leggibili (DCL001-DCL013)
- Campi legacy rifiutati con messaggio chiaro invece di warning ignorati
- Type checking progressivo (mypy ora rileva accessi a campi inesistenti)
- `_CompatModel.__eq__` confronta automaticamente con dict per test retrocompat

**Negative:**
- Complessità del wrapper `_CompatModel` (mantenere due interfacce sul mismo oggetto)
- `isinstance(x, dict)` nei consumatori non funziona più — richiesto refactor
- `exclude_unset=True` significa che campi non configurati non appaiono in model_dump
- Doppia manutenzione finché tutti i consumatori non migrano ad accesso tipizzato

**Status attuale:** tutti i consumatori interni del toolkit migrati. Dataset-incubator
usa ancora l'interfaccia dict (compatibile via `_CompatModel.get()`).
