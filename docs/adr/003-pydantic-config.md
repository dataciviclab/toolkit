# ADR-003: Config Pydantic con migrazione graduale da dict

**Status:** superseded (2026-07-30) — sostituito da dataclass semplici (PR #435)

## Contesto

Il toolkit legge `dataset.yml` per sapere dove trovare i dati, come trasformarli
e dove salvare l'output. La configurazione si è evoluta nel tempo: da pochi campi
a ~20 sezioni annidate con tipi specifici (Path, liste, enum, bool da stringa).

Serviva un modo per validare il YAML all'ingresso con errori espliciti,
mantenendo la compatibilità con le config esistenti in `dataset-incubator`.

## Decisione originale

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

## Conseguenze (all'epoca)

- Errori di configurazione espliciti e leggibili
- Campi legacy rifiutati con messaggio chiaro
- Type checking progressivo (mypy)
- Complessità del wrapper `_CompatModel`
- `isinstance(x, dict)` non funzionava più
- Doppia manutenzione per interfaccia dict

## Perché è stato superseded

L'architettura Pydantic + `_CompatModel` introduceva complessità sproporzionata
per il beneficio. Il wrapper `_CompatModel` doveva mantenere due interfacce
sullo stesso oggetto, e i consumatori downstream (soprattutto dataset-incubator)
continuavano a usare l'interfaccia dict.

**PR #435 (2026-07-30):** 24 modelli Pydantic → 1 dataclass `PipelineConfig`.

| Componente | Prima | Dopo |
|---|---|---|
| Modelli | 24 Pydantic models | 1 dataclass (`PipelineConfig`) |
| Parsing | Pydantic v2 | `yaml.safe_load` + validazione inline |
| Wrapper | `_CompatModel` (dict access) | Nessuno — dataclass pura |
| Righe nette | ~1.200 | ~300 (-1.089) |
| `isinstance(x, dict)` | Non funzionava | Funziona (`PipelineConfig` è un oggetto normale) |
| Type checking | mypy su Pydantic generics | mypy su dataclass semplici |

La validazione è ora inline in `load_config()`: errori espliciti
(es. "Sezione 'dataset' mancante") senza codici DCL né schema Pydantic.

## Lezioni apprese

- Pydantic è overengineering per un carico di configurazione stabile (< 20 sezioni).
- Un wrapper di compatibilità raddoppia la superficie di manutenzione.
- La validazione inline con messaggi espliciti è più facile da debuggare
  rispetto a errori Pydantic generici.
- Le dataclass sono sufficienti quando il modello è conosciuto a compile-time.
