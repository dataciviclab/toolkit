## Sintesi

Descrivi in poche righe cosa cambia e perché.

## Contesto collegato

Closes #

## Cosa cambia

- [ ] Bug fix
- [ ] Nuova funzionalità del motore
- [ ] Nuovo plugin sorgente
- [ ] Modifica contratto pubblico (dataset.yml, path output, schema parquet)
- [ ] Refactor / performance
- [ ] Documentazione
- [ ] Dipendenze o CI

## Impatto su contratti pubblici

Se modifichi un contratto pubblico, segna cosa impatti.

- [ ] Struttura `dataset.yml` (nuovo campo, cambio obbligatorietà)
- [ ] Path output (nuovo layer, cambio percorso artifact)
- [ ] Schema parquet (nuova colonna, rename, cambio tipo)
- [ ] CLI o MCP tool (nuovo comando, cambio parametro)
- [ ] API pubblica del toolkit (firma funzione, classe, eccezione)

> Se segnato, hai aggiornato downstream? [ ] `dataset-incubator` — [ ] `docs/`

## Verifica

Spiega come hai verificato il cambiamento.

```bash
# Esempi: comandi usati per testare
pytest -m core -x --tb=short
ruff check .
mypy toolkit/
```

- [ ] `pytest -m core` passa
- [ ] `ruff check .` passa
- [ ] `mypy toolkit/` passa (o motiva le eccezioni)
- [ ] Modificato o aggiunto test con marker appropriato (`contract` / `policy` / `regression` / `adapter` / `pure_unit` / `smoke`)

## Checklist PR

- [ ] Perimetro stretto: una PR = un layer o un fix mirato
- [ ] Se nuovo plugin: test + docs inclusi
- [ ] Issue collegata o motivazione dell'assenza
- [ ] **Se rimuovo un modulo/funzione pubblica**: ho verificato l'assenza di import con `rg` su tutta l'org
  e lasciato shim backward compat con `DeprecationWarning` (vedi deprecation policy in CONTRIBUTING)

## Note per chi revisiona

Rischi, limiti, punti da controllare con attenzione.
