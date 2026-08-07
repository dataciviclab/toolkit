# ADR-005: Support dataset unificati — tipo, materializzazione e orchestrazione ensure

**Status:** accepted (2026-08)

## Contesto

Il toolkit gestisce già i "support" (dataset di riferimento per i join nei
clean.sql: anagrafiche, glossari, codelist) con un contratto minimo:

```yaml
support:
  - name: comuni
    config: "../../support_datasets/istat-elenco-comuni/dataset.yml"
    years: [2026]
```

Infrastruttura esistente su main: campo `support:` nel config model,
`core/support.py` (`resolve_support_payloads`, `check_support_path_drift`,
`flatten_support_template_ctx`), orchestrazione in `cmd_run.run_full` che
esegue il support **sempre** (run_year step=all per ogni entry/anno),
placeholder template `{support.NAME.mart}` / `{support.NAME.outputs}`.

Limiti emersi:
1. **Solo `type: dataset`** — le codelist SDMX (`eurostat/codelists/*.csv`) e i
   mapping locali (`dcl-bologna/mapping/*.csv`) non sono rappresentabili:
   non sono dataset runnabili, oggi sono file CSV referenziati nei SQL con
   path relativi/hardcoded e fragili.
2. **Nessun skip-if-exists** — a ogni `run full` il support viene rieseguito
   interamente (RAW+CLEAN+MART) anche se le sue tabelle sono già presenti e
   stabili (es. anagrafiche che cambiano una volta l'anno).
3. **Solo layer MART esposto** — `resolve_support_payloads` guarda
   `mart.tables` e `flatten_support_template_ctx` espone solo la **prima**
   tabella mart; il caso reale del lab (`inps-rdc-pdc` join su
   `mart_codici_catastali`, seconda tabella di `istat-elenco-comuni`) non è
   coperto dal placeholder, e manca l'accesso al layer CLEAN del support.

Alternative considerate:
- **Mantenere lo status quo** (run incondizionato, path hardcoded nei SQL,
  anti-drift solo warning): non risolve la fragilità dei path né il costo dei
  run; le codelist restano fuori contratto.
- **Support come dataset di prima classe con run sempre**: nessun guadagno
  sul costo; non rappresenta codelist/file.
- **Contratto `support` esteso + orchestrazione `ensure`** (scelta).

## Decisione

### 1. Contratto `support:` esteso con `type`

`type` è opzionale, default `dataset` (backward compatible — nessun config
esistente si rompe):

```yaml
support:
  - name: comuni
    type: dataset              # materializza: run del config (se mancano output)
    config: "../../support_datasets/istat-elenco-comuni/dataset.yml"
    years: [2026]
  - name: geo
    type: codelist             # materializza: fetch (SDMX) → parquet canonico
    provider: sdmx
    agency: ESTAT
    id: GEO
  - name: quartieri
    type: file                 # materializza: exec command (se dichiarato)
    path: "mapping/colonnine-quartieri.csv"
    command: "python mapping/colonnine_quartieri.py"   # opzionale
```

Validazione per tipo:
- `dataset` → richiede `config` + `years` (come oggi)
- `codelist` → richiede `provider`/`agency`/`id` (o `endpoint`); il fetch è
  quello già presente in `SdmxSource.fetch_codelist` (profilo ESTAT, #450)
- `file` → richiede `path` (relativo al root del candidate, normalizzato);
  `command` opzionale per la rigenerazione

### 2. Orchestrazione `ensure` (skip-if-exists + materializza-se-manca)

Sostituisce il blocco "Process support datasets" in `cmd_run.run_full`:

```
per ogni support entry:
    output_attesi = support_output_paths(entry, year)
        dataset  → clean parquet + tutte le tabelle mart (per anno)
        codelist → out/data/support/{name}/{name}.parquet
        file     → path dichiarato
    se tutti gli output esistono  e  non --refresh-support  → SKIP (log reuse)
    se mancano:
        dataset  → run_year(support_cfg, year, step="all")   # invariato
        codelist → fetch_codelist + scrittura parquet canonico
        file     → exec command (se presente), altrimenti errore esplicito
    (smoke: materializza su {root}/smoke — pattern già esistente)
```

Proprietà:
- **Skip per-anno** (un support con anni [2026] già materializzato non viene
  rieseguito; un anno nuovo scatena il run solo per quell'anno)
- **Freshness baseline = esistenza** del file atteso; l'aggiornamento
  forzato è esplicito via `--refresh-support` (le codelist SDMX cambiano
  raramente — NUTS ~ogni 3 anni; il confronto hash/run-record è evoluzione
  futura)
- Un support fallito blocca il candidate (invariato)
- Dry-run: solo log, nessuna esecuzione

### 3. Output attesi = CLEAN + MART e set placeholder completo

`resolve_support_payloads` considera come output attesi sia il layer CLEAN
sia tutte le tabelle MART del support (altrimenti un support usato solo in
clean verrebbe considerato mancante). `flatten_support_template_ctx` espone:

| Placeholder | Risolve a |
|---|---|
| `{support.NAME.clean}` | parquet clean del support (per anno) |
| `{support.NAME.mart}` | prima tabella mart (backward compat) |
| `{support.NAME.mart.TABLE}` | tabella mart specifica (il caso `mart_codici_catastali`) |
| `{support.NAME.outputs}` | lista completa (invariato) |
| `{support.NAME.path}` | file materializzato (codelist/file) |

`check_support_path_drift` esteso ai nuovi placeholder
(`clean`, `mart.TABLE`, `path`).

### 4. Layer materializzazione codelist

Path canonico dedicato: `out/data/support/{name}/{name}.parquet` (senza anno:
le codelist non sono serie temporali, sono una fotografia al refresh).
Le codelist **non** sono dataset (niente raw/clean/mart, niente catalogo):
un layer separato evita confusione col catalogo clean. La migrazione dei
`codelists/*.csv` del repo eurostat è responsabilità del repo eurostat
(placeholder + anti-drift la impongono).

### 5. Ordine dei support

Sequenziale nell'ordine dichiarato (come oggi) + **validazione anti-ciclo**
(nessun support può riferire se stesso, direttamente o transitivamente).
L'ordinamento topologico completo è rimandato finché non esiste un caso reale
di support→support.

## Conseguenze

**Positive:**
- Codelist e mapping locali entrano nel contratto `dataset.yml` con
  materializzazione e path canonici → niente più path relativi/hardcoded
  nei SQL (l'anti-drift li segnala)
- Risparmio sui run: anagrafiche stabili non vengono rieseguite a ogni
  `run full`; skip per-anno
- Accesso al layer CLEAN del support e alle tabelle MART specifiche
  (risolve il caso reale `mart_codici_catastali`)
- Riusa l'infrastruttura esistente (`resolve_support_payloads`, template
  ctx, anti-drift, smoke handling) — nessuna riscrittura

**Negative:**
- Nuovo campo `type` nel contratto `support:` (default `dataset`, nessuna
  rottura, ma il contratto dataset.yml cresce)
- Materializzazione codelist richiede il profilo ESTAT/ISTAT già mergiato
  (#450) — le codelist SDMX non-ESTAT restano fuori finché non c'è un
  provider corrispondente
- `--refresh-support` forza la rigenerazione manualmente: la freshness
  automatica (hash/run-record) non è coperta

## Implementazione

1. `toolkit/core/config.py` — `type` nel modello support + validazione per
   tipo + `_normalize_paths`
2. `toolkit/core/support.py` — output attesi clean+mart, `support_output_paths`,
   `materialize_support` (per tipo), placeholder set esteso
3. `toolkit/cli/cmd_run.py` — blocco ensure (skip-if-exists + materialize),
   flag `--refresh-support`
4. `toolkit/core/sql_validation.py` — anti-drift esteso
5. `toolkit/domain/path_resolver.py` — payload support per i nuovi tipi
6. Test: estensione `test_support.py` + nuovi (skip, codelist, file, placeholder)
7. Pilota: `inps-rdc-pdc` → `{support.comuni.mart.mart_codici_catastali}`
