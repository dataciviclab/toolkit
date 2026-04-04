# Toolkit Conventions

Questa pagina raccoglie i contratti operativi stabili del toolkit per la pipeline `RAW -> CLEAN -> MART`.

## Paths

- I path relativi dichiarati in `dataset.yml` sono risolti rispetto alla directory che contiene il file `dataset.yml`.
- Questo vale per `root`, `raw.*.args.path`, `clean.sql`, `mart.tables[].sql` e gli altri campi path whitelisted dal loader.

## RAW Manifest

- Il layer RAW scrive sempre `manifest.json` nella directory `raw/<dataset>/<year>/`.
- Il manifest include sempre `primary_output_file`, che rappresenta il file RAW canonico da usare a valle.
- `primary_output_file` e `sources[].output_file` sono path relativi al year-dir RAW, in formato POSIX.
- Lo schema del manifest e` cross-layer: RAW lo produce, CLEAN lo legge.

## Profile Artifacts

- Gli artefatti di profiling vivono in `raw/<dataset>/<year>/_profile/`.
- Il file JSON canonico e` `raw_profile.json`, ma viene scritto solo per policy `standard|debug`.
- `profile.json` resta un alias di compatibilita` opzionale, controllato da `output.legacy_aliases`.
- `suggested_read.yml` e` il contratto usato da CLEAN per i format hints e resta richiesto solo quando `clean.read.source: auto`.
- `run raw` puo` scrivere un `suggested_read.yml` conservativo gia` nel percorso canonico.
- `profile raw` puo` rigenerare lo stesso file insieme ad artefatti diagnostici piu` ricchi.
- `suggested_mapping.yml` resta un artefatto diagnostico opzionale per uso umano; non e` un input del runtime canonico del toolkit.

## Artifacts Policy

| Policy | Keep | Drop |
|---|---|---|
| `minimal` | file primario RAW, parquet finali, `metadata.json`, `manifest.json`, `*_validation.json`, `suggested_read.yml` solo con `clean.read.source: auto` | `raw_profile.json`, `profile.json`, `profile.md`, `suggested_mapping.yml`, `_run/*.sql` |
| `standard` | tutto il required + `raw_profile.json` + `_run/*.sql` | `profile.md`, `suggested_mapping.yml`; `profile.json` solo se `output.legacy_aliases: true` |
| `debug` | tutti gli artefatti, inclusi report e alias legacy | nulla |

- Config canonica:

```yaml
output:
  artifacts: standard
  legacy_aliases: true
```

## CLEAN Input

- CLEAN usa una policy manifest-first: se `manifest.json` contiene un `primary_output_file` valido, quello ha precedenza.
- Se il manifest manca, e` incompleto o punta a un file non valido, CLEAN usa il fallback legacy di selezione e logga un warning.
- Il fallback supporta `explicit`, `latest`, `largest`, `all`.

## Read Config

- La forma canonica e`:

```yaml
clean:
  read:
    source: auto  # oppure config_only
```

- L'alias scalare `clean.read: auto|config_only` e` ancora supportato ma deprecato.

## Read Precedence

- La configurazione finale del reader CLEAN segue sempre questo ordine:
  `defaults -> suggested(format-only) -> config_overrides`
- I suggerimenti letti da `_profile/suggested_read.yml` sono filtrati alle sole chiavi di formato.

## Read Robustness

- `clean.read_mode` governa la strategia di lettura:
  - `strict`
  - `fallback`
  - `robust`
- I suggested hints non forzano mai il preset robusto.
- Il preset robusto viene applicato solo se richiesto da `clean.read_mode` o dal fallback runtime del reader.

## Positional Fixed Schema

- `clean.read.columns` dichiara uno schema canonico per il reader CLEAN.
- `clean.read.normalize_rows_to_columns: true` attiva una lettura CSV normalizzata lato toolkit:
  - richiede `clean.read.columns`
  - salta l'header se `header: true`
  - pad-da a destra le righe piu corte fino al numero di colonne atteso
  - fallisce se una riga ha piu colonne di quelle dichiarate

Usarlo quando:

- stai leggendo CSV pubblici multi-anno con schema quasi stabile ma non identico
- vuoi mantenere un mapping posizionale unico nel `clean.sql`
- alcune annualita hanno colonne finali assenti o vuote

Non usarlo quando:

- il file ha gia uno schema per nome colonna stabile
- il problema e solo di delimitatore o quoting
- non hai deciso esplicitamente uno schema canonico

## Metadata

- Ogni `metadata.json` include `metadata_schema_version`.
- Il metadata CLEAN include anche i campi di audit `read_params_source`, `read_source_used`, `read_params_used`.

## Validation

- Ogni validation report JSON include `validation_schema_version`.
- I path di output restano:
  - RAW: `raw_validation.json`
  - CLEAN: `_validate/clean_validation.json`
  - MART: `_validate/mart_validation.json`

## ANAC Delta Ingestion Spike

Per i dataset ANAC con aggiornamenti incrementali, il toolkit oggi offre solo diagnostica e confronto tra layer, non un merge stateful canonico.

Punti gia' disponibili:

- `status` e `inspect` espongono i profili dei layer prodotti nei metadata
- `toolkit.core.layer_profile.compare_layer_profiles` calcola anche `row_count_delta`
- i report diagnostici mostrano `added_columns`, `removed_columns` e `type_changes`

Prima di introdurre un eventuale `delta_merge` servono verifiche sui dump reali:

- forma effettiva del payload ANAC/OCDS usato dal Lab
- chiave operativa per l'upsert, se esiste davvero una chiave stabile
- collocazione reale dei subappalti nel payload principale o in dataset separati
- regole di conflitto per record duplicati, rettifiche e annullamenti
- sufficienza di un eventuale `delta_state.json` solo filesystem-first

Stato raccomandato per adesso:

- tenere il tema come spike tecnico, non come feature canonica
- evitare nuovi campi di config finche' il modello non e' verificato su dati reali
- usare questo documento come base per il follow-up di implementazione solo dopo la validazione

## Fonti pubbliche italiane — quirks noti

Pattern ricorrenti su CSV e XLSX da portali pubblici italiani. Da considerare
prima di scrivere `clean.sql` e `clean.read`.

### Encoding

La maggior parte dei portali PA produce file in `cp1252` (Windows-1252), non UTF-8.
Dichiarare sempre l'encoding esplicitamente:

```yaml
clean:
  read:
    encoding: cp1252
```

Se non dichiarato e il file contiene caratteri accentati, il run fallisce o
produce artefatti silenziosi.

### ZIP con XLSX annidati

Alcune fonti (es. MEF/Finanze, ISTAT) distribuiscono un archivio ZIP che
contiene uno o piu XLSX. Il toolkit non estrae ZIP automaticamente: il RAW
extractor deve essere configurato per gestire il pattern ZIP → file interno.

Verificare con `toolkit scout-url <url>` se la sorgente e` un ZIP prima di
configurare l'extractor.

### Schema instabile tra annualita

I CSV multi-anno di fonti come IRPEF, AIFA o SIOPE cambiano spesso:
- colonne aggiunte o rimosse tra un anno e l'altro
- nomi colonna con varianti ortografiche (maiuscolo/minuscolo, spazi vs underscore)
- righe di intestazione o footer aggiuntive in alcuni anni

Usare `toolkit inspect schema-diff --config dataset.yml --json` per confrontare
i segnali RAW tra anni prima di scrivere il `clean.sql`.

Per CSV con schema posizionale quasi stabile, usare `normalize_rows_to_columns: true`
(vedi sezione Positional Fixed Schema).

### Colonne con nomi impliciti o posizionali

Alcuni CSV non hanno header o hanno un header non standard (riga 2, merged cell
da XLSX). Dichiarare:

```yaml
clean:
  read:
    header: false
    skip: 1       # righe da saltare prima dell'header reale
```

### Chiavi territoriali

Le chiavi geografiche nei dataset PA italiani non sono sempre ISTAT-standard:

- codici ISTAT comuni a 6 cifre vs 3+3 (provincia+comune)
- nomi comune con varianti storiche (fusioni, cambio denominazione)
- codici regione con offset legacy

Dichiarare i limiti noti nel `notes.md` del candidate e nel README di `analisi/`.
Non tentare di normalizzare le chiavi nel `clean.sql` senza documentare la
scelta esplicitamente.
