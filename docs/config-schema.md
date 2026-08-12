# Config Schema

Questa pagina descrive il contratto effettivo di `dataset.yml` dopo l'introduzione del modello tipizzato.

I path relativi sono sempre risolti rispetto alla directory che contiene `dataset.yml`.

## Top-level

| Campo | Tipo | Obbligatorio | Note |
|---|---|---|---|
| `schema_version` | `int` | no | default `1` |
| `root` | `string \| null` | no | se assente: `DCL_ROOT`, altrimenti directory del `dataset.yml` |
| `dataset` | `object` | si | include `name`, `years` |
| `raw` | `object` | no | configurazione acquisizione RAW |
| `clean` | `object` | no | configurazione CLEAN |
| `mart` | `object` | no | configurazione MART |
| `cross_year` | `object` | no | ⛔ rimosso — usa `mart.tables[].years` |
| `config` | `object` | no | policy parser config |
| `validation` | `object` | no | solo opzioni globali del validation gate |
| `output` | `object` | no | policy artefatti |
| `duckdb` | `object` | no | configurazione motore DuckDB (es. `memory_limit`) |

## dataset

| Campo | Tipo | Default |
|---|---|---|
| `dataset.name` | `string` | nessuno |
| `dataset.years` | `list[int]` | nessuno |
| `dataset.tags` | `list[string]` | `[]` |
| `dataset.category` | `string \| null` | `null` |
| `dataset.time_coverage` | `{start_year: int, end_year: int} \| null` | `null` |

`time_coverage` dichiara l'intervallo temporale del contenuto (non degli anni di
run). Esposto dal `dataset_loader` (manifest) e dal layer tool per la
catalogazione; usato dai candidate per serie storiche (es. `ga-sentenze`:
2023-2026). Opzionale ma raccomandato per serie storiche.

## raw

| Campo | Tipo | Default |
|---|---|---|
| `raw.output_policy` | `overwrite \| versioned` | `versioned` |
| `raw.extractor.type` | `identity \| unzip_all \| unzip_first \| unzip_first_csv` | `identity` |
| `raw.extractor.args` | `object` | `{}` |
| `raw.sources` | `list[RawSource]` | `[]` |

`RawSource`:

| Campo | Tipo | Default |
|---|---|---|
| `name` | `string \| null` | `null` |
| `type` | `string` | `http_file` |
| `client` | `ClientConfig` | `{}` |
| `args` | `object` | `{}` |
| `extractor` | `object \| null` | `null` |
| `primary` | `bool` | `false` |

`ClientConfig` shape minima:

| Campo | Tipo | Default |
|---|---|---|
| `timeout` | `int \| null` | `null` |
| `retries` | `int \| null` | `null` |
| `user_agent` | `string \| null` | `null` |
| `headers` | `dict[string,string] \| null` | `null` |

`raw.sources[].args` e `raw.extractor.args` devono essere sempre oggetti YAML, non liste o stringhe.

Esempio `ckan`:

```yaml
raw:
  sources:
    - name: bdap_lea
      type: ckan
      client:
        timeout: 60
        retries: 2
      args:
        portal_url: https://bdap-opendata.rgs.mef.gov.it/SpodCkanApi/api/3
        dataset_id: "d598ebd9-949d-4214-bb33-cd9c1be08f15"
        resource_id: "33344"
```

Note pratiche per `ckan`:

- il toolkit interroga `resource_show` prima del download
- se `resource_show` non e disponibile o non risolve il file, il toolkit ripiega su `package_show`
- se il portale restituisce un file URL in `http://`, il toolkit lo forza automaticamente a `https://`
- se `filename` non e dichiarato, il toolkit prova a inferire l'estensione dall'URL risolto

Esempio `sdmx`:

```yaml
raw:
  sources:
    - name: popolazione_residente
      type: sdmx
      client:
        timeout: 60
        retries: 2
      args:
        agency: IT1
        flow: 22_289
        version: "1.5"
        filters:
          FREQ: A
          REF_AREA: "001001"
          DATA_TYPE: JAN
          SEX: "9"
          AGE: TOTAL
          MARITAL_STATUS: "99"
```

Note pratiche per `sdmx`:

- la `version` e' obbligatoria e deve coincidere con la versione corrente esposta dal dataflow
- non esiste fallback silenzioso a `latest`
- in v1 i `filters` sono supportati solo sulle dimensioni di serie, non su `TIME_PERIOD`
- il filtro temporale va applicato nel layer `clean.sql` (per esempio `WHERE TIME_PERIOD = '2024'`), non in `raw.sources[].args.filters`
- il plugin restituisce un CSV normalizzato con colonne `DIM`, `DIM_label` e `value`

Esempio `http_post_file`:

```yaml
raw:
  sources:
    - name: pensioni_dag
      type: http_post_file
      client:
        timeout: 120
        retries: 2
      args:
        url: "https://datipensioni.mef.gov.it/datipensioni/downloadFile"
        post_data:
          filename: "Dati_Tipo_Pensione_totale.csv"
          categoria: "pensioni"
        filename: "Dati_Tipo_Pensione_totale.csv"
      primary: true
```

Note pratiche per `http_post_file`:

- i parametri del form POST si dichiarano in `args.post_data` come coppie chiave-valore
- il retry è sicuro solo per endpoint idempotenti (download file): per POST non-idempotenti impostare `client.retries: 0`
- il resto del comportamento (infer estensione, extractor, caching) è identico a `http_file`
## clean

| Campo | Tipo | Default |
|---|---|---|
| `clean.sql` | `string` | nessuno | path al file SQL (usa le macro standard — vedi [standard-macros.md](standard-macros.md)) |
| `clean.read_mode` | `strict \| fallback \| robust` | `fallback` |
| `clean.read_source` | `auto \| config_only \| null` | `null` |
| `clean.read` | `CleanRead \| null` | `null` |
| `clean.required_columns` | `list[str]` | `[]` |
| `clean.validate` | `CleanValidate` | `{}` |

`CleanRead`:

| Campo | Tipo | Default |
|---|---|---|
| `source` | `auto \| config_only` | `auto` |
| `delim` | `string \| null` | `null` |
| `header` | `bool` | `true` |
| `encoding` | `string \| null` | `null` |
| `decimal` | `string \| null` | `null` |
| `skip` | `int \| null` | `null` |
| `auto_detect` | `bool \| null` | `null` |
| `quote` | `string \| null` | `null` |
| `escape` | `string \| null` | `null` |
| `comment` | `string \| null` | `null` |
| `ignore_errors` | `bool \| null` | `null` |
| `strict_mode` | `bool \| null` | `null` |
| `null_padding` | `bool \| null` | `null` |
| `parallel` | `bool \| null` | `null` |
| `nullstr` | `string \| list[string] \| null` | `null` |
| `columns` | `dict[string,string] \| null` | `null` |
| `normalize_rows_to_columns` | `bool` | `false` |
| `align_by_header` | `bool` | `false` |
| `trim_whitespace` | `bool` | `true` |
| `sample_size` | `int \| null` | `null` |
| `sheet_name` | `string \| int \| null` | `null` |
| `mode` | `explicit \| latest \| largest \| all \| null` | `latest`¹ |
| `glob` | `string` | `*` |
| `prefer_from_raw_run` | `bool` | `true` |
| `allow_ambiguous` | `bool` | `false` |
| `include` | `list[string] \| null` | `null` |

¹ Il default runtime è `latest` (file raw più recente). Se `include` è specificato e `mode` è omesso, il default diventa `explicit`. Precedenza: `explicit` > `include`, `latest` > altrimenti.

Note pratiche:

- **Macro SQL standard**: il toolkit carica automaticamente 8 macro DuckDB
  (`normalize_string`, `cast_int`, `cast_bigint`, `cast_double`,
  `normalize_italian_number`, `normalize_italian_integer`, `decode_flag`,
  `remove_dot_thousands`). Possono essere usate in qualsiasi `clean.sql`
  senza importazioni né configurazioni. Dettaglio: [standard-macros.md](standard-macros.md).
- i file `.xlsx` sono supportati nel layer CLEAN via `engine="openpyxl"`
- i file `.xls` (Excel 97-2003) sono supportati via `engine="xlrd"`
- RAW conserva il workbook originale senza convertirlo
- per Excel le opzioni principali sono `header`, `skip`, `columns`, `trim_whitespace`, `sheet_name`
- `sheet_name` (stringa o intero): seleziona il foglio da leggere; default = primo foglio (`0`)
- `mode: all` consente di leggere tutti i fogli di un workbook in una vista unica (`raw_input`)
- con `header: false` + `columns` è possibile specificare i nomi-colonna manualmente per file Excel senza intestazione
- `normalize_rows_to_columns: true` ha senso solo insieme a `columns`
- con `normalize_rows_to_columns: true`, il toolkit normalizza le righe corte del CSV allo schema atteso prima di esporre `raw_input`
- `align_by_header: true` (insieme a `normalize_rows_to_columns: true`) allinea le righe per nome colonna invece che per posizione: colonne attese ma non presenti nell'header vengono riempite con stringa vuota; colonne extra nel CSV vengono ignorate; colonne in ordine diverso vengono riallineate. Richiede `header: true`.

`CleanValidate`:

| Campo | Tipo | Default |
|---|---|---|
| `primary_key` | `list[str]` | `[]` |
| `not_null` | `list[str]` | `[]` |
| `ranges` | `dict[str, RangeRule]` | `{}` |
| `max_null_pct` | `dict[str, float]` | `{}` |
| `min_rows` | `int \| null` | `null` |

`RangeRule`:

| Campo | Tipo | Default |
|---|---|---|
| `min` | `float \| null` | `null` |
| `max` | `float \| null` | `null` |

## mart

| Campo | Tipo | Default |
|---|---|---|
| `mart.tables` | `list[MartTable]` | `[]` |
| `mart.required_tables` | `list[str]` | `[]` |
| `mart.validate` | `MartValidate` | `{}` |

`MartTable`:

| Campo | Tipo | Default |
|---|---|---|
| `name` | `string` | nessuno |
| `sql` | `string` | nessuno |

`MartValidate`:

| Campo | Tipo | Default |
|---|---|---|
| `table_rules` | `dict[str, MartTableRuleConfig]` | `{}` |

`MartTableRuleConfig`:

| Campo | Tipo | Default |
|---|---|---|
| `required_columns` | `list[str]` | `[]` |
| `not_null` | `list[str]` | `[]` |
| `primary_key` | `list[str]` | `[]` |
| `ranges` | `dict[str, RangeRule]` | `{}` |
| `min_rows` | `int \| null` | `null` |

Note pratiche:

- `clean.validate` controlla il parquet CLEAN complessivo del dataset/anno
- `mart.validate.table_rules` controlla invece ogni tabella MART per nome
- le chiavi di `table_rules` devono corrispondere ai `name` dichiarati in `mart.tables`
- se una regola punta a una tabella non dichiarata, il validator la segnala come orphan rule

Esempio minimo corretto:

```yaml
mart:
  tables:
    - name: mart_summary
      sql: sql/mart/mart_summary.sql
  required_tables:
    - mart_summary
  validate:
    table_rules:
      mart_summary:
        min_rows: 1
```

Esempio tipico con piu' vincoli su una tabella:

```yaml
mart:
  tables:
    - name: mart_summary
      sql: sql/mart/mart_summary.sql
  required_tables:
    - mart_summary
  validate:
    table_rules:
      mart_summary:
        required_columns:
          - anno
          - totale
        not_null:
          - anno
          - totale
        primary_key:
          - anno
        ranges:
          totale:
            min: 0
        min_rows: 1
```

Esempio completo con due tabelle:

```yaml
clean:
  validate:
    primary_key:
      - anno
      - comune
    not_null:
      - anno

mart:
  tables:
    - name: mart_summary
      sql: sql/mart/mart_summary.sql
    - name: mart_detail
      sql: sql/mart/mart_detail.sql
  required_tables:
    - mart_summary
    - mart_detail
  validate:
    table_rules:
      mart_summary:
        required_columns:
          - anno
          - totale
        primary_key:
          - anno
        min_rows: 1
      mart_detail:
        required_columns:
          - anno
          - comune
        primary_key:
          - anno
          - comune
```

Errori comuni:

- mettere `required_columns`, `not_null` o `primary_key` direttamente sotto `mart.validate` invece che dentro `table_rules.<nome_tabella>`
- usare come chiave di `table_rules` un nome diverso da quello dichiarato in `mart.tables`
- aspettarsi che `clean.validate` valga automaticamente anche per le tabelle MART

## mart.tables[].years — output multi-anno (sostituisce cross_year)

Invece del modulo `cross_year` rimosso, le tabelle MART possono dichiarare `years` per aggregare dati da più anni:

```yaml
mart:
  tables:
    - name: clean_union
      sql: sql/multi_year/clean_union.sql
      years: [2022, 2023]
      source_layer: clean
```

Campi aggiuntivi su `MartTable` (oltre a `name` e `sql`):

| Campo | Tipo | Default |
|---|---|---|
| `years` | `list[int]` | `None` (usa anno corrente) |
| `source_layer` | `clean \| mart` | `clean` |
| `source_table` | `string \| null` | `null` |

Note:

- quando `years` è specificato, il runner unisce tutti i parquet degli anni indicati e li espone come view `clean_input`
- con `source_layer: mart`, `source_table` è obbligatorio; legge `<year>/<source_table>.parquet` da ogni anno
- gli output vengono scritti in `root/data/mart/<dataset>/<name>.parquet` (livello dataset, senza anno)
- la validazione multi-anno non produce un validation.json separato (la validazione è integrata in MART)

## support

Dataset / codelist / file di riferimento per i join nei SQL di clean e mart.
Vengono eseguiti (o riusati) **prima** del candidate; il risultato è esposto
al template SQL come placeholder `{support.NAME.*}` (vedi ADR-005).

| Campo | Tipo | Default |
|---|---|---|
| `support[].name` | `str` | — (obbligatorio, univoco) |
| `support[].type` | `dataset` \| `codelist` \| `file` | `dataset` |
| `support[].config` | path dataset.yml | — (solo `dataset`) |
| `support[].years` | `list[int]` | — (solo `dataset`) |
| `support[].id` | `str` | — (solo `codelist`) |
| `support[].agency` | `str` | `ESTAT` (solo `codelist`) |
| `support[].provider` | `str` | `sdmx` (solo `codelist`) |
| `support[].path` | `str` | — (solo `file`, relativo al root candidate) |
| `support[].command` | `str` | — (solo `file`, opzionale: rigenera il file) |

```yaml
support:
  - name: comuni
    type: dataset                 # materializza: run del config (skip se clean+mart presenti)
    config: "../../support_datasets/istat-elenco-comuni/dataset.yml"
    years: [2026]
  - name: geo
    type: codelist                # materializza: fetch_codelist → parquet canonico
    agency: ESTAT
    id: GEO
  - name: quartieri
    type: file                    # materializza: exec command se il file manca
    path: "mapping/colonnine-quartieri.csv"
    command: "python mapping/colonnine_quartieri.py"
```

**Orchestrazione (ensure):** gli output attesi per tipo sono — `dataset`:
parquet clean + tutte le tabelle mart (per anno); `codelist`:
`out/data/support/{name}/{name}.parquet`; `file`: il path dichiarato.
Se gli output sono già presenti il run li **riusa** (skip-if-exists, per-anno);
la rigenerazione forzata è `toolkit run --refresh-support`. Un support fallito
blocca il candidate. L'esecuzione di un `command` file richiede
`TOOLKIT_ALLOW_SCRIPT_SOURCE=1`.

**Placeholder nel SQL** (il toolkit li risolve prima di eseguire):

| Placeholder | Risolve a |
|---|---|
| `{support.NAME.mart}` | prima tabella mart del support (compat) |
| `{support.NAME.mart.TABLE}` | tabella mart specifica |
| `{support.NAME.clean}` | parquet clean del support |
| `{support.NAME.path}` | file materializzato (codelist/file) |
| `{support.NAME.outputs}` | lista completa degli output |

I SQL devono usare i placeholder e non path hardcoded: `check_support_path_drift`
segnala il drift come warning in dry-run.

## validation

Campi supportati:

| Campo | Tipo | Default |
|---|---|---|
| `validation.fail_on_error` | `bool` | `true` |

## config

Campi supportati:

| Campo | Tipo | Default |
|---|---|---|
| `config.strict` | `bool` | `false` |

Se `config.strict: true`, ogni warning legacy `DCLxxx` viene promosso a errore durante il parse.
Lo stesso comportamento è disponibile da CLI con `--strict-config`.

## output

Campi supportati:

| Campo | Tipo | Default |
|---|---|---|
| `output.artifacts` | `str` | `standard` (accettato ma ignorato) |
| `output.legacy_aliases` | — | rimosso, non ha più effetto |

## duckdb

Configurazione del motore DuckDB per il dataset (opzionale). Il valore viene
passato a `safe_connect` nel layer clean. Gerarchia: default lab `2GB` →
`duckdb.memory_limit` (per-dataset) → env `DUCKDB_MEMORY_LIMIT` (per ambiente).

| Campo | Tipo | Default |
|---|---|---|
| `duckdb.memory_limit` | `string` | `None` (default lab `2GB`) |

Esempio — dataset con join pesanti nel clean (OOM con il default):

```yaml
duckdb:
    memory_limit: "4GB"
```

## Legacy supportato

I seguenti campi legacy sono ancora accettati, ma generano warning con codice `DCLxxx`.
Con `config.strict: true` o `--strict-config`, gli stessi casi diventano errori.

| Code | Legacy | Replacement | Status |
|---|---|---|---|---|

## Legacy rimosso

Le forme seguenti non sono piu supportate. Non generano warning legacy: falliscono subito con errore di config e va usata la shape canonica.

| Legacy rimosso | Usa invece |
|---|---|
| `raw.source` | `raw.sources` |
| `raw.sources[].plugin` | `raw.sources[].type` |
| `raw.sources[].id` | `raw.sources[].name` |
| `clean.read: "auto"` | `clean.read.source: auto` |
| `clean.read.csv.*` | `clean.read.*` |
| `clean.sql_path` | `clean.sql` |
| `mart.sql_dir` | `mart.tables[].sql` |
| `bq` | rimuovere il campo |

## Esempi minimi

### RAW only

```yaml
dataset:
  name: raw_demo
  years: [2024]

raw:
  sources:
    - name: local_csv
      type: local_file
      args:
        path: data/input.csv
        filename: input_{year}.csv
```

### CLEAN only

Presuppone che il layer RAW esista gia sotto `root/data/raw/...`.

```yaml
dataset:
  name: clean_demo
  years: [2024]

clean:
  sql: sql/clean.sql
  read:
    mode: explicit
    include: raw_*.csv
    delim: ";"
```

### MART

Presuppone che il layer CLEAN esista gia sotto `root/data/clean/...`.

```yaml
dataset:
  name: mart_demo
  years: [2024]

mart:
  tables:
    - name: mart_summary
      sql: sql/mart/mart_summary.sql
  required_tables: mart_summary
  validate:
    table_rules:
      mart_summary:
        min_rows: 1
```

### Multi-year MART

Presuppone che i layer annuali richiesti esistano già sotto `root/data/clean/...` oppure `root/data/mart/...`.

```yaml
dataset:
  name: mart_multi_demo
  years: [2022, 2023]

mart:
  tables:
    - name: clean_union
      sql: sql/multi_year/clean_union.sql
      years: [2022, 2023]
      source_layer: clean
```

## Errori config: come leggerli

Il parser restituisce errori con path del campo e messaggio.

Esempi tipici:

- `Config validation failed: output.unknown_flag: Extra inputs are not permitted`
- `Config validation failed: raw.sources: Input should be a valid list`
- `Config validation failed: clean.validate.primary_key: clean.validate.primary_key must be a string or a list of strings`
- `Config validation failed: raw.sources: Input should be a valid list`

Regola pratica:

- se il path punta a una sezione nota (`output`, `validation`, `clean.validate`, `mart.validate`, `config`), il campo non e supportato
- se il path punta a un tipo (`raw.sources`, `clean.read.include`, `root`), la forma YAML e sbagliata
- se compare un warning `DCLxxx`, il file e ancora accettato ma va migrato alla forma canonica
