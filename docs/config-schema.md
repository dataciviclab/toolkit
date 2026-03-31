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
| `cross_year` | `object` | no | output opzionali multi-anno |
| `config` | `object` | no | policy parser config |
| `validation` | `object` | no | solo opzioni globali del validation gate |
| `output` | `object` | no | policy artefatti |

## dataset

| Campo | Tipo | Default |
|---|---|---|
| `dataset.name` | `string` | nessuno |
| `dataset.years` | `list[int]` | nessuno |

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
## clean

| Campo | Tipo | Default |
|---|---|---|
| `clean.sql` | `string` | nessuno |
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
| `trim_whitespace` | `bool` | `true` |
| `sample_size` | `int \| null` | `null` |
| `sheet_name` | `string \| int \| null` | `null` |
| `mode` | `explicit \| latest \| largest \| all \| null` | `null` |
| `glob` | `string` | `*` |
| `prefer_from_raw_run` | `bool` | `true` |
| `allow_ambiguous` | `bool` | `false` |
| `include` | `list[string] \| null` | `null` |

Note pratiche:

- i file `.xlsx` sono supportati nel layer CLEAN
- RAW conserva il workbook originale senza convertirlo
- per `.xlsx`, le opzioni utili sono soprattutto `header`, `skip`, `columns`, `trim_whitespace`, `sheet_name`
- `sheet_name` usa il primo foglio se omesso
- `normalize_rows_to_columns: true` ha senso solo insieme a `columns`
- con `normalize_rows_to_columns: true`, il toolkit normalizza le righe corte del CSV allo schema atteso prima di esporre `raw_input`

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

## cross_year

`cross_year` definisce output opzionali multi-anno. Non entra nel loop annuale di `raw/clean/mart`.

L'esecuzione e esplicita:

```bash
py -m toolkit.cli.app run cross_year --config dataset.yml
```

Campi supportati:

| Campo | Tipo | Default |
|---|---|---|
| `cross_year.tables` | `list[CrossYearTable]` | `[]` |

`CrossYearTable`:

| Campo | Tipo | Default |
|---|---|---|
| `name` | `string` | nessuno |
| `sql` | `string` | nessuno |
| `source_layer` | `clean \| mart` | `clean` |
| `source_table` | `string \| null` | `null` |

Note pratiche:

- con `source_layer: clean`, il runner unisce tutti i parquet annuali del layer CLEAN e li espone come view `clean_input` e `clean`
- con `source_layer: mart`, `source_table` e obbligatorio; il runner legge `<year>/<source_table>.parquet` e lo espone come view `mart_input` e `mart`
- gli output vengono scritti in `root/data/cross/<dataset>/`

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
| `output.artifacts` | `minimal \| standard \| debug` | `standard` |
| `output.legacy_aliases` | `bool` | `true` |

## Legacy supportato

I seguenti campi legacy sono ancora accettati, ma generano warning con codice `DCLxxx`.
Con `config.strict: true` o `--strict-config`, gli stessi casi diventano errori.

| Code | Legacy | Replacement | Status |
|---|---|---|---|
| `DCL013` | `cross_year.* unknown keys` | rimuovere il campo | ignored |

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

### CROSS_YEAR

Presuppone che i layer annuali richiesti esistano gia sotto `root/data/clean/...` oppure `root/data/mart/...`.

```yaml
dataset:
  name: cross_demo
  years: [2022, 2023]

cross_year:
  tables:
    - name: clean_union
      sql: sql/cross/clean_union.sql
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
