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
| `config` | `object` | no | policy parser config |
| `validation` | `object` | no | solo opzioni globali del validation gate |
| `output` | `object` | no | policy artefatti |
| `bq` | `object \| null` | no | accettato ma ignorato, con warning |

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
| `nullstr` | `string \| list[string] \| null` | `null` |
| `columns` | `dict[string,string] \| null` | `null` |
| `trim_whitespace` | `bool` | `true` |
| `sample_size` | `int \| null` | `null` |
| `mode` | `explicit \| latest \| largest \| all \| null` | `null` |
| `glob` | `string` | `*` |
| `prefer_from_raw_run` | `bool` | `true` |
| `allow_ambiguous` | `bool` | `false` |
| `include` | `list[string] \| null` | `null` |

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
| `DCL001` | `raw.source` | `raw.sources` | deprecated |
| `DCL002` | `raw.sources[].plugin` | `raw.sources[].type` | deprecated |
| `DCL003` | `raw.sources[].id` | `raw.sources[].name` | deprecated |
| `DCL004` | `clean.read: "auto"` | `clean.read.source: auto` | deprecated |
| `DCL005` | `clean.read.csv.*` | `clean.read.*` | deprecated |
| `DCL006` | `clean.sql_path` | `clean.sql` | ignored |
| `DCL007` | `mart.sql_dir` | `mart.tables[].sql` | ignored |
| `DCL008` | `bq` | rimuovere il campo | ignored |

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

## Errori config: come leggerli

Il parser restituisce errori con path del campo e messaggio.

Esempi tipici:

- `Config validation failed: output.unknown_flag: Extra inputs are not permitted`
- `Config validation failed: raw.sources: Input should be a valid list`
- `Config validation failed: clean.validate.primary_key: clean.validate.primary_key must be a string or a list of strings`
- `DCL001 raw.source is deprecated, usare raw.sources`

Regola pratica:

- se il path punta a una sezione nota (`output`, `validation`, `clean.validate`, `mart.validate`, `config`), il campo non e supportato
- se il path punta a un tipo (`raw.sources`, `clean.read.include`, `root`), la forma YAML e sbagliata
- se compare un warning `DCLxxx`, il file e ancora accettato ma va migrato alla forma canonica
