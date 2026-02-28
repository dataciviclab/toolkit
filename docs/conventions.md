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

## Metadata

- Ogni `metadata.json` include `metadata_schema_version`.
- Il metadata CLEAN include anche i campi di audit `read_params_source`, `read_source_used`, `read_params_used`.

## Validation

- Ogni validation report JSON include `validation_schema_version`.
- I path di output restano:
  - RAW: `raw_validation.json`
  - CLEAN: `_validate/clean_validation.json`
  - MART: `_validate/mart_validation.json`
