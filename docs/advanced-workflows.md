# Advanced Workflows

Questa nota raccoglie i flussi e le opzioni del toolkit che restano supportati, ma non fanno parte del percorso canonico dei repo dataset clonati dal template.

Percorso canonico:

- `toolkit run all --config dataset.yml`
- `toolkit validate all --config dataset.yml`
- `toolkit status --dataset <dataset> --year <year> --latest --config dataset.yml`
- notebook locali che leggono output e metadata sotto `root/data/...`

Questa categoria include anche tooling di supporto che non va confuso con il runtime principale del toolkit:

- `toolkit.profile`
- `resume`
- run parziali per layer
- plugin periferici o experimental

## Step singoli

Utili per debug o per ripetere solo una parte della pipeline:

```bash
toolkit run raw --config dataset.yml
toolkit run clean --config dataset.yml
toolkit run mart --config dataset.yml
```

Questi comandi non sono il happy path raccomandato per i nuovi repo dataset, ma restano strumenti operativi supportati.

## Resume

`resume` serve quando esiste gia` un run record e vuoi ripartire dal primo layer non `SUCCESS` oppure forzare una ripartenza da `raw|clean|mart`.

Esempi:

```bash
toolkit resume --dataset my_dataset --year 2024 --latest --config dataset.yml
toolkit resume --dataset my_dataset --year 2024 --run-id <run_id> --from-layer clean --config dataset.yml
```

Il comando verifica anche gli artefatti minimi del layer precedente prima di ripartire.

## Profile RAW

`toolkit profile raw --config dataset.yml` genera hint utili per `clean.read` quando il RAW e` sporco, ambiguo o poco noto.

Artefatti principali:

- `raw/<dataset>/<year>/_profile/raw_profile.json`
- `raw/<dataset>/<year>/_profile/suggested_read.yml`

`profile.json` resta un alias legacy opzionale e non e` il nome canonico da promuovere nei nuovi repo.

## CLEAN read e input selection

Opzioni utili ma avanzate:

- `clean.read.mode`: `explicit | latest | largest | all`
- `clean.read.include`
- `clean.read.glob`
- `clean.read.prefer_from_raw_run`
- `clean.read.allow_ambiguous`
- `clean.read.source`: `auto | config_only`
- `clean.read_mode`: `strict | fallback | robust`

Uso consigliato:

- repo dataset nuovi: configurazione esplicita e `--strict-config`
- `profile raw` solo se serve capire meglio il formato RAW

## Artifact policy

La policy artifacts resta disponibile per tuning operativo:

```yaml
output:
  artifacts: standard   # minimal | standard | debug
  legacy_aliases: true
```

Regola pratica:

- `standard`: default consigliato
- `minimal`: riduce artefatti opzionali
- `debug`: conserva anche SQL renderizzate e dettagli di debug

`legacy_aliases` resta supportato per compatibilita`, ma non va promosso nei nuovi repo dataset.

## Plugin periferici

`local_file` e `http_file` sono le sorgenti builtin centrali del toolkit.

Plugin come `api_json_paged` e `html_table` restano disponibili, ma vanno considerati periferici o experimental:

- non sono parte del quickstart
- non sono il contratto stabile per i repo dataset nuovi
- non andrebbero usati come base del template senza evidenza reale

## Compat legacy

Il toolkit mantiene compatibilita` con alcune forme legacy del config per facilitare la migrazione.

Per i repo nuovi:

- usa la shape canonica documentata in [config-schema.md](./config-schema.md)
- usa `--strict-config` nei comandi CLI
- non basarti su alias o campi legacy nei notebook e negli script del repo dataset
