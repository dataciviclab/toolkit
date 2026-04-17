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

## Quando usare cosa

Regola pratica:

- se stai eseguendo un dataset per la prima volta, parti da `toolkit run all`
- se hai cambiato fonte, anni, extractor, `dataset.yml` o il perimetro del RAW,
  torna a `toolkit run all`
- se hai cambiato `clean.sql` o la logica `clean.read`, riparti da
  `toolkit run clean` e poi `toolkit run mart`
- se hai toccato solo SQL `mart`, preferisci `toolkit run mart`
- se hai aggiunto o modificato solo output multi-anno, preferisci
  `toolkit run cross_year`
- se un run si interrompe ma il run record e gli artefatti precedenti sono
  ancora coerenti, usa `toolkit resume`
- se hai toccato solo notebook, docs o script locali del repo dataset, non
  rilanciare la pipeline per default

Matrice minima:

| Tipo di modifica | Comando consigliato |
|---|---|
| prima esecuzione del dataset | `toolkit run all --config dataset.yml` |
| cambio fonte o perimetro anni | `toolkit run all --config dataset.yml` |
| cambio `dataset.yml` con impatto su input/layer | `toolkit run all --config dataset.yml` |
| cambio `clean.sql` o `clean.read` | `toolkit run clean --config dataset.yml` poi `toolkit run mart --config dataset.yml` |
| cambio solo `mart.sql` | `toolkit run mart --config dataset.yml` |
| cambio solo `cross_year` | `toolkit run cross_year --config dataset.yml` |
| run interrotto a meta' con run record/artifacts coerenti | `toolkit resume ... --config dataset.yml` |
| cambio solo notebook/docs | nessun rerun automatico |

Il toolkit non impone di cancellare `raw/`, `clean/`, `mart/` o `cross/` tra un
run e l'altro. Negli ambienti di lavoro questi output possono restare come
cache locale finche' il loro perimetro e' ancora coerente con la config e con
il layer che stai rieseguendo.

In pratica:

- non trattare `run all` come default per ogni modifica minima
- non cancellare gli output locali "per pulizia" se non hai cambiato il loro perimetro
- usa i rerun parziali quando il punto di ingresso corretto e' chiaro
- usa `resume` per recovery, non come scorciatoia generica a meta' sviluppo

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

Nota pratica:

- `run raw` scrive gia` un `suggested_read.yml` leggero e conservativo quando il file primario e` profilabile
- `profile raw` resta il comando da usare quando vuoi profiling piu` ricco, report diagnostici e `suggested_mapping.yml`

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
- `inspect schema-diff` quando vuoi confrontare rapidamente hints e colonne tra piu anni senza aprire a mano i metadata RAW

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


## ANAC Delta Ingestion (Spike aperto)

Il toolkit oggi offre solo diagnostica e confronto tra layer, non un merge stateful canonico.

Disponibile: `status`/`inspect` espongono profili layer, `compare_layer_profiles` calcola `row_count_delta`, i report diagnostici mostrano `added_columns`, `removed_columns`, `type_changes`.

Prima di introdurre un eventuale `delta_merge` servono verifiche su dati reali:
- forma effettiva del payload ANAC/OCDS usato dal Lab
- chiave operativa stabile per l'upsert
- regole di conflitto per duplicati, rettifiche e annullamenti

**Stato**: Tenere come spike tecnico. Non aggiungere campi di config finché il modello non è verificato su dati reali.

## Compat legacy

Per i repo nuovi:

- usa la shape canonica documentata in [config-schema.md](./config-schema.md)
- usa `--strict-config` nei comandi CLI
- non basarti su alias o campi legacy nei notebook e negli script del repo dataset
