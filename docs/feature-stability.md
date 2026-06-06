# Feature Stability

Questa matrice serve a chiarire cosa il toolkit considera percorso canonico, cosa resta supportato ma secondario, e cosa non va trattato come parte del quickstart dei repo dataset clonati dal template.

| Area | Stato | Uso raccomandato |
|---|---|---|
| `query` | stable | query SQL su parquet (path o dataset.yml + layer) |
| `parquet_preview(sql=...)` | stable | API core per SQL arbitrario su parquet |
| `run all` | stable | percorso canonico |
| `validate all` | stable | percorso canonico |
| `status` | stable | percorso canonico |
| path contract di `dataset.yml` | stable | percorso canonico |
| output `raw/clean/mart/_runs` | stable | percorso canonico |
| `inspect paths` | stable | helper per notebook e repo dataset |
| `resume` | supported / advanced | debug operativo e recovery |
| `inspect profile` | supported / advanced | diagnostica su RAW sporchi o ambigui |
| `run raw\|clean\|mart` | supported / advanced | debug e re-run parziali |
| `scout` | stable | esplorazione URL esterni, probe e routing automatico |
| `scout --scaffold` | stable | probe + scaffold candidate dataset (dataset.yml, SQL, README) |
| `scout --run` | supported / advanced | scout + scaffold + raw run in unico comando |
| `mart` tabelle con `years` | stable | multi-year (sostituisce ex `cross_year`) |
| `inspect schema-diff` | supported / advanced | confronto rapido segnali schema RAW tra anni |
| artifact policy | deprecated / ignored | accettato per backward compat ma senza effetto |
| `legacy_aliases` | removed | non ha più effetto — `raw_profile.json` è l'unico formato |
| config legacy | compatibility only | usare `--strict-config` nei repo nuovi |
| `inspect url` | removed | sostituito da `toolkit scout` |

Lettura equivalente a livello package:

- core runtime: `toolkit.raw`, `toolkit.clean`, `toolkit.mart`, `toolkit.scout`, `toolkit.cli` (`run`, `validate`, `status`, `inspect`)
- advanced tooling: `resume`, run parziali, `inspect profile`, `inspect schema-diff`
- compatibility only: config legacy e alias storici

Sorgenti builtin supportate dal runtime canonico: `local_file`, `http_file`, `http_post_file`, `ckan`, `sdmx`, `sparql`. Il runtime può conservare `.xlsx` e `.xls` in RAW e leggerli in CLEAN — il file originale resta l'artefatto sorgente.

Regola pratica:

- se stai creando o clonando un repo dataset nuovo, resta nel percorso canonico
- se devi fare recovery, diagnostica o bootstrap, usa i comandi advanced
- non basarti su compat legacy o helper frozen come parte del contratto stabile
