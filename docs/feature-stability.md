# Feature Stability

Questa matrice serve a chiarire cosa il toolkit considera percorso canonico, cosa resta supportato ma secondario, e cosa non va trattato come parte del quickstart dei repo dataset clonati dal template.

| Area | Stato | Uso raccomandato |
|---|---|---|
| `run all` | stable | percorso canonico |
| `validate all` | stable | percorso canonico |
| `status` | stable | percorso canonico |
| path contract di `dataset.yml` | stable | percorso canonico |
| output `raw/clean/mart/_runs` | stable | percorso canonico |
| `inspect paths` | stable | helper per notebook e repo dataset |
| `resume` | supported / advanced | debug operativo e recovery |
| `profile raw` | supported / advanced | diagnostica su RAW sporchi o ambigui |
| `run raw|clean|mart` | supported / advanced | debug e re-run parziali |
| `run cross_year` | supported / advanced | output multi-anno e workflow non canonici |
| `inspect schema-diff` | supported / advanced | confronto rapido segnali schema RAW tra anni |
| artifact policy `minimal|standard|debug` | supported / advanced | tuning operativo |
| `legacy_aliases` | compatibility only | non promuovere nei repo nuovi |
| config legacy | compatibility only | usare `--strict-config` nei repo nuovi |
Lettura equivalente a livello package:

- core runtime: `toolkit.raw`, `toolkit.clean`, `toolkit.mart`, `toolkit.cli` (`run`, `validate`, `status`, `inspect`)
- advanced tooling: `toolkit.profile`, `resume`, run parziali, `cross_year`, `inspect schema-diff`
- compatibility only: config legacy e alias storici

Regola pratica:

- se stai creando o clonando un repo dataset nuovo, resta nel percorso canonico
- se devi fare recovery, diagnostica o bootstrap, usa i comandi advanced
- non basarti su compat legacy o helper frozen come parte del contratto stabile
