# Runtime Boundaries

Questa nota chiarisce quali parti del package `toolkit/` fanno davvero parte del runtime principale e quali no.

## Core Runtime

Queste aree definiscono il contratto stabile del toolkit:

- `toolkit.raw`
- `toolkit.clean`
- `toolkit.mart`
- `toolkit.cli` per `run`, `validate`, `status`, `inspect`
- `toolkit.core` per config, path, metadata, run tracking e validation

Sono le superfici che i repo dataset e il `project-template` dovrebbero considerare centrali.

## Advanced Tooling

Queste aree restano supportate, ma non fanno parte del percorso canonico:

- `toolkit.profile`
- `toolkit.cross` — output multi-anno (`run cross_year`)
- `toolkit.cli.cmd_resume`
- `toolkit.cli.cmd_profile`
- esecuzione parziale `run raw|clean|mart`

Servono per recovery, diagnostica e output specializzati, non come baseline per i repo nuovi.

## Experimental

Funzionalita' presenti ma non ancora parte del contratto stabile:

- `toolkit.cli.cmd_scout_url` (`toolkit scout-url`) — scouting rapido di un URL pubblico

## Compatibility Only

La compatibilita' mantenuta dal toolkit riguarda soprattutto config legacy, alias documentati e alcune superfici CLI storiche.

Non va trattata come parte del contratto stabile per i repo nuovi.

## Builtin Sources

Le sorgenti builtin supportate dal runtime canonico sono:

- `local_file`
- `http_file`

Nota:

- il runtime canonico puo' conservare file `.xlsx` in RAW e leggerli in CLEAN
- questo non cambia il ruolo del layer RAW: il file originale resta l'artefatto sorgente
