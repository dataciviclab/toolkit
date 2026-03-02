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
- `toolkit.cli.cmd_resume`
- `toolkit.cli.cmd_profile`
- esecuzione parziale `run raw|clean|mart`

Servono per recovery, diagnostica e casi sporchi, non come baseline per i repo nuovi.

## Compatibility Only

La compatibilita' mantenuta dal toolkit riguarda soprattutto config legacy, alias documentati e alcune superfici CLI storiche.

Non va trattata come parte del contratto stabile per i repo nuovi.

## Experimental / Peripheral Plugins

Nel package `toolkit.plugins` convivono plugin di importanza diversa:

- builtin centrali: `local_file`, `http_file`
- periferici / experimental: `api_json_paged`, `html_table`

I plugin experimental restano disponibili ma non fanno parte del contratto stabile per i repo dataset clonati dal template.
