# ADR-006: Ingestione incrementale delle fonti stream — stato nel data plane

**Status:** proposed (2026-11-03)

## Contesto

Il modello RAW del toolkit è *fetch-all → overwrite*: ogni fonte è trattata
come uno snapshot ri-fetchabile. È corretto per un CSV annuale o un dump; non
lo è per le fonti che si muovono a ogni evento (voti in aula, sindacato
ispettivo, GU): ri-fetchare è caro e fragile.

Conseguenza: ogni fonte che ha bisogno di incrementale viene risolta **fuori
dal toolkit**, con uno script dedicato e una baseline esterna.

Evidenza nel Lab:

- `gu-monitor/scripts/collect.py` — watermark su `max(data)`, finestra con
  `overlap`, dedup per chiave. Pattern completo, ma fuori dal toolkit.
- `open-politica/camera-voti` e `senato-votazioni` — flag `--incremental`,
  delta su `max(data)`, baseline = parquet precedente.
- `rna-aiuti-stato/full_batch.py` + `data/derived/` persistente su runner
  self-hosted.

La baseline non sopravvive a un runner effimero: in CI serve la cache di GitHub
Actions (eviction 7 giorni, chiave per-branch) o un runner self-hosted con disco
persistente. Il costo è misurabile: il run `open-politica` del 1/11 è durato
**1h37m**, ha completato **34/35** dataset, è morto su `senato-votazioni`
(timeout) e ha caricato **zero** (sync dopo il run).

`toolkit#72` aveva scoped un `delta_merge` e chiuso lo spike **senza verdetto**:
*"non aggiungere campi di config finché il modello non è verificato"*. Questa
ADR dà il verdetto di modello.

Alternative considerate:

- **Status quo per-repo** (script + baseline esterna): non generalizza, ogni
  repo reinventa il watermark e il posto dove tenerlo.
- **Cache GitHub Actions**: fragile — eviction, chiave, per-branch.
- **Disco del runner self-hosted**: lega lo stato alla VM, non generalizza.
- **Stato stateful nel core senza contratto**: escluso da `#72`.

## Decisione

1. **Distinguere snapshot e stream a livello di fonte.** `raw.sources[].incremental`
   opzionale; assente = `full` (backward compatible).
2. **Contratto di delta coerente con la paginazione keyset esistente**:
   `{key, key_expr, key_column, key_literal}` per il watermark; placeholder
   `{since}` nelle query (SPARQL e source HTTP con parametri) accanto a
   `{keyset}` già supportato.
3. **Merge canonico per chiave** (append + dedup/upsert) come step di raw/clean
   — il `delta_merge` di `#72`.
4. **Lo stato (watermark + baseline) vive nel data plane**: accanto all'artefatto
   pubblicato (ultimo parquet + `_state.json`), risolvibile da un checkout
   fresco. Il run diventa: *scarica baseline → fetch delta → merge → publish →
   scrivi watermark*.
5. **Il dataset è l'unità atomica di run+publish**: la pubblicazione avviene
   quando quel dataset riesce, non in un passo globale dopo tutti i run.

## Implementazione incrementale (primo pezzo, #536)

Il contratto è stato implementato in due fasi. Fase 1 (append-only):

- `raw.sources[].incremental {key_expr, key_column, key_literal}` +
  placeholder `{since}`; richiede `output_policy: overwrite`.
- La baseline è il raw scritto dallo stesso source (**path formattato**, con
  `{year}` sostituito); source con ext non deterministica (`from_origin`,
  `script`) sono rifiutati.
- `{since}` è **obbligatorio**: senza, il full verrebbe accodato al baseline.
  La validazione (chiavi, presenza del placeholder) sta in `core.config` ed è
  applicata al load — `validate`/`inspect` la vedono.
- Delta vuoto = no-op (`EmptyResultError` distinta da `DownloadError`).
- Append fedele byte-per-byte; header del delta confrontato col baseline
  (schema drift → errore).

**Limiti noti** (da coprire in fase 2 / follow-up):

- solo **append esclusivo** (`>`). Le righe con `key == max` pubblicate *dopo*
  il run sono perse (batch con la stessa data): per dati con revisioni serve
  dedup/upsert.
- la baseline va ancora ripristinata nel checkout (disco/runner o download):
  la risoluzione automatica dal data plane (GCS) è il pezzo successivo.
- nessuna scrittura di `_state.json`: il watermark è derivato dal raw stesso.

## Conseguenze

**Positive:**

- run incrementale corretto e riproducibile su runner effimero: VM e cache
  GitHub non più necessari per la *correttezza* (restano scelte di capacità).
- il pattern watermark (già in `gu-monitor`) diventa contratto unico, non
  per-repo.
- un fallimento su un dataset non distrugge il lavoro degli altri.
- default `full` = nessuna rottura per i repo esistenti.

**Negative:**

- superficie pubblica nuova (campo config + artifact `_state.json`): da
  documentare, backward-compatible.
- merge/upsert con regole di conflitto (rettifiche, annullamenti) da definire
  per fonte — è il rischio che `#72` voleva verificare prima.
- costo di implementazione nel core, cross-repo.

## Stato

**proposed** — in attesa di conferma. Sblocca: migrazione `open-politica`
(fonti stream), chiusura di `toolkit#72`.
