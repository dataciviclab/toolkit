# ADR-002: Tre layer di trasformazione (RAW → CLEAN → MART)

**Status:** implemented (2026-02)

## Contesto

I dataset pubblici arrivano in formati eterogenei: CSV con delimiteri e encoding
vari, Excel multi-foglio, file zippati, risposte JSON da API, flussi SDMX.
Il toolkit doveva garantire che qualsiasi analisi fosse riproducibile e
auditabile, dalla fonte originale ai dati aggregati.

Alternative considerate:
- **Script monolitico** che scarica, pulisce e aggrega in un unico passo
- **Due layer** (raw + clean)
- **Tre layer** (raw + clean + mart)

## Decisione

Tre layer progressivi, ognuno con directory, manifest e validation separati:

```
RAW  →  CLEAN  →  MART
```

| Layer | Cosa produce | Chi lo usa |
|---|---|---|
| **RAW** | File originale identico alla fonte + manifest + metadata | Audit, verifica fonte |
| **CLEAN** | Parquet normalizzato (nomi colonna stabili, tipi fissi) | Analisi, data-explorer |
| **MART** | Parquet aggregato per dimensione | Report, dashboard |

Ogni layer ha:
- Una directory dedicata: `data/{layer}/{dataset}/{year}/`
- `metadata.json` con input, output, hash, config_hash e summary di validazione
- `_validate/{layer}_validation.json` (CLEAN e MART)
- `_profile/` per RAW con profiling automatico
- Run record in `data/_runs/{dataset}/{year}/` per tracciabilità

## Conseguenze

**Positive:**
- Separazione netta delle responsabilità — ogni layer ha il suo modulo
- Si può rieseguire solo il layer interessato (`toolkit run clean`)
- Auditabile: RAW è sempre l'originale, CLEAN è sempre derivato deterministicamente
- Resume possibile: se CLEAN fallisce, il RAW già completato non si rifà
- MART multipli per dataset (più tabelle aggregate)

**Negative:**
- Tre directory invece di una — più file system I/O
- Contratto di path complesso (`root/data/{layer}/{dataset}/{year}/...`)
- CLEAN deve sapere cosa ha prodotto RAW (bridge via manifest.json)
- Overhead per dataset piccoli (pochi KB)

**Tradeoff accettato:** la complessità in più è giustificata dalla tracciabilità
e dalla possibilità di run parziali, essenziali per un ambiente di incubazione
dati con cicli di iterazione rapidi.
