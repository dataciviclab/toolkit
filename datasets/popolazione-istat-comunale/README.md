# Popolazione ISTAT comunale

## Tipo

**Support dataset** — base infrastrutturale per join, coverage check, indicatori pro capite.

## Fonte

ISTAT — demo.posas `POSAS_{year}_it_Comuni.zip`
URL: `https://demo.istat.it/data/posas/POSAS_{year}_it_Comuni.zip`
Formato: ZIP → CSV, `;` delim, `utf-8` con BOM. Skip 1 riga (header doppio).

## Schema

**Clean**: 22 colonne — rinomine a snake_case, più colonna calcolata `fascia_eta`. Escluse le righe con ETA=999 (totali pre-aggregati — somma ridondante delle età 0-100). Utilizza le macro standard del toolkit (`cast_int`, `normalize_string`).

**Mart `popolazione_by_comune`**: `[anno_riferimento, codice_comune, comune, popolazione_residente, popolazione_maschile, popolazione_femminile]` — un record per comune (SUM GROUP BY).

**Mart `popolazione_by_eta`**: `[anno_riferimento, codice_comune, comune, eta, popolazione_residente, popolazione_maschile, popolazione_femminile]` — un record per comune per classe di età (0-100).

**Mart `popolazione_by_regione`**: `[anno_riferimento, regione, popolazione_residente, popolazione_maschile, popolazione_femminile, numero_comuni, superficie_km2, densita_ab_km2]` — aggregazione regionale con densità. JOIN con `comuni-master` per codice ISTAT e denominazione.

**Mart `popolazione_by_provincia`**: `[anno_riferimento, sigla_provincia, provincia, regione, popolazione_residente, popolazione_maschile, popolazione_femminile, numero_comuni, superficie_km2, densita_ab_km2]` — aggregazione provinciale con densità.

**Mart `popolazione_indicatori`**: `[anno_riferimento, codice_comune, comune, popolazione_totale, pop_0_14, pop_15_64, pop_65_plus, pop_under_18, pop_75_plus, indice_vecchiaia, rapporto_dipendenza, pct_under_18, pct_over_65, pct_over_75]` — indicatori demografici per comune.

**Hierarchy `h_fascia`**: `[codice_comune, comune, fascia_eta]` + 17 metriche demografiche — un record per comune per fascia d'età (0-14, 15-29, 30-44, 45-59, 60-74, 75+). Generato automaticamente dalla mart hierarchy del toolkit.

Chiave di join: `codice_comune`

## Copertura

| Anno | Comuni | Popolazione |
|---|---|---|
| 2019 | 7.954 | 59.816.673 |
| 2020 | 7.914 | 59.641.488 |
| 2021 | 7.903 | 59.236.213 |
| 2022 | 7.904 | 59.030.133 |
| 2023 | 7.901 | 58.997.201 |
| 2024 | 7.900 | 58.971.230 |
| 2025 | 7.896 | 58.943.464 |

*La variazione 7.954 → 7.896 riflette fusioni comunali nel periodo — non un errore. Vedi notes.md §Cautele.*

## QC

- Clean filtra ETA=999 (totali pre-aggregati — somma ridondante) ✅
- Maschi + Femmine = Popolazione su tutti gli anni ✅
- Nessun comune senza nome o con pop=0 ✅
- by_comune vs sum(by_eta): differenza = 0 ✅
- hierarchy h_fascia vs mart by_comune: ratio 1.0000 su popolazione_residente ✅

## Uso

- join con dataset comunali (es. IRPEF)
- controlli coverage territoriale
- indicatori pro capite
- detector anomalie territoriali

## Stato

`runnable` — tutti e 7 gli anni con raw, clean, mart verificati.
