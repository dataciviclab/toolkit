# DBnomics come canale toolkit (sostituto ISTAT live)

## Contesto

ISTAT SDMX (`esploradati.istat.it`): 5 req/min per IP, ban 1–2gg, timeout/redirect da IP Lab.
DBnomics è il **mirror pubblico** dello stesso SDMX (provider `ISTAT`, dataset = dataflow id).

## Implementato (toolkit)

`type: dbnomics` — plugin `toolkit/plugins/dbnomics.py`

```yaml
raw:
  sources:
    - name: istat_pil
      type: dbnomics
      client:
        timeout: 120
        retries: 2
      args:
        provider: ISTAT
        dataset: "93_498"          # o "ISTAT/93_498"
        dimensions:
          REF_AREA: [IT, ITC1]
          FREQ: [A]
          DATA_TYPE_AGGR: [B1GQ_B_W2_S1]
          VALUATION: [V]
          ADJUSTMENT: [N]
        page_size: 1000
        max_pages: 200
        filename: istat_pil_series.csv
```

### Output contratto

CSV lungo UTF-8:

```text
series_code,provider,dataset,<dim…>,TIME_PERIOD,OBS_VALUE
```

### API

- `GET {base}/series/{provider}/{dataset}?dimensions=json&observations=1&limit=&offset=`
- Paginazione automatica fino a `num_found` o `max_pages`
- Dimensioni: stringa o lista (OR con `+` lato API)
- Missing `NA` saltati

### Client

- `HttpClient` (SSL fallback, retries, UA)
- Default `page_size=1000` (cap API), `max_pages=200` (guardrail cubi enormi)

## Policy Lab

| Caso | Canale |
|---|---|
| Discovery + run routine ISTAT | **dbnomics** |
| Freschezza se mirror indietro | SDMX 1–2 call mirate, sessione rara |
| Cubo enorme (comuni × NACE) | filtri dimensions + max_pages; o preprocess parallelo come ASIA |
| CI automatico contro SDMX ISTAT | **no** |

## Test

`tests/test_dbnomics_plugin.py` — unit con fake client (6 test).
Smoke live: fetch `93_498` IT B1GQ filtro ristretto.

## Estensioni successive (opzionali)

- Scout probe `dbnomics.world` (routing)
- `dataset_meta` (dimension_values_labels) per validazione filtri
- Provider Eurostat via DBnomics (già coperto da `type: sdmx` ESTAT)
