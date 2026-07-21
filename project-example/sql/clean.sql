WITH base AS (
  SELECT
    CAST(COALESCE("Regione", "REGIONE") AS VARCHAR) AS regione,
    CAST(COALESCE("Provincia", "PROVINCIA") AS VARCHAR) AS provincia,
    CAST(COALESCE("Comune", "COMUNE") AS VARCHAR) AS comune,

    COALESCE(
      CAST("Raccolta differenziata (%)" AS VARCHAR),
      CAST("Raccolta differenziata %" AS VARCHAR),
      CAST("RD (%)" AS VARCHAR),
      CAST("RD%" AS VARCHAR),

      -- varianti strane viste in giro
      CAST("Raccolta differenziata ( % )" AS VARCHAR)
    ) AS pct_rd_raw,

    COALESCE(
      CAST("Rifiuti urbani totali (t)" AS VARCHAR),
      CAST("Rifiuti urbani totali t" AS VARCHAR),
      CAST("RU totali (t)" AS VARCHAR),
      CAST("RU totali t" AS VARCHAR)
    ) AS ru_tot_t_raw

  FROM raw_input
)

SELECT
    cast_int({year}) AS anno,

    normalize_string(regione) AS regione,
    normalize_string(provincia) AS provincia,
    normalize_string(comune) AS comune,

    -- Il formato italiano (%, . e ,) viene normalizzato dal toolkit:
    -- normalize_italian_number gestisce 1.234,56 → 1234.56
    -- La % viene rimossa manualmente prima della macro
    normalize_italian_number(
      REPLACE(normalize_string(pct_rd_raw), '%', '')
    ) AS pct_rd,

    normalize_italian_number(ru_tot_t_raw) AS ru_tot_t

FROM base
WHERE regione  IS NOT NULL AND TRIM(regione)  <> ''
  AND provincia IS NOT NULL AND TRIM(provincia) <> ''
  AND comune   IS NOT NULL AND TRIM(comune)   <> ''
