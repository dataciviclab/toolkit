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
  CAST({year} AS INTEGER) AS anno,

  CAST(TRIM(regione)  AS VARCHAR) AS regione,
  CAST(TRIM(provincia) AS VARCHAR) AS provincia,
  CAST(TRIM(comune)   AS VARCHAR) AS comune,

  CAST(
    NULLIF(
      REPLACE(
        REPLACE(
          REPLACE(TRIM(CAST(pct_rd_raw AS VARCHAR)), '%', ''),
        '.', ''),
      ',', '.'),
    '-')
    AS DOUBLE
  ) AS pct_rd,

  CAST(
    NULLIF(
      REPLACE(
        REPLACE(TRIM(CAST(ru_tot_t_raw AS VARCHAR)), '.', ''),
      ',', '.'),
    '-')
    AS DOUBLE
  ) AS ru_tot_t

FROM base
WHERE regione  IS NOT NULL AND TRIM(regione)  <> ''
  AND provincia IS NOT NULL AND TRIM(provincia) <> ''
  AND comune   IS NOT NULL AND TRIM(comune)   <> ''
