WITH base AS (
  SELECT
    COALESCE("Regione", "REGIONE") AS regione,
    COALESCE("Provincia", "PROVINCIA") AS provincia,
    COALESCE("Comune", "COMUNE") AS comune,

    COALESCE(
      "Raccolta differenziata (%)",
      "Raccolta differenziata %",
      "RD (%)",
      "RD%",

      -- varianti strane viste in giro
      "Raccolta differenziata ( % )"
    ) AS pct_rd_raw,

    COALESCE(
      "Rifiuti urbani totali (t)",
      "Rifiuti urbani totali t",
      "RU totali (t)",
      "RU totali t"
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
  AND comune   IS NOT NULL AND TRIM(comune)   <> '';