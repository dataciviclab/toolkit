WITH base AS (
  SELECT
    TRY_CAST(TRIM(CAST("Anno di Riferimento" AS VARCHAR)) AS INTEGER) AS anno,
    TRY_CAST(TRIM(CAST("Codice Regione" AS VARCHAR)) AS INTEGER) AS codice_regione,
    TRIM(CAST("Descrizione Regione" AS VARCHAR)) AS regione,
    TRY_CAST(TRIM(CAST("Codice Ente SSN" AS VARCHAR)) AS INTEGER) AS codice_ente_ssn,
    TRIM(CAST("Descrizione Ente" AS VARCHAR)) AS descrizione_ente,
    TRIM(CAST("Codice Voce Contabile" AS VARCHAR)) AS codice_voce_contabile,
    TRIM(CAST("Descrizione Voce Contabile" AS VARCHAR)) AS descrizione_voce_contabile,
    TRY_CAST(TRIM(CAST("Importo Totale" AS VARCHAR)) AS DOUBLE) AS importo_totale
  FROM raw_input
)

SELECT *
FROM base
WHERE anno IS NOT NULL;
