SELECT
  TRY_CAST("Anno di imposta" AS INTEGER) AS anno_imposta,
  "Codice catastale" AS codice_catastale,
  "Codice Istat Comune" AS codice_istat_comune,
  "Denominazione Comune" AS comune,
  "Sigla Provincia" AS sigla_provincia,
  "Regione" AS regione,
  TRY_CAST("Numero contribuenti" AS BIGINT) AS numero_contribuenti
FROM raw_input
