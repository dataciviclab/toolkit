SELECT
  cast_int("Anno di imposta") AS anno_imposta,
  normalize_string("Codice catastale") AS codice_catastale,
  normalize_string("Codice Istat Comune") AS codice_istat_comune,
  normalize_string("Denominazione Comune") AS comune,
  normalize_string("Sigla Provincia") AS sigla_provincia,
  normalize_string("Regione") AS regione,
  cast_bigint("Numero contribuenti") AS numero_contribuenti
FROM raw_input
