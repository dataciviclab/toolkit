WITH base AS (
  SELECT
    cast_int("Anno di Riferimento") AS anno,
    cast_int("Codice Regione") AS codice_regione,
    normalize_string("Descrizione Regione") AS regione,
    cast_int("Codice Ente SSN") AS codice_ente_ssn,
    normalize_string("Descrizione Ente") AS descrizione_ente,
    normalize_string("Codice Voce Contabile") AS codice_voce_contabile,
    normalize_string("Descrizione Voce Contabile") AS descrizione_voce_contabile,
    cast_double("Importo Totale") AS importo_totale
  FROM raw_input
)

SELECT *
FROM base
WHERE anno IS NOT NULL;
