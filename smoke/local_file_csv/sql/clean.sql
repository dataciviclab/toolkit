SELECT
    CAST(anno AS INTEGER) AS anno,
    comune,
    provincia,
    regione,
    CAST(codice_comune AS INTEGER) AS codice_comune,
    categoria,
    CAST(valore AS DOUBLE) AS valore
FROM raw_input
