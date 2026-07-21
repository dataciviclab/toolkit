SELECT
    cast_int(anno) AS anno,
    comune,
    provincia,
    regione,
    cast_int(codice_comune) AS codice_comune,
    categoria,
    cast_double(valore) AS valore
FROM raw_input
