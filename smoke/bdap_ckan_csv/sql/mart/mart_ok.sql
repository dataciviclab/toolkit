SELECT
  anno,
  regione,
  descrizione_voce_contabile,
  COUNT(*) AS righe,
  SUM(importo_totale) AS importo_totale
FROM clean_input
WHERE anno IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
