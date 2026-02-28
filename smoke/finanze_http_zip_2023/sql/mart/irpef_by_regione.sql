SELECT
  regione,
  COUNT(*) AS comuni,
  SUM(numero_contribuenti) AS contribuenti
FROM clean_input
GROUP BY regione
ORDER BY regione
