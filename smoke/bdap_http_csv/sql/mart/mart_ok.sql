SELECT
  anno,
  saldo_netto,
  indebitamento_netto,
  avanzo_primario,
  entrate_finali,
  spese_finali,
  entrate_finali - spese_finali AS differenza_entrate_spese,
  CASE
    WHEN spese_finali IS NULL OR spese_finali = 0 THEN NULL
    ELSE entrate_finali / spese_finali
  END AS rapporto_entrate_spese
FROM clean_input
WHERE anno IS NOT NULL
ORDER BY anno
